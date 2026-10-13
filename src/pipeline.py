"""Оркестрация загрузки исторических данных ETL.

Модуль координирует этапы Extract, Transform и Load:
параллельно получает данные за несколько периодов, последовательно
обрабатывает результаты и сохраняет их в PostgreSQL.

При ошибке API сохраняет информацию о сбойном периоде
и не продвигает контрольную точку ETL через образовавшийся пропуск.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter

from src.config import ETLConfig
from src.constants import API_CONCURRENT_REQUESTS
from src.extract import GraderExtractor
from src.load import GraderLoader
from src.rejected import RejectedRecords
from src.state import ETLState
from src.transform import GraderTransformer

# Один период представлен парой дат: начало и конец интервала.
type Period = tuple[datetime, datetime]


@dataclass
class RunProgress:
    """Хранит промежуточные результаты текущего запуска ETL."""

    processed_periods: int = 0
    total_inserted: int = 0
    failed_periods: int = 0
    skipped_periods: int = 0
    has_gap: bool = False


class GraderPipeline:
    """Управляет исторической загрузкой и обработкой периодов ETL."""

    def __init__(
        self,
        config: ETLConfig,
        extractor: GraderExtractor,
        transformer: GraderTransformer,
        loader: GraderLoader,
        state: ETLState,
        rejected: RejectedRecords,
        logger: logging.Logger,
    ) -> None:
        """Получает подготовленные компоненты ETL через конструктор."""
        # Компоненты создаются вне пайплайна и передаются ему
        # для координации этапов обработки.
        self.config = config
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.state = state
        self.rejected = rejected
        self.logger = logger

    def _get_period_batch(
        self,
        next_period: Period,
    ) -> tuple[list[Period], Period | None]:
        """Формирует очередную группу периодов для загрузки."""
        periods = []
        current_period: Period | None = next_period

        # Размер группы ограничен числом параллельных запросов.
        for _ in range(API_CONCURRENT_REQUESTS):
            if current_period is None:
                break

            start, end = current_period
            periods.append((start, end))
            current_period = self.config.get_period(end)

        return periods, current_period

    async def _fetch_periods_async(
        self,
        pending_periods: list[Period],
        semaphore: asyncio.Semaphore,
    ) -> dict[Period, list[dict] | BaseException]:
        """Параллельно получает данные за ещё не загруженные периоды."""
        if pending_periods:
            self.logger.info(
                'Началось скачивание данных API: %s периодов',
                len(pending_periods),
            )

        download_started = perf_counter()

        # Создаём запросы только для отсутствующих периодов.
        tasks = [
            self.extractor.from_api_async(
                start=start,
                end=end,
                semaphore=semaphore,
            )
            for start, end in pending_periods
        ]

        # Ошибки отдельных запросов возвращаются как результаты,
        # не прерывая обработку остальных периодов.
        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        if pending_periods:
            self.logger.info(
                'Скачивание завершено за %.2f сек.',
                perf_counter() - download_started,
            )

        # Сопоставляем каждый период с его ответом или исключением.
        return dict(
            zip(
                pending_periods,
                results,
                strict=True,
            )
        )

    def _load_period_records(
        self,
        period: Period,
        records: list[dict],
        progress: RunProgress,
    ) -> None:
        """Преобразует и сохраняет данные одного успешного периода."""
        start, end = period

        self.logger.info(
            'Обработка периода UTC: %s — %s',
            start,
            end,
        )

        # Преобразуем ответ API и отделяем корректные записи.
        transform_started = perf_counter()

        valid_records = self.transformer.transform_records(records)

        self.logger.info(
            'Преобразование завершено: %s корректных записей, время %.2f сек.',
            len(valid_records),
            perf_counter() - transform_started,
        )

        # Сохраняем отклонённые записи отдельно.
        self.rejected.save(
            records=self.transformer.rejected_records,
            period_start=start,
        )

        self.logger.info(
            'Началась запись в PostgreSQL: %s записей',
            len(valid_records),
        )

        load_started = perf_counter()

        # После ошибки API данные сохраняются, но checkpoint
        # больше не продвигается в рамках текущего запуска.
        inserted = self.loader.load_records(
            records=valid_records,
            period_start=start,
            period_end=end,
            update_state=not progress.has_gap,
        )

        self.logger.info(
            'Запись в PostgreSQL завершена: добавлено %s, пропущено дубликатов %s, время %.2f сек.',
            inserted,
            len(valid_records) - inserted,
            perf_counter() - load_started,
        )

        # Обновляем показатели только после успешного сохранения.
        progress.total_inserted += inserted
        progress.processed_periods += 1

    def _process_period(
        self,
        period: Period,
        period_results: dict[Period, list[dict] | BaseException],
        loaded_periods: set[Period],
        progress: RunProgress,
    ) -> None:
        """Обрабатывает загруженный, успешный или сбойный период."""
        start, end = period

        # Уже сохранённые периоды повторно не обрабатываем.
        if period in loaded_periods:
            progress.skipped_periods += 1

            self.logger.info(
                'Пропускаем ранее загруженный период: %s — %s',
                start,
                end,
            )

            if not progress.has_gap:
                # Продвигаем checkpoint через ранее сохранённый период,
                # только если перед ним не возникло пропуска.
                self.loader.load_records(
                    records=[],
                    period_start=start,
                    period_end=end,
                    update_state=True,
                )

            return

        result = period_results[period]

        # asyncio.gather возвращает исключения как результаты.
        if isinstance(result, BaseException):
            # Системные исключения не считаем обычными ошибками API.
            if not isinstance(result, Exception):
                raise result

            progress.failed_periods += 1
            progress.has_gap = True

            self.logger.error(
                'Ошибка API за период %s — %s: %s',
                start,
                end,
                result,
            )

            self.rejected.save_api_error(start, end, result)
            return

        # Успешный ответ передаём на преобразование и загрузку.
        self._load_period_records(
            period=period,
            records=result,
            progress=progress,
        )

        # Учитываем успешно сохранённый период,
        # чтобы не обрабатывать его повторно.
        loaded_periods.add(period)

    async def load_history_async(
        self,
        last_end: datetime | None,
    ) -> tuple[int, int, int]:
        """Загружает историю и возвращает число периодов, записей и ошибок."""
        # Все счётчики и флаг пропуска хранятся в одном объекте.
        progress = RunProgress()

        # Получаем периоды, уже сохранённые в PostgreSQL.
        loaded_periods = self.state.get_loaded_periods()

        # Определяем начало загрузки и лимит параллельных запросов.
        next_period = self.config.get_period(last_end)
        semaphore = asyncio.Semaphore(API_CONCURRENT_REQUESTS)

        # Последовательно обрабатываем группы периодов.
        while next_period is not None:
            periods, next_period = self._get_period_batch(next_period)

            # Исключаем ранее загруженные периоды из запросов API.
            pending_periods = [period for period in periods if period not in loaded_periods]

            period_results = await self._fetch_periods_async(
                pending_periods,
                semaphore,
            )

            # Обрабатываем периоды в хронологическом порядке,
            # независимо от порядка завершения запросов API.
            for period in periods:
                self._process_period(
                    period=period,
                    period_results=period_results,
                    loaded_periods=loaded_periods,
                    progress=progress,
                )

            self.logger.info(
                'Прогресс: успешно %s, пропущено готовых %s, ошибок API %s, новых записей %s',
                progress.processed_periods,
                progress.skipped_periods,
                progress.failed_periods,
                progress.total_inserted,
            )

        # Возвращаем число обработанных периодов, добавленных записей
        # и периодов, завершившихся ошибкой API.
        return (
            progress.processed_periods,
            progress.total_inserted,
            progress.failed_periods,
        )
