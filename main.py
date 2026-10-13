"""Оркестрация ETL-процесса и отправка итоговых уведомлений.

Модуль объединяет основные компоненты ETL:
инициализацию PostgreSQL, извлечение и обработку данных,
обновление статистики в Google Sheets и отправку уведомлений.
"""

import asyncio
from datetime import datetime
from time import perf_counter

from src.config import ETLConfig
from src.database import DatabaseInitializer
from src.email_notifier import EmailNotifier
from src.extract import GraderExtractor
from src.google_sheets import GoogleSheetsManager
from src.grader_statistics import GraderStatistics
from src.load import GraderLoader
from src.logger import GraderLogger
from src.pipeline import GraderPipeline
from src.rejected import RejectedRecords
from src.state import ETLState
from src.telegram_notifier import TelegramNotifier
from src.transform import GraderTransformer


class GraderETL:
    """Управляет выполнением ETL и формированием итоговых отчётов."""

    def __init__(self) -> None:
        """Создаёт и связывает основные компоненты ETL."""
        # Настраиваем общий логгер для всех компонентов.
        self.logger = GraderLogger().logger
        # Загружаем конфигурацию ETL.
        self.config = ETLConfig()

        # Создаём компоненты для работы с базой данных,
        # API, преобразованием записей и состоянием загрузки.
        self.database = DatabaseInitializer(self.logger)
        self.extractor = GraderExtractor(self.logger)
        self.transformer = GraderTransformer(self.logger)
        self.loader = GraderLoader(self.logger)
        self.state = ETLState(self.logger)
        self.rejected = RejectedRecords(self.logger)
        # Передаём компоненты в пайплайн, который управляет
        # последовательностью обработки исторических периодов.
        self.pipeline = GraderPipeline(
            config=self.config,
            extractor=self.extractor,
            transformer=self.transformer,
            loader=self.loader,
            state=self.state,
            rejected=self.rejected,
            logger=self.logger,
        )

    async def load_history_async(
        self,
        last_end: datetime | None,
    ) -> tuple[int, int, int]:
        """Запускает загрузку исторических данных."""
        # Делегируем обработку периодов основному пайплайну.
        # Получаем количество успешных периодов, новых записей
        # и периодов с ошибками.
        return await self.pipeline.load_history_async(last_end)

    def run(self) -> None:
        """Запускает ETL и формирует итоговый отчёт."""
        # Засекаем время выполнения всего процесса.
        started_at = perf_counter()
        # Начальные значения используются также в отчёте,
        # если выполнение завершится с ошибкой.
        processed_periods = 0
        total_inserted = 0
        failed_periods = 0

        self.logger.info('Запуск ETL')

        try:
            # Создаём базу данных и таблицы при необходимости.
            self.database.initialize()

            # Определяем контрольную точку для продолжения загрузки.
            last_end = self.state.get_last_successful_end()

            # Запускаем асинхронную обработку исторических периодов.
            (
                processed_periods,
                total_inserted,
                failed_periods,
            ) = asyncio.run(
                self.load_history_async(last_end),
            )

            # Если хотя бы один период завершился ошибкой API,
            # считаем результат частично успешным.
            status = 'Частично успешно' if failed_periods else 'Успешно'

            self.logger.info(
                'ETL завершён. Успешных периодов: %s, ошибок API: %s, новых записей: %s',
                processed_periods,
                failed_periods,
                total_inserted,
            )

            # Обновляем статистику после загрузки данных.
            self.update_google_sheets()

            # Отправляем итоговые показатели.
            self.send_report(
                status=status,
                processed_periods=processed_periods,
                total_inserted=total_inserted,
                failed_periods=failed_periods,
            )

        except Exception as error:
            # Логируем непредвиденную ошибку выполнения ETL.
            self.logger.exception('ETL завершился с ошибкой')

            # Пытаемся отправить отчёт об ошибке
            # с доступными на момент сбоя показателями.
            self.send_report(
                status='Ошибка',
                processed_periods=processed_periods,
                total_inserted=total_inserted,
                failed_periods=failed_periods,
                error_message=str(error),
            )

            # Передаём исключение выше, не скрывая сбой ETL.
            raise

        finally:
            # Записываем продолжительность выполнения
            # независимо от успешности завершения.
            self.logger.info(
                'Время выполнения ETL: %.2f сек.',
                perf_counter() - started_at,
            )

    def update_google_sheets(self) -> None:
        """Обновляет ежедневную статистику в Google Sheets."""
        try:
            # Получаем агрегированную статистику из PostgreSQL.
            statistics = GraderStatistics(self.logger)
            records = statistics.get_daily_statistics()

            # Передаём подготовленные строки в Google Sheets.
            google_sheets = GoogleSheetsManager(self.logger)
            google_sheets.update_statistics(records)

        except Exception:
            # Сбой внешнего сервиса не отменяет данные,
            # уже сохранённые в PostgreSQL.
            self.logger.exception(
                'Не удалось обновить Google Sheets. Данные в PostgreSQL сохранены',
            )

    def send_report(
        self,
        status: str,
        processed_periods: int,
        total_inserted: int,
        failed_periods: int = 0,
        error_message: str = '',
    ) -> None:
        """Отправляет итоговый отчёт в Telegram и на почту."""
        # Подбираем значок в зависимости от результата ETL.
        icons = {
            'Успешно': '✅',
            'Частично успешно': '⚠️',
            'Ошибка': '❌',
        }

        # Формируем единый текст отчёта для обоих каналов.
        message = (
            f'{icons.get(status, "ℹ️")} Grader ETL — {status}\n\n'
            f'Обработано периодов: {processed_periods}\n'
            f'Ошибок API: {failed_periods}\n'
            f'Новых записей: {total_inserted}\n\n'
            f'Google Sheets:\n{GoogleSheetsManager.get_spreadsheet_url()}'
        )

        # При аварийном завершении добавляем краткое описание ошибки.
        # Ограничиваем длину текста до 500 символов.
        if error_message:
            message += f'\n\nОшибка: {error_message[:500]}'

        # Telegram и email обрабатываются независимо:
        # ошибка одного канала не блокирует другой.
        try:
            telegram = TelegramNotifier(self.logger)

            if not telegram.send_message(message):
                self.logger.error('Telegram: сообщение не отправлено. Подробная причина указана выше в логе')

        except Exception as error:
            self.logger.exception(
                'Telegram: ошибка подготовки уведомления: %s',
                error,
            )

        # Отправляем тот же отчёт по электронной почте.
        try:
            email = EmailNotifier(self.logger)

            if not email.send_report(
                subject=f'Grader ETL — {status}',
                body=message,
            ):
                self.logger.error('Email: сообщение не отправлено. Подробная причина указана выше в логе')

        except Exception as error:
            self.logger.exception(
                'Email: ошибка подготовки уведомления: %s',
                error,
            )


# Точка входа: запускаем ETL только при прямом выполнении main.py.
if __name__ == '__main__':
    GraderETL().run()
