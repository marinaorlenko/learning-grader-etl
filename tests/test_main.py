"""Тесты оркестрации ETL без реального API и PostgreSQL.

Проверяем управление исторической загрузкой:
последовательность обработки периодов, продолжение после ошибок API,
сохранение пропущенных периодов и возобновление загрузки.

Вместо реальных внешних сервисов используем тестовые реализации
конфигурации, хранилища, API и преобразования данных.
Это позволяет воспроизводить сбои и проверять поведение пайплайна.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from main import GraderETL

from src.pipeline import GraderPipeline

# Моделируем четыре последовательных календарных дня в UTC.
# На этом интервале проверяем успешную загрузку и разные сценарии сбоев.
START = datetime(2023, 5, 15, tzinfo=timezone.utc)
END = START + timedelta(days=4)


class FakeConfig:
    """Формирует дневные периоды в заданных границах."""

    def get_period(self, last_end=None):
        """Возвращает следующий день или None после окончания истории."""
        start = last_end if last_end is not None else START

        if start >= END:
            return None

        return start, min(start + timedelta(days=1), END)


class FakeStorage:
    """Имитирует таблицы etl_state и etl_periods."""

    def __init__(self):
        # Состояние и сведения о периодах храним в памяти,
        # не создавая подключение к PostgreSQL.
        self.last_end = None
        self.loaded_periods = set()
        self.load_calls = []

    def get_last_successful_end(self):
        """Возвращает последнюю сохранённую контрольную дату."""
        return self.last_end

    def get_loaded_periods(self):
        """Возвращает копию множества уже загруженных периодов."""
        return self.loaded_periods.copy()

    def load_records(
        self,
        records,
        period_start,
        period_end,
        update_state=True,
    ):
        """Имитирует сохранение записей, периода и состояния ETL."""
        # Запоминаем каждый вызов для проверки повторных загрузок.
        self.load_calls.append({
            'start': period_start,
            'end': period_end,
            'records': records,
            'update_state': update_state,
        })

        # Период считаем загруженным независимо от числа записей.
        self.loaded_periods.add((period_start, period_end))

        # Контрольную дату обновляем только по указанию пайплайна.
        # Это позволяет не перескакивать через ошибочные периоды.
        if update_state:
            self.last_end = period_end

        return len(records)


class FakeExtractor:
    """Имитирует ответы API и ошибки отдельных периодов."""

    def __init__(self, failed_dates=None):
        # Набор дат с искусственными ошибками можно изменять
        # между запусками, моделируя восстановление API.
        self.failed_dates = set(failed_dates or [])
        self.requested_dates = []

    async def from_api_async(self, start, end, semaphore):
        """Возвращает тестовую запись или имитирует ошибку API."""
        # Используем переданный семафор, как в реальном Extract.
        async with semaphore:
            self.requested_dates.append(start)

            if start in self.failed_dates:
                raise ConnectionError('Ошибка тестового API')

            return [
                {
                    'user_id': f'user_{start:%Y%m%d}',
                }
            ]


class FakeTransformer:
    """Имитирует преобразование записей."""

    def __init__(self):
        self.rejected_records = []

    def transform_records(self, records):
        """Возвращает записи без изменений для изоляции оркестрации."""
        # Здесь не проверяем правила Transform — только передачу данных
        # между этапами и управление загрузкой.
        self.rejected_records = []
        return records


@pytest.fixture
def environment():
    """Создаёт ETL с тестовыми зависимостями."""
    storage = FakeStorage()
    extractor = FakeExtractor()
    saved_errors = []
    saved_rejected = []

    # Создаём экземпляр GraderETL без вызова __init__,
    # чтобы не запускать реальные подключения и инициализацию.
    etl = GraderETL.__new__(GraderETL)

    # Подменяем рабочие компоненты тестовыми реализациями.
    etl.logger = logging.getLogger('test_grader_etl')
    etl.config = FakeConfig()
    etl.state = storage
    etl.loader = storage
    etl.extractor = extractor
    etl.transformer = FakeTransformer()

    # SimpleNamespace создаёт простой объект с нужными методами.
    # Вместо записи rejected-файлов сохраняем аргументы вызовов
    # в списках для последующих проверок.
    etl.rejected = SimpleNamespace(
        save=lambda **kwargs: saved_rejected.append(kwargs),
        save_api_error=lambda start, end, error: saved_errors.append((start, end, error)),
    )

    # Используем настоящий GraderPipeline, но с подменёнными зависимостями.
    # Таким образом проверяем именно управление процессом ETL.
    etl.pipeline = GraderPipeline(
        config=etl.config,
        extractor=etl.extractor,
        transformer=etl.transformer,
        loader=etl.loader,
        state=etl.state,
        rejected=etl.rejected,
        logger=etl.logger,
    )

    return SimpleNamespace(
        etl=etl,
        storage=storage,
        extractor=extractor,
        saved_errors=saved_errors,
        saved_rejected=saved_rejected,
    )


def run_history(environment):
    """Запускает тестовую асинхронную историческую загрузку."""
    # Берём контрольную дату из хранилища, как при обычном запуске.
    last_end = environment.storage.get_last_successful_end()

    # asyncio.run позволяет вызвать асинхронный метод из обычного теста.
    return asyncio.run(environment.etl.load_history_async(last_end))


def test_successful_history(environment):
    """Проверяет успешную загрузку всех четырёх периодов."""
    processed, inserted, failed = run_history(environment)

    # Четыре дня: четыре успешных периода, четыре записи, ноль ошибок.
    assert (processed, inserted, failed) == (4, 4, 0)
    assert environment.storage.last_end == END
    assert len(environment.storage.loaded_periods) == 4
    assert len(environment.extractor.requested_dates) == 4
    assert not environment.saved_errors


def test_api_error_does_not_stop_loading(environment):
    """Проверяет продолжение загрузки после ошибки одного периода."""
    # Имитируем ошибку API на втором дне.
    failed_date = START + timedelta(days=1)
    environment.extractor.failed_dates.add(failed_date)

    processed, inserted, failed = run_history(environment)

    assert (processed, inserted, failed) == (3, 3, 1)

    # Состояние останавливается перед ошибочным периодом.
    assert environment.storage.last_end == failed_date

    # Периоды после ошибки всё равно сохранены.
    assert len(environment.storage.loaded_periods) == 3
    assert (
        failed_date,
        failed_date + timedelta(days=1),
    ) not in environment.storage.loaded_periods

    # Ошибка передана в механизм сохранения rejected.
    assert len(environment.saved_errors) == 1
    assert environment.saved_errors[0][0] == failed_date

    # API был вызван для всех четырёх периодов.
    assert len(environment.extractor.requested_dates) == 4


def test_resume_after_api_error(environment):
    """Проверяет восстановление пропущенного периода при повторном запуске."""
    # Первый запуск: второй день завершается ошибкой.
    failed_date = START + timedelta(days=1)
    environment.extractor.failed_dates.add(failed_date)

    first_result = run_history(environment)

    assert first_result == (3, 3, 1)
    assert environment.storage.last_end == failed_date

    # API восстановился перед повторным запуском.
    # Очищаем историю вызовов, чтобы проверить только второй запуск.
    environment.extractor.failed_dates.clear()
    environment.extractor.requested_dates.clear()
    environment.storage.load_calls.clear()

    second_result = run_history(environment)

    assert second_result == (1, 1, 0)

    # Повторно запрашиваем только ранее ошибочный день.
    assert environment.extractor.requested_dates == [failed_date]

    # Контрольная дата продвинулась через уже загруженные дни.
    assert environment.storage.last_end == END
    assert len(environment.storage.loaded_periods) == 4

    # Успешно загруженные ранее периоды не скачиваются снова.
    # Три вызова Load включают один новый период и операции
    # продвижения состояния через уже сохранённые периоды
    assert len(environment.storage.load_calls) == 3
    assert sum(bool(call['records']) for call in environment.storage.load_calls) == 1


def test_multiple_api_errors(environment):
    """Несколько ошибок не мешают обработке остальных периодов."""
    # Имитируем ошибки в первом и третьем днях.
    environment.extractor.failed_dates.update({
        START,
        START + timedelta(days=2),
    })

    processed, inserted, failed = run_history(environment)

    assert (processed, inserted, failed) == (2, 2, 2)
    assert len(environment.saved_errors) == 2
    assert len(environment.storage.loaded_periods) == 2

    # Первый период не загрузился, поэтому состояние не продвинулось.
    assert environment.storage.last_end is None


def test_already_loaded_history(environment):
    """Проверяет отсутствие повторных запросов после полной загрузки."""
    # Сначала успешно загружаем всю историю.
    assert run_history(environment) == (4, 4, 0)

    environment.extractor.requested_dates.clear()
    environment.storage.load_calls.clear()

    # Повторный запуск не должен обращаться к API или выполнять Load.
    result = run_history(environment)

    assert result == (0, 0, 0)
    assert environment.extractor.requested_dates == []
    assert environment.storage.load_calls == []
    assert environment.storage.last_end == END


def test_empty_period_is_saved(environment):
    """Проверяет сохранение периодов с пустым ответом API."""

    # Подменяем Extract функцией, возвращающей пустой список.
    # Даже без записей период должен считаться успешно обработанным.
    async def empty_api(start, end, semaphore):
        """Имитирует успешный ответ API без записей."""
        return []

    environment.extractor.from_api_async = empty_api

    processed, inserted, failed = run_history(environment)

    assert (processed, inserted, failed) == (4, 0, 0)
    assert len(environment.storage.loaded_periods) == 4
    assert environment.storage.last_end == END
