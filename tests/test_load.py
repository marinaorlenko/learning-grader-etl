"""Тесты загрузки данных в PostgreSQL без реального подключения.

Проверяем логику GraderLoader: сохранение записей и периодов,
обновление состояния ETL, обработку дубликатов и откат транзакций.

Для изоляции тестов используем имитации соединения и курсора PostgreSQL.
Через monkeypatch подменяем psycopg.connect, поэтому тесты
не требуют работающей базы данных.
"""

from datetime import datetime, timezone

import psycopg
import pytest

from src.load import GraderLoader

# Общий тестовый период — один календарный день в UTC.
PERIOD_START = datetime(2023, 5, 15, tzinfo=timezone.utc)
PERIOD_END = datetime(2023, 5, 16, tzinfo=timezone.utc)


class FakeCursor:
    """Имитирует выполнение SQL-запросов через курсор PostgreSQL."""

    def __init__(
        self,
        inserted=True,
        fail_state=False,
        fail_period=False,
    ):
        # Флаги позволяют моделировать успешную вставку, дубликат
        # или ошибку на конкретном этапе загрузки.
        self.inserted = inserted
        self.fail_state = fail_state
        self.fail_period = fail_period
        # Сохраняем выполненные запросы для проверки их порядка и состава.
        self.queries = []

    def __enter__(self):
        """Возвращает курсор при входе в контекстный менеджер."""
        return self

    def __exit__(self, *args):
        """Завершает контекст без подавления исключений."""
        return False

    def execute(self, query, params):
        """Фиксирует SQL-запрос или имитирует ошибку базы данных."""
        # Искусственно вызываем ошибки сохранения периода или состояния,
        # чтобы проверить откат транзакции в GraderLoader.
        if self.fail_period and 'INSERT INTO etl_periods' in query:
            raise psycopg.DatabaseError('Ошибка сохранения периода')

        if self.fail_state and 'INSERT INTO etl_state' in query:
            raise psycopg.DatabaseError('Ошибка сохранения состояния')

        self.queries.append((query, params))

    def fetchone(self):
        """Имитирует RETURNING при успешной вставке или конфликте."""
        # При дубликате INSERT ... ON CONFLICT DO NOTHING
        # не возвращает вставленную строку.
        return ('user_001',) if self.inserted else None


class FakeConnection:
    """Имитирует соединение и результат транзакции PostgreSQL."""

    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        """Возвращает тестовое соединение."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Отмечает фиксацию транзакции или откат при исключении."""
        # Успешное завершение контекста имитирует COMMIT,
        # а выход с исключением — ROLLBACK.
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True

        return False

    def cursor(self):
        """Возвращает тестовый курсор вместо настоящего курсора БД."""
        return self.fake_cursor


@pytest.fixture
def loader(monkeypatch):
    """Создаёт загрузчик с тестовыми настройками подключения."""
    # Подменяем переменную окружения только на время теста.
    monkeypatch.setenv('DB_USER', 'test_user')
    return GraderLoader()


@pytest.fixture
def record():
    """Возвращает тестовую запись в формате таблицы grader_attempts."""
    # Одна запись используется в разных сценариях загрузки.
    return {
        'user_id': 'user_001',
        'oauth_consumer_key': 'test_key',
        'lis_result_sourcedid': 'course_001',
        'lis_outcome_service_url': 'https://example.com/grade',
        'is_correct': True,
        'attempt_type': 'submit',
        'created_at': datetime(2023, 5, 15, 12, 30, tzinfo=timezone.utc),
    }


def test_load_success(monkeypatch, loader, record):
    """Проверяет успешную загрузку записи, периода и состояния ETL."""
    cursor = FakeCursor()
    connection = FakeConnection(cursor)

    # Подменяем реальное подключение заранее подготовленным соединением.
    monkeypatch.setattr(
        psycopg,
        'connect',
        lambda **kwargs: connection,
    )

    result = loader.load_records(
        [record],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
    )

    # Ожидаем одну новую запись и успешную фиксацию транзакции.
    assert result == 1
    assert connection.committed
    assert not connection.rolled_back
    assert len(cursor.queries) == 3

    # Проверяем последовательность трёх операций:
    # запись попытки, регистрация периода, обновление состояния.
    assert 'INSERT INTO grader_attempts' in cursor.queries[0][0]
    assert 'INSERT INTO etl_periods' in cursor.queries[1][0]
    assert 'INSERT INTO etl_state' in cursor.queries[2][0]


def test_duplicate_record(monkeypatch, loader, record):
    """Проверяет, что дубликат не учитывается как новая запись."""
    # Имитируем отсутствие новой строки после INSERT.
    cursor = FakeCursor(inserted=False)
    connection = FakeConnection(cursor)

    monkeypatch.setattr(
        psycopg,
        'connect',
        lambda **kwargs: connection,
    )

    result = loader.load_records(
        [record],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
    )

    # Дубликат не увеличивает счётчик, но период успешно обрабатывается.
    assert result == 0
    assert connection.committed
    assert not connection.rolled_back
    assert len(cursor.queries) == 3


def test_empty_records(monkeypatch, loader):
    """Проверяет регистрацию периода, в котором нет записей."""
    cursor = FakeCursor()
    connection = FakeConnection(cursor)

    monkeypatch.setattr(
        psycopg,
        'connect',
        lambda **kwargs: connection,
    )

    result = loader.load_records(
        [],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
    )

    # Пустой период считается успешно обработанным.
    # Записи не вставляются, но период и состояние сохраняются.
    assert result == 0
    assert connection.committed
    assert len(cursor.queries) == 2

    assert 'INSERT INTO etl_periods' in cursor.queries[0][0]
    assert 'INSERT INTO etl_state' in cursor.queries[1][0]


def test_load_without_state_update(monkeypatch, loader, record):
    """Проверяет сохранение данных без обновления состояния ETL."""
    cursor = FakeCursor()
    connection = FakeConnection(cursor)

    monkeypatch.setattr(
        psycopg,
        'connect',
        lambda **kwargs: connection,
    )

    # Режим update_state=False используется, когда период загружен,
    # но непрерывная последовательность успешных периодов ещё не восстановлена.
    result = loader.load_records(
        [record],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        update_state=False,
    )

    assert result == 1
    assert connection.committed
    assert len(cursor.queries) == 2

    assert 'INSERT INTO grader_attempts' in cursor.queries[0][0]
    assert 'INSERT INTO etl_periods' in cursor.queries[1][0]

    # Убеждаемся, что SQL-запрос обновления etl_state не выполнялся.
    assert not any('INSERT INTO etl_state' in query for query, _ in cursor.queries)


def test_transaction_rollback(monkeypatch, loader, record):
    """Проверяет откат транзакции при ошибке сохранения состояния."""
    # Имитируем сбой на последнем этапе загрузки — обновлении etl_state.
    cursor = FakeCursor(fail_state=True)
    connection = FakeConnection(cursor)

    monkeypatch.setattr(
        psycopg,
        'connect',
        lambda **kwargs: connection,
    )

    with pytest.raises(psycopg.DatabaseError):
        loader.load_records(
            [record],
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    # При ошибке транзакция не должна считаться зафиксированной.
    assert connection.rolled_back
    assert not connection.committed


def test_period_rollback(monkeypatch, loader, record):
    """Проверяет откат транзакции при ошибке регистрации периода."""
    # Имитируем сбой на этапе сохранения etl_periods.
    cursor = FakeCursor(fail_period=True)
    connection = FakeConnection(cursor)

    monkeypatch.setattr(
        psycopg,
        'connect',
        lambda **kwargs: connection,
    )

    with pytest.raises(psycopg.DatabaseError):
        loader.load_records(
            [record],
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert connection.rolled_back
    assert not connection.committed


def test_invalid_period(loader):
    """Проверяет отклонение периода с неправильными границами."""
    # Проверка дат должна выполняться до подключения к PostgreSQL.
    with pytest.raises(ValueError):
        loader.load_records(
            [],
            period_start=PERIOD_END,
            period_end=PERIOD_START,
        )
