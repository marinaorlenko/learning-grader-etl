"""Интеграционные тесты GraderLoader с реальной PostgreSQL.

Проверяем взаимодействие загрузчика с настоящей базой данных:
сохранение записей, регистрацию обработанных периодов,
обновление состояния ETL, защиту от дубликатов и откат транзакций.

Каждый тест выполняется в отдельной временной схеме PostgreSQL.
Это позволяет проверить реальные SQL-операции, не изменяя
основные таблицы проекта.
"""

from datetime import datetime, timezone
from uuid import uuid4

import psycopg
import pytest

from src.load import GraderLoader

# Общий тестовый период продолжительностью один день в UTC.
PERIOD_START = datetime(2023, 5, 15, tzinfo=timezone.utc)
PERIOD_END = datetime(2023, 5, 16, tzinfo=timezone.utc)


@pytest.fixture
def integration_db(monkeypatch):
    """Создаёт временную схему PostgreSQL и удаляет её после теста."""
    loader = GraderLoader()
    # UUID обеспечивает уникальное имя схемы для каждого теста,
    # чтобы тесты не конфликтовали между собой.
    schema = f'test_etl_{uuid4().hex}'

    # Настраиваем search_path: SQL-запросы загрузчика будут
    # обращаться к таблицам временной схемы вместо public.
    config = loader.config.copy()
    config['options'] = f'-c search_path={schema}'

    # Создаём временную схему и копии рабочих таблиц.
    # INCLUDING ALL переносит структуру таблиц и ограничения,
    # чтобы тестирование соответствовало реальной базе.
    with psycopg.connect(**loader.config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')

            cursor.execute(
                f'''
                CREATE TABLE "{schema}".grader_attempts (
                    LIKE public.grader_attempts
                    INCLUDING ALL
                )
                '''
            )

            cursor.execute(
                f'''
                CREATE TABLE "{schema}".etl_periods (
                    LIKE public.etl_periods
                    INCLUDING ALL
                )
                '''
            )

            cursor.execute(
                f'''
                CREATE TABLE "{schema}".etl_state (
                    LIKE public.etl_state
                    INCLUDING ALL
                )
                '''
            )

    # Передаём загрузчику конфигурацию с тестовой схемой.
    loader.config = config

    try:
        # yield передаёт объект загрузчика и имя схемы тесту.
        # После завершения теста выполняется блок finally.
        yield loader, schema
    finally:
        # Удаляем только созданную тестовую схему.
        # CASCADE удаляет также все её таблицы и тестовые записи.
        with psycopg.connect(**config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
def record():
    """Возвращает одну тестовую запись в формате GraderLoader."""
    # Данные соответствуют структуре grader_attempts.
    # Запись используется повторно для проверки разных сценариев.
    return {
        'user_id': 'integration_test_user',
        'oauth_consumer_key': '',
        'lis_result_sourcedid': 'test_course',
        'lis_outcome_service_url': None,
        'is_correct': True,
        'attempt_type': 'submit',
        'created_at': datetime(
            2023,
            5,
            15,
            12,
            30,
            tzinfo=timezone.utc,
        ),
    }


def test_real_load(integration_db, record):
    """Проверяет запись данных, периода, состояния и отсутствие дубликатов."""
    loader, schema = integration_db

    # Загружаем одну запись и регистрируем успешно обработанный период.
    inserted = loader.load_records(
        records=[record],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
    )

    assert inserted == 1

    # Проверяем результат непосредственно в PostgreSQL,
    # а не только возвращаемое значение метода загрузки.
    with psycopg.connect(**loader.config) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM grader_attempts')
            assert cursor.fetchone()[0] == 1

            cursor.execute('SELECT COUNT(*) FROM etl_periods')
            assert cursor.fetchone()[0] == 1

            cursor.execute('SELECT last_successful_end FROM etl_state')
            assert cursor.fetchone()[0] == PERIOD_END

    # Повторная загрузка того же периода и записи
    # не должна создавать дубликаты.
    inserted_again = loader.load_records(
        records=[record],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
    )

    assert inserted_again == 0


def test_real_rollback(integration_db, record):
    """Проверяет откат загрузки при ошибке сохранения состояния."""
    loader, schema = integration_db

    # Удаляем тестовую таблицу состояния, чтобы вызвать ошибку.
    # Это позволяет проверить откат всей транзакции.
    with psycopg.connect(**loader.config) as connection:
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE etl_state')

    # При попытке обновить отсутствующую таблицу ожидаем
    # исключение PostgreSQL.
    with pytest.raises(psycopg.Error):
        loader.load_records(
            records=[record],
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    # Запись и период должны откатиться вместе.
    # Иначе в базе могли бы остаться частично загруженные данные.
    with psycopg.connect(**loader.config) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM grader_attempts')
            assert cursor.fetchone()[0] == 0

            cursor.execute('SELECT COUNT(*) FROM etl_periods')
            assert cursor.fetchone()[0] == 0


def test_real_load_without_state(integration_db, record):
    """Проверяет загрузку периода без продвижения состояния ETL."""
    loader, schema = integration_db

    # update_state=False позволяет сохранить успешно обработанный
    # период, не сдвигая общую точку продолжения загрузки.
    # Это важно, когда между успешными периодами есть пропуск.
    inserted = loader.load_records(
        records=[record],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        update_state=False,
    )

    assert inserted == 1

    with psycopg.connect(**loader.config) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM grader_attempts')
            assert cursor.fetchone()[0] == 1

            cursor.execute('SELECT COUNT(*) FROM etl_periods')
            assert cursor.fetchone()[0] == 1

            # Состояние ETL не должно обновляться.
            cursor.execute('SELECT COUNT(*) FROM etl_state')
            assert cursor.fetchone()[0] == 0


def test_real_empty_period(integration_db):
    """Проверяет регистрацию успешно обработанного периода без записей."""
    loader, schema = integration_db

    # Пустой ответ API не означает ошибку загрузки.
    # Такой период необходимо зарегистрировать как обработанный,
    # чтобы ETL не запрашивал его повторно.
    inserted = loader.load_records(
        records=[],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
    )

    assert inserted == 0

    with psycopg.connect(**loader.config) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM grader_attempts')
            assert cursor.fetchone()[0] == 0

            # Даже без записей период должен быть отмечен успешным.
            cursor.execute('SELECT COUNT(*) FROM etl_periods')
            assert cursor.fetchone()[0] == 1

            # Точка продолжения сдвигается к концу пустого периода.
            cursor.execute('SELECT last_successful_end FROM etl_state')
            assert cursor.fetchone()[0] == PERIOD_END
