"""Транзакционная загрузка записей и состояния ETL в PostgreSQL.

Модуль реализует этап Load ETL-процесса:
сохраняет обработанные записи, фиксирует загруженные периоды
и при необходимости обновляет контрольную точку ETL.

Все операции одного периода выполняются в общей транзакции.
"""

import logging
import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv

from src.constants import DEFAULT_DB_NAME, PIPELINE_NAME
from src.db_types import NamedDatabaseConnectionConfig


class GraderLoader:
    """Загружает записи и фиксирует состояние ETL в PostgreSQL."""

    # Поля записи в порядке, соответствующем INSERT_QUERY.
    FIELDS = (
        'user_id',
        'oauth_consumer_key',
        'lis_result_sourcedid',
        'lis_outcome_service_url',
        'is_correct',
        'attempt_type',
        'created_at',
    )

    # Добавляем обработанные записи в основную таблицу.
    # ON CONFLICT DO NOTHING пропускает конфликты уникальности.
    # RETURNING позволяет определить, была ли вставлена запись.
    INSERT_QUERY = """
        INSERT INTO grader_attempts (
            user_id,
            oauth_consumer_key,
            lis_result_sourcedid,
            lis_outcome_service_url,
            is_correct,
            attempt_type,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING user_id
    """

    # Фиксируем успешно обработанный период загрузки.
    # Повторная вставка уже существующего периода пропускается
    # при конфликте с ограничением уникальности.
    PERIOD_QUERY = """
        INSERT INTO etl_periods (
            pipeline_name,
            period_start,
            period_end
        )
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
    """

    # Сохраняем контрольную точку ETL — конец последнего
    # успешно обработанного непрерывного периода.
    # При наличии записи обновляем её значение.
    STATE_QUERY = """
        INSERT INTO etl_state (
            pipeline_name,
            last_successful_end
        )
        VALUES (%s, %s)
        ON CONFLICT (pipeline_name)
        DO UPDATE SET
            last_successful_end = EXCLUDED.last_successful_end
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Загружает и проверяет настройки подключения к PostgreSQL."""
        # Получаем параметры подключения из переменных окружения.
        load_dotenv()

        self.logger = logger or logging.getLogger('grader_etl')

        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', DEFAULT_DB_NAME)
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD', '')

        # Имя пользователя обязательно для подключения к базе.
        if not db_user:
            raise ValueError('Не задана обязательная переменная окружения DB_USER')

        # Формируем параметры подключения для psycopg.
        self.config: NamedDatabaseConnectionConfig = {
            'host': db_host,
            'port': db_port,
            'dbname': db_name,
            'user': db_user,
            'password': db_password,
        }

    def load_records(
        self,
        records: list[dict],
        period_start: datetime,
        period_end: datetime,
        update_state: bool = True,
    ) -> int:
        """Сохраняет записи и период, при необходимости обновляя состояние ETL."""
        # Проверяем корректность временных границ периода.
        if period_start >= period_end:
            raise ValueError('Начало периода должно быть раньше окончания')

        # Преобразуем словари в кортежи значений.
        # Порядок значений определяется константой FIELDS.
        values = [tuple(record[field] for field in self.FIELDS) for record in records]

        self.logger.info(
            'Получено для загрузки: %s записей',
            len(values),
        )

        # Счётчик фактически добавленных записей без дубликатов.
        inserted_count = 0

        try:
            # Все операции выполняются в одной транзакции.
            # При успешном завершении контекста изменения фиксируются,
            # при исключении PostgreSQL откатывает транзакцию.
            with psycopg.connect(**self.config) as connection:
                with connection.cursor() as cursor:
                    # Вставляем записи по одной.
                    for row in values:
                        cursor.execute(self.INSERT_QUERY, row)
                        # RETURNING возвращает строку только при вставке.
                        # При конфликте fetchone() возвращает None.
                        inserted_count += int(cursor.fetchone() is not None)

                    # Отмечаем период как успешно загруженный,
                    # даже если список записей был пустым.
                    cursor.execute(
                        self.PERIOD_QUERY,
                        (PIPELINE_NAME, period_start, period_end),
                    )

                    # Обновляем контрольную точку только при разрешении.
                    # Это позволяет сохранять отдельные периоды истории,
                    # не продвигая состояние через пропущенные интервалы.
                    if update_state:
                        cursor.execute(
                            self.STATE_QUERY,
                            (PIPELINE_NAME, period_end),
                        )

        except psycopg.Error:
            # Фиксируем ошибку вместе с traceback.
            # Исключение передаётся вызывающему коду.
            self.logger.exception('Ошибка транзакции загрузки PostgreSQL')
            raise

        # После успешного завершения транзакции записываем итоги.
        self.logger.info(
            'Добавлено новых записей: %s',
            inserted_count,
        )
        self.logger.info(
            'Пропущено дубликатов: %s',
            len(values) - inserted_count,
        )
        self.logger.info(
            'Успешный период сохранён: %s — %s',
            period_start,
            period_end,
        )

        if update_state:
            self.logger.info(
                'Состояние ETL сохранено: %s',
                period_end,
            )
        else:
            self.logger.info('Состояние ETL не обновлено')

        return inserted_count
