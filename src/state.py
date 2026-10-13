"""Чтение состояния последней успешной обработки ETL.

Модуль получает из PostgreSQL контрольную точку ETL
и список ранее загруженных периодов.

Эти данные используются для возобновления исторической загрузки
без повторного скачивания уже сохранённых периодов.
"""

import logging
import os
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

from src.constants import DEFAULT_DB_NAME, PIPELINE_NAME
from src.db_types import NamedDatabaseConnectionConfig


class ETLState:
    """Читает контрольную точку ETL и ранее загруженные периоды."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Загружает и проверяет настройки подключения к PostgreSQL."""
        # Загружаем переменные окружения из .env.
        load_dotenv()

        self.logger = logger or logging.getLogger('grader_etl')

        # Имя пользователя обязательно для подключения к базе.
        db_user = os.getenv('DB_USER')

        if not db_user:
            raise ValueError('Не задана обязательная переменная окружения DB_USER')

        # Формируем параметры подключения с локальными
        # значениями по умолчанию для необязательных настроек.
        self.config: NamedDatabaseConnectionConfig = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'dbname': os.getenv('DB_NAME', DEFAULT_DB_NAME),
            'user': db_user,
            'password': os.getenv('DB_PASSWORD', ''),
        }

    def get_last_successful_end(self) -> datetime | None:
        """Возвращает конец последнего непрерывного успешного периода в UTC."""
        # Контрольная точка хранится отдельно для каждого пайплайна.
        # Она показывает, до какого момента история обработана
        # без пропущенных периодов.

        query = """
            SELECT last_successful_end
            FROM etl_state
            WHERE pipeline_name = %s
        """

        # Получаем сохранённую контрольную точку из PostgreSQL.
        with psycopg.connect(**self.config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (PIPELINE_NAME,))
                result = cursor.fetchone()

        # Если состояние отсутствует, загрузка ещё не выполнялась
        # либо контрольная точка пока не была сохранена.
        if result is None:
            self.logger.info('Предыдущих успешных загрузок нет')
            return None

        # Приводим время к UTC для согласованности
        # с периодами, формируемыми ETLConfig.
        last_end = result[0].astimezone(timezone.utc)

        self.logger.info(
            'Последний непрерывный успешный период заканчивается: %s',
            last_end,
        )

        return last_end

    def get_loaded_periods(self) -> set[tuple[datetime, datetime]]:
        """Возвращает интервалы успешно загруженных периодов в UTC."""
        # Получаем все сохранённые периоды текущего пайплайна,
        # включая периоды после возможного пропуска в истории.

        query = """
            SELECT period_start, period_end
            FROM etl_periods
            WHERE pipeline_name = %s
        """

        with psycopg.connect(**self.config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (PIPELINE_NAME,))
                rows = cursor.fetchall()

        # Преобразуем границы периодов в UTC.
        # Используем множество для быстрого поиска периода
        # и автоматического исключения повторяющихся интервалов.
        periods = {
            (
                start.astimezone(timezone.utc),
                end.astimezone(timezone.utc),
            )
            for start, end in rows
        }

        self.logger.info(
            'Найдено ранее загруженных периодов: %s',
            len(periods),
        )

        return periods
