"""Расчёт ежедневной статистики попыток на основе данных PostgreSQL.

Модуль агрегирует записи из таблицы grader_attempts по дням
и формирует показатели активности и успешности пользователей.

Результат подготавливается в формате строк для Google Sheets.
"""

import logging
import os

import psycopg
from dotenv import load_dotenv

from src.constants import DEFAULT_DB_NAME
from src.db_types import NamedDatabaseConnectionConfig


class GraderStatistics:
    """Рассчитывает ежедневные показатели активности из PostgreSQL."""

    # SQL-запрос агрегирует все попытки по календарным датам.
    # FILTER позволяет вычислить отдельные показатели за один запрос.
    QUERY = """
        SELECT
            -- Календарная дата попытки без времени.
            created_at::date AS attempt_date,

            -- Общее количество попыток за день.
            COUNT(*) AS total_attempts,

            -- Количество попыток с правильным результатом.
            COUNT(*) FILTER (
                WHERE is_correct = TRUE
            ) AS successful_attempts,

            -- Доля успешных попыток в процентах.
            -- NULLIF защищает от деления на ноль,
            -- ROUND округляет результат до двух знаков.
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE is_correct = TRUE
                ) / NULLIF(COUNT(*), 0),
                2
            ) AS success_rate,

            -- Количество уникальных пользователей за день.
            COUNT(DISTINCT user_id) AS unique_users,

            -- Количество запусков кода без отправки решения.
            COUNT(*) FILTER (
                WHERE attempt_type = 'run'
            ) AS run_attempts,

            -- Количество отправок решений на проверку.
            COUNT(*) FILTER (
                WHERE attempt_type = 'submit'
            ) AS submit_attempts
        FROM grader_attempts
        GROUP BY created_at::date
        ORDER BY attempt_date;
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Загружает и проверяет настройки подключения к PostgreSQL."""
        self.logger = logger or logging.getLogger('grader_etl')

        # Загружаем параметры подключения из .env.
        load_dotenv()

        # Для локальной базы предусмотрены значения по умолчанию.
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', DEFAULT_DB_NAME)
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD', '')

        # Имя пользователя обязательно для подключения.
        if not db_user:
            raise ValueError('Не задана обязательная переменная окружения DB_USER')

        # Формируем словарь параметров для psycopg.connect().
        self.config: NamedDatabaseConnectionConfig = {
            'host': db_host,
            'port': db_port,
            'dbname': db_name,
            'user': db_user,
            'password': db_password,
        }

    def get_daily_statistics(self) -> list[list]:
        """Возвращает ежедневную статистику для Google Sheets."""
        # Выполняем агрегирующий SQL-запрос и получаем все строки.
        # Контекстные менеджеры закрывают курсор и соединение.
        with psycopg.connect(**self.config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(self.QUERY)
                rows = cursor.fetchall()

        # Преобразуем значения PostgreSQL в типы,
        # подходящие для передачи через Google Sheets API.
        # Порядок элементов соответствует заголовкам таблицы.
        result = [
            [
                row[0].isoformat(),
                int(row[1]),
                int(row[2]),
                float(row[3]),
                int(row[4]),
                int(row[5]),
                int(row[6]),
            ]
            for row in rows
        ]

        self.logger.info(
            'Рассчитана ежедневная статистика: %s дней',
            len(result),
        )

        return result
