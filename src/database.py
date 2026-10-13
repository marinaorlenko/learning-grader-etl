"""Инициализация базы данных и таблиц PostgreSQL.

Модуль подготавливает подключение к PostgreSQL, создаёт базу данных
при её отсутствии и выполняет SQL-скрипт создания таблиц и индексов.

Параметры подключения загружаются из переменных окружения.
"""

import logging
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql

from src.constants import DATABASE_INIT_SQL_PATH, DEFAULT_DB_NAME
from src.db_types import DatabaseConnectionConfig


class DatabaseInitializer:
    """Создает базу данных и инициализирует ее структуру."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Загружает и проверяет параметры подключения к PostgreSQL."""
        # Загружаем переменные окружения из .env.
        load_dotenv()

        # Используем переданный логгер или стандартный логгер проекта.
        self.logger = logger or logging.getLogger('grader_etl')

        # Имя пользователя обязательно для подключения к PostgreSQL.
        db_user = os.getenv('DB_USER')

        if not db_user:
            raise ValueError('Не указан DB_USER в .env')

        # Формируем параметры подключения с настройками по умолчанию
        # для локального PostgreSQL.
        self.config: DatabaseConnectionConfig = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'user': db_user,
            'password': os.getenv('DB_PASSWORD', ''),
        }

        # Имя создаваемой базы берём из .env или константы проекта.
        self.db_name = os.getenv('DB_NAME', DEFAULT_DB_NAME)

    def create_database(self) -> None:
        """Создаёт базу данных, если она еще не существует."""
        self.logger.info('Проверка базы данных: %s', self.db_name)

        # Подключаемся к служебной базе postgres.
        # CREATE DATABASE требует выполнения вне транзакции,
        # поэтому используем autocommit=True.
        with psycopg.connect(
            **self.config,
            dbname='postgres',
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                # Проверяем существование базы по системному каталогу.
                cursor.execute(
                    'SELECT 1 FROM pg_database WHERE datname = %s',
                    (self.db_name,),
                )

                # Если база существует, повторно её не создаём.
                if cursor.fetchone():
                    self.logger.info(
                        'База данных %s уже существует',
                        self.db_name,
                    )
                    return

                # sql.Identifier безопасно формирует SQL-идентификатор
                # из имени базы данных.
                cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.db_name)))
                self.logger.info(
                    'База данных %s успешно создана',
                    self.db_name,
                )

    def create_tables(self) -> None:
        """Выполняет SQL-скрипт создания таблиц и индексов."""
        # Определяем абсолютный путь к SQL-файлу относительно проекта.
        # Это позволяет запускать ETL из разных рабочих каталогов.
        sql_path = Path(__file__).resolve().parent.parent / DATABASE_INIT_SQL_PATH
        # Читаем SQL-скрипт в кодировке UTF-8.
        query = sql_path.read_text(encoding='utf-8')

        # Подключаемся уже к целевой базе данных.
        # При успешном завершении контекста изменения фиксируются.
        with psycopg.connect(
            **self.config,
            dbname=self.db_name,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)

        self.logger.info('Таблицы и индексы PostgreSQL проверены')

    def initialize(self) -> None:
        """Последовательно создаёт базу данных и её таблицы."""
        try:
            # Сначала обеспечиваем существование самой базы.
            self.create_database()
            # Затем создаём необходимые таблицы и индексы.
            self.create_tables()

        except psycopg.Error:
            # Записываем исключение вместе с traceback
            # и передаём его выше для обработки приложением.
            self.logger.exception('Ошибка инициализации PostgreSQL')
            raise
