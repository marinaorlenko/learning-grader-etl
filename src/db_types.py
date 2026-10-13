"""Типы конфигурации подключения к PostgreSQL."""

from typing import TypedDict


class DatabaseConnectionConfig(TypedDict):
    """Параметры подключения без имени базы данных."""

    host: str
    port: str
    user: str
    password: str


class NamedDatabaseConnectionConfig(DatabaseConnectionConfig):
    """Параметры подключения к конкретной базе данных."""

    dbname: str
