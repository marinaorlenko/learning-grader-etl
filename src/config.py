"""Загрузка настроек API и определение временных интервалов ETL.

Модуль отвечает за чтение обязательных параметров подключения из .env
и расчёт следующего периода исторической загрузки.

Границы истории и длительность периода задаются в src/constants.py.
"""

import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from src.constants import (
    HISTORY_END,
    HISTORY_START,
    LOAD_UNTIL_NOW,
    PERIOD_DAYS,
)


class ETLConfig:
    """Настройки подключения к API и расчёт периодов загрузки."""

    def __init__(self) -> None:
        """Загружает настройки API из .env и проверяет их наличие."""
        # Загружаем переменные из .env в окружение приложения.
        # Ключ доступа не хранится непосредственно в исходном коде.
        load_dotenv()

        self.api_url = os.getenv('API_URL')
        self.api_client = os.getenv('API_CLIENT')
        self.api_client_key = os.getenv('API_CLIENT_KEY')

        # Без обязательных параметров выполнение ETL невозможно.
        # Прерываем инициализацию до первого обращения к API.
        if not all((
            self.api_url,
            self.api_client,
            self.api_client_key,
        )):
            raise ValueError('Не заданы обязательные настройки API: API_URL, API_CLIENT или API_CLIENT_KEY')

    @staticmethod
    def get_period(
        last_successful_end: datetime | None = None,
    ) -> tuple[datetime, datetime] | None:
        """Возвращает следующий период загрузки в UTC.

        Или None, если история завершена.
        """
        # В рабочем режиме верхняя граница определяется текущим временем.
        # Для ограниченной исторической загрузки используется HISTORY_END.
        end_limit = datetime.now(timezone.utc) if LOAD_UNTIL_NOW else HISTORY_END

        # При возобновлении продолжаем с последней контрольной даты.
        # При первом запуске начинаем с HISTORY_START.
        start = last_successful_end if last_successful_end is not None else HISTORY_START

        # Проверяем, чтобы дата была с часовым поясом, чтобы избежать
        # неоднозначности при сравнении и обработке временных интервалов.
        if start.tzinfo is None:
            raise ValueError('Дата начала периода должна содержать часовой пояс')

        # Приводим начало периода к UTC для единообразной обработки.
        start = start.astimezone(timezone.utc)

        # Если достигли верхней границы, новых периодов больше нет.
        if start >= end_limit:
            return None

        # Ограничиваем длину периода настройкой PERIOD_DAYS.
        # Последний интервал может быть короче, если достигнут end_limit.
        end = min(
            start + timedelta(days=PERIOD_DAYS),
            end_limit,
        )

        return start, end
