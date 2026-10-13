"""Извлечение исходных данных из API и локальных JSON-файлов.

Модуль реализует этап Extract ETL-процесса:
получение записей, проверку формата ответа и временных границ,
а также параллельную загрузку с повторными попытками при
временных сетевых ошибках.

Для выполнения блокирующих HTTP-запросов в асинхронном режиме
используется asyncio.to_thread().
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path

import requests

from src.config import ETLConfig
from src.constants import (
    API_DATETIME_FORMAT,
    API_MAX_ATTEMPTS,
    API_REQUEST_TIMEOUT,
)


class GraderExtractor:
    """Получает и проверяет исходные данные из API или JSON-файла."""

    # HTTP-коды временных ошибок, при которых запрос можно повторить.
    # Ошибки авторизации и некорректных параметров сюда не включены.
    RETRYABLE_STATUS_CODES = {
        HTTPStatus.REQUEST_TIMEOUT,
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    }

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Настраивает логирование операций извлечения данных."""
        self.logger = logger or logging.getLogger('grader_etl')

    def from_file(self, file_path: str | Path) -> list[dict]:
        """Читает записи из локального JSON-файла и проверяет тип данных."""
        path = Path(file_path)

        self.logger.info('Чтение данных из файла: %s', path)

        # Открываем файл в UTF-8 и преобразуем JSON в объект Python.
        with path.open(encoding='utf-8') as file:
            records = json.load(file)

        # Ожидаем список записей, как и при получении данных из API.
        if not isinstance(records, list):
            raise ValueError('JSON должен содержать список записей')

        self.logger.info('Из файла получено %s записей', len(records))
        return records

    @staticmethod
    def validate_period(
        records: list[dict],
        start: datetime,
        end: datetime,
    ) -> None:
        """Проверяет, что распознаваемые даты записей входят в период."""
        for record in records:
            # Некорректные структуры записей обрабатываются на этапе Transform.
            if not isinstance(record, dict):
                continue

            created_at = record.get('created_at')

            # Отсутствующие или некорректные типы дат здесь пропускаем.
            if not isinstance(created_at, str):
                continue

            try:
                record_date = datetime.fromisoformat(created_at)
            except ValueError:
                continue

            # Даты без часового пояса интерпретируем как UTC.
            # Даты с часовым поясом приводим к UTC для сравнения.
            if record_date.tzinfo is None:
                record_date = record_date.replace(tzinfo=timezone.utc)
            else:
                record_date = record_date.astimezone(timezone.utc)

            # Используем полуоткрытый интервал [start, end).
            # Это исключает пересечение соседних периодов загрузки.
            if not start <= record_date < end:
                raise ValueError(
                    f'API вернул запись за пределами периода: {record_date}, ожидалось {start} — {end}'
                )

    def from_api(
        self,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        """Запрашивает записи API за период и проверяет полученный ответ."""
        # Проверяем корректность временных границ до обращения к API.
        if start >= end:
            raise ValueError('Начало периода должно быть раньше конца')

        # Получаем адрес API и параметры авторизации из конфигурации.
        config = ETLConfig()

        # Формируем параметры запроса в формате, ожидаемом API.
        params = {
            'client': config.api_client,
            'client_key': config.api_client_key,
            'start': start.strftime(API_DATETIME_FORMAT),
            'end': end.strftime(API_DATETIME_FORMAT),
        }

        # В журнал записываем только границы периода, без ключа API.
        self.logger.info(
            'Запрос к API за период %s — %s UTC',
            params['start'],
            params['end'],
        )

        try:
            # Выполняем HTTP GET с ограничением времени ожидания.
            response = requests.get(
                config.api_url,
                params=params,
                timeout=API_REQUEST_TIMEOUT,
            )
            # Преобразуем неуспешные HTTP-статусы в исключения.
            response.raise_for_status()
            # Разбираем JSON-ответ API.
            records = response.json()

        except (requests.RequestException, ValueError):
            # Фиксируем ошибку вместе с traceback и передаём её выше
            self.logger.exception('Ошибка получения данных из API')
            raise

        # API должен возвращать список, включая пустой список.
        if not isinstance(records, list):
            raise ValueError('API вернул данные не в формате списка')

        # Проверяем, что API не вернул записи из других периодов.
        self.validate_period(records, start, end)

        self.logger.info(
            'Из API получено %s записей',
            len(records),
        )

        return records

    @classmethod
    def is_retryable(cls, error: requests.RequestException) -> bool:
        """Определяет, допускает ли ошибка повторный HTTP-запрос."""
        if isinstance(error, requests.HTTPError):
            # Для HTTP-ошибок решение зависит от кода ответа сервера
            response = error.response
            return response is not None and response.status_code in cls.RETRYABLE_STATUS_CODES

        # Тайм-ауты и ошибки соединения считаем временными.
        return isinstance(
            error,
            (requests.Timeout, requests.ConnectionError),
        )

    async def from_api_async(
        self,
        start: datetime,
        end: datetime,
        semaphore: asyncio.Semaphore,
        max_attempts: int = API_MAX_ATTEMPTS,
    ) -> list[dict]:
        """Получает записи асинхронно с ограничением параллельности и повторами."""
        # Семафор ограничивает число одновременно обрабатываемых
        # периодов, чтобы не перегружать API.
        async with semaphore:
            for attempt in range(1, max_attempts + 1):
                try:
                    # requests.get — блокирующая операция.
                    # asyncio.to_thread выполняет её в отдельном потоке,
                    # не блокируя основной цикл событий.
                    return await asyncio.to_thread(
                        self.from_api,
                        start,
                        end,
                    )

                except requests.RequestException as error:
                    # Не повторяем запрос при постоянной ошибке
                    # или после исчерпания максимального числа попыток.
                    if not self.is_retryable(error) or attempt == max_attempts:
                        self.logger.error(
                            'Запрос за период %s — %s завершился ошибкой после %s попыток: %s',
                            start,
                            end,
                            attempt,
                            error,
                        )
                        raise

                    # Экспоненциальная задержка между попытками:
                    # после первой ошибки — 2 сек., после второй — 4 сек.
                    delay = 2**attempt

                    self.logger.warning(
                        'Временная ошибка API за период %s — %s. Повтор через %s сек. Попытка %s/%s',
                        start,
                        end,
                        delay,
                        attempt + 1,
                        max_attempts,
                    )

                    # Асинхронное ожидание не блокирует другие задачи.
                    await asyncio.sleep(delay)

        # Защитная проверка на случай выхода из цикла без результата.
        raise RuntimeError('Не удалось получить данные из API')
