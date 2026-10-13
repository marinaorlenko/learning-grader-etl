"""Отправка уведомлений о результатах ETL через Telegram Bot API.

Модуль отправляет текстовые сообщения в указанный Telegram-чат
и обрабатывает сетевые ошибки, HTTP-ошибки и некорректные ответы API.

Результат отправки возвращается в виде логического значения.
"""

import logging
import os

import requests
from dotenv import load_dotenv

from src.constants import TELEGRAM_REQUEST_TIMEOUT


class TelegramNotifier:
    """Отправляет уведомления о работе ETL через Telegram-бота."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Загружает и проверяет настройки Telegram-бота."""
        # Загружаем переменные окружения из .env.
        load_dotenv()

        self.logger = logger or logging.getLogger('grader_etl')

        # Токен бота и идентификатор чата не храним в исходном коде.
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')

        # Оба параметра обязательны для отправки уведомлений.
        if not self.token or not self.chat_id:
            raise ValueError('Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID')

    def send_message(self, message: str) -> bool:
        """Отправляет сообщение и возвращает True при успехе, иначе False."""
        # Формируем адрес метода sendMessage Telegram Bot API.
        url = f'https://api.telegram.org/bot{self.token}/sendMessage'

        try:
            # Отправляем текст сообщения в заданный чат.
            # Тайм-аут ограничивает ожидание ответа сервера.
            response = requests.post(
                url,
                json={
                    'chat_id': self.chat_id,
                    'text': message,
                },
                timeout=TELEGRAM_REQUEST_TIMEOUT,
            )

            # При HTTP-ошибке (4xx или 5xx) вызывается исключение.
            response.raise_for_status()

            # Преобразуем JSON-ответ Telegram в объект Python.
            result = response.json()

            # Проверяем ожидаемый формат ответа API.
            if not isinstance(result, dict):
                raise ValueError('Некорректный формат ответа Telegram API')

            # Telegram использует поле ok для обозначения
            # успешного выполнения запроса.
            if not result.get('ok'):
                raise RuntimeError(result.get('description', 'Ошибка Telegram API'))

            self.logger.info('Уведомление отправлено в Telegram')
            return True

        # Сервер не ответил за установленное время.
        except requests.Timeout:
            self.logger.error('Telegram: превышено время ожидания ответа сервера')
            return False

        # Telegram вернул HTTP-ошибку.
        # Сохраняем статус и ограниченный фрагмент ответа
        # для диагностики причины отказа.
        except requests.HTTPError as error:
            response = error.response
            status = response.status_code if response is not None else 'неизвестен'

            self.logger.error(
                'Telegram: сервер вернул HTTP %s. Ответ: %s',
                status,
                response.text[:500] if response is not None else str(error),
            )
            return False

        # Не удалось установить соединение с Telegram.
        except requests.ConnectionError as error:
            self.logger.error(
                'Telegram: ошибка соединения с сервером: %s',
                error,
            )
            return False

        # Обрабатываем остальные ошибки requests,
        # некорректный JSON и ошибки проверки ответа API.
        except (requests.RequestException, ValueError, RuntimeError) as error:
            self.logger.error(
                'Telegram: ошибка отправки сообщения: %s',
                error,
            )
            return False
