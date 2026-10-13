"""Отправка отчётов ETL по электронной почте через SMTP.

Модуль формирует текстовые email-уведомления и отправляет их
через SMTP-сервер Яндекса с авторизацией OAuth2 (XOAUTH2).

Поддерживаются защищённые соединения SMTP SSL и STARTTLS.
Ошибки авторизации и отправки фиксируются в журнале ETL.
"""

import base64
import logging
import os
import smtplib
import ssl
from email.message import EmailMessage

from dotenv import load_dotenv

from src.constants import SMTP_TIMEOUT


class EmailNotifier:
    """Отправляет отчёты ETL через Яндекс SMTP с OAuth2."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Загружает и проверяет настройки отправки email-уведомлений."""
        # Читаем параметры SMTP и OAuth-токен из переменных окружения.
        # Секретные данные не хранятся непосредственно в исходном коде.
        load_dotenv()

        self.logger = logger or logging.getLogger('grader_etl')
        self.host = os.getenv('SMTP_HOST', 'smtp.yandex.com')
        self.port = int(os.getenv('SMTP_PORT', '465'))
        self.user = os.getenv('SMTP_USER')
        self.token = os.getenv('SMTP_OAUTH_TOKEN')
        self.sender = os.getenv('SMTP_FROM_EMAIL') or self.user
        self.recipient = os.getenv('SMTP_TO_EMAIL')
        # SSL устанавливает защищённое соединение сразу.
        # STARTTLS переводит уже открытое SMTP-соединение на TLS.
        self.use_ssl = os.getenv('SMTP_USE_SSL', 'true').lower() == 'true'
        self.use_tls = os.getenv('SMTP_USE_TLS', 'false').lower() == 'true'

        # Проверяем обязательные параметры до попытки отправки.
        if not all([self.user, self.token, self.sender, self.recipient]):
            raise ValueError('Не заполнены настройки SMTP в .env')

        # Для одного подключения должен быть выбран ровно один режим.
        if self.use_ssl == self.use_tls:
            raise ValueError('Укажи только один режим: SMTP_USE_SSL или SMTP_USE_TLS')

    def send_report(self, subject: str, body: str) -> bool:
        """Отправляет текстовый отчёт и возвращает True при успехе."""
        # Формируем стандартное email-сообщение с темой,
        # отправителем, получателем и текстом отчёта.
        message = EmailMessage()
        message['Subject'] = subject
        message['From'] = self.sender
        message['To'] = self.recipient
        message.set_content(body)

        # XOAUTH2 передаёт OAuth-токен вместо обычного SMTP-пароля.
        # Символы \x01 являются разделителями полей протокола SASL.
        # SMTP ожидает строку авторизации в кодировке Base64.
        auth_string = f'user={self.user}\x01auth=Bearer {self.token}\x01\x01'
        encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('ascii')

        try:
            # Создаём SSL-контекст с проверкой сертификата сервера.
            context = ssl.create_default_context()

            server: smtplib.SMTP

            # Выбираем тип подключения в зависимости от настроек.
            if self.use_ssl:
                server = smtplib.SMTP_SSL(
                    self.host,
                    self.port,
                    context=context,
                    timeout=SMTP_TIMEOUT,
                )
            else:
                server = smtplib.SMTP(
                    self.host,
                    self.port,
                    timeout=SMTP_TIMEOUT,
                )

            with server:
                # EHLO сообщает серверу о клиенте и запрашивает
                # доступные возможности SMTP.
                server.ehlo()

                # При STARTTLS сначала включаем шифрование,
                # затем повторно запрашиваем возможности сервера.
                if self.use_tls:
                    server.starttls(context=context)
                    server.ehlo()

                # Выполняем авторизацию через механизм SASL XOAUTH2.
                code, response = server.docmd(
                    'AUTH',
                    f'XOAUTH2 {encoded_auth}',
                )

                # Код 235 означает успешную SMTP-аутентификацию.
                if code != 235:
                    raise smtplib.SMTPAuthenticationError(code, response)

                # Передаём подготовленное письмо SMTP-серверу.
                server.send_message(message)

            # Успешное завершение отправки фиксируем в журнале.
            self.logger.info(
                'Email-отчёт отправлен: %s',
                self.recipient,
            )
            return True

        # Ошибка OAuth2-аутентификации: неверный, отозванный
        # или неподходящий для SMTP токен.
        except smtplib.SMTPAuthenticationError as error:
            self.logger.error(
                'Email: Яндекс отклонил авторизацию. SMTP %s: %s',
                error.smtp_code,
                error.smtp_error,
            )
            return False

        # Сервер отказался принимать письмо для указанного получателя.
        except smtplib.SMTPRecipientsRefused as error:
            self.logger.error(
                'Email: SMTP-сервер отклонил получателя: %s',
                error.recipients,
            )
            return False

        # Другие ошибки, для которых SMTP-сервер вернул код ответа.
        except smtplib.SMTPResponseException as error:
            self.logger.error(
                'Email: ошибка SMTP %s: %s',
                error.smtp_code,
                error.smtp_error,
            )
            return False

        # Ошибки соединения и сетевого взаимодействия.
        except (smtplib.SMTPException, OSError) as error:
            self.logger.error(
                'Email: ошибка соединения или отправки: %s',
                error,
            )
            return False
