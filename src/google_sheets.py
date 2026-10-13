"""Создание Google-таблицы и выгрузка ежедневной статистики.

Модуль отвечает за OAuth2-авторизацию в Google, создание или поиск
таблицы, сохранение её идентификатора и обновление статистики ETL.

Для работы используются Google Sheets API и Google Drive API.
"""

import json
import logging
from http import HTTPStatus
from pathlib import Path
from typing import Any

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.constants import (
    GOOGLE_CREDENTIALS_PATH,
    GOOGLE_SPREADSHEET_ID_PATH,
    GOOGLE_TOKEN_PATH,
)


class GoogleSheetsManager:
    """Управляет созданием и обновлением таблиц Google Sheets."""

    # Разрешения для редактирования Google Sheets и работы
    # с файлами Google Drive, созданными или открытыми приложением.
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
    ]

    # Названия документа и рабочего листа со статистикой.
    SPREADSHEET_NAME = 'Grader ETL — Daily Statistics'
    SHEET_NAME = 'Daily Statistics'

    # Заголовки семи столбцов ежедневной статистики.
    HEADERS = [
        'Дата',
        'Всего попыток',
        'Успешных попыток',
        'Процент успешности',
        'Уникальных пользователей',
        'Запусков кода',
        'Отправок решений',
    ]

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Настраивает авторизацию и клиенты Google Sheets и Drive."""
        self.logger = logger or logging.getLogger('grader_etl')

        # Пути к OAuth-настройкам, пользовательскому токену
        # и локально сохранённому идентификатору таблицы.
        self.credentials_path = Path(GOOGLE_CREDENTIALS_PATH)
        self.token_path = Path(GOOGLE_TOKEN_PATH)
        self.spreadsheet_id_path = Path(GOOGLE_SPREADSHEET_ID_PATH)

        # Авторизуем пользователя перед созданием API-клиентов.
        self.credentials = self._authorize()

        # Создаём отдельные клиенты для работы с содержимым таблиц
        # и поиска файлов в Google Drive.
        self.sheets_service: Any = build(
            'sheets',
            'v4',
            credentials=self.credentials,
        )
        self.drive_service: Any = build(
            'drive',
            'v3',
            credentials=self.credentials,
        )

    def _authorize(self) -> Credentials:
        """Получает Google OAuth-токен и обновляет его при необходимости."""
        credentials = None

        # Если пользователь уже авторизовался, используем
        # сохранённый токен вместо повторного открытия браузера.
        if self.token_path.exists():
            credentials = Credentials.from_authorized_user_file(
                str(self.token_path),
                self.SCOPES,
            )

            # Истёкший access-токен обновляем через refresh-токен.
            if credentials.expired:
                if not credentials.refresh_token:
                    raise RuntimeError(
                        'Google OAuth-токен истёк, а refresh-токен отсутствует. '
                        'Требуется повторная авторизация.'
                    )

                try:
                    credentials.refresh(Request())
                    self.logger.info('Google OAuth-токен успешно обновлён')
                except RefreshError as exc:
                    # Если обновление отклонено Google, потребуется
                    # повторная интерактивная авторизация.
                    self.logger.error(
                        'Не удалось обновить Google OAuth-токен: %s',
                        exc,
                    )
                    raise RuntimeError(
                        'Google OAuth-токен недействителен. Требуется повторная авторизация.'
                    ) from exc

        # При отсутствии сохранённого токена запускаем
        # первичную авторизацию через локальный браузер.
        if not credentials:
            if not self.credentials_path.exists():
                raise FileNotFoundError(f'Не найден файл Google OAuth: {self.credentials_path}')

            self.logger.info('Запуск первичной авторизации Google OAuth')

            flow = InstalledAppFlow.from_client_secrets_file(
                str(self.credentials_path),
                self.SCOPES,
            )
            credentials = flow.run_local_server(port=0)

        # Проверяем действительность полученных учётных данных.
        if not credentials.valid:
            raise RuntimeError('Google OAuth-авторизация не выполнена. Проверьте токен и настройки доступа.')

        # Сохраняем актуальный токен для следующих запусков ETL.
        self.token_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        self.token_path.write_text(
            credentials.to_json(),
            encoding='utf-8',
        )

        self.logger.info('Авторизация Google выполнена')
        return credentials

    def get_or_create_spreadsheet(self) -> str:
        """Находит существующую Google-таблицу или создаёт новую."""
        # Сначала проверяем локально сохранённый ID таблицы.
        # Это позволяет избежать повторного поиска в Google Drive.
        if self.spreadsheet_id_path.exists():
            saved = json.loads(self.spreadsheet_id_path.read_text(encoding='utf-8'))
            spreadsheet_id = saved.get('spreadsheet_id')

            if spreadsheet_id:
                try:
                    # Проверяем, что таблица существует и доступна.
                    self.sheets_service.spreadsheets().get(
                        spreadsheetId=spreadsheet_id,
                    ).execute()

                    self.logger.info(
                        'Найдена существующая таблица: %s',
                        spreadsheet_id,
                    )
                    return spreadsheet_id

                except HttpError as exc:
                    # Ошибка 403 означает отсутствие доступа.
                    # Не создаём новую таблицу вместо недоступной.
                    if exc.resp.status == HTTPStatus.FORBIDDEN:
                        self.logger.error(
                            'Нет доступа к Google-таблице: %s',
                            spreadsheet_id,
                        )
                        raise

                    # Только при 404 продолжаем поиск другой таблицы.
                    # Остальные ошибки Google API передаём выше.
                    if exc.resp.status != HTTPStatus.NOT_FOUND:
                        raise

                    self.logger.warning(
                        'Сохранённая Google-таблица не найдена: %s',
                        spreadsheet_id,
                    )

        # Экранируем апострофы в названии для поискового запроса Drive.
        escaped_name = self.SPREADSHEET_NAME.replace("'", "\\'")

        # Ищем существующий документ Google Sheets по имени.
        # Исключаем файлы, находящиеся в корзине.
        result = (
            self.drive_service
            .files()  # pylint: disable=no-member
            .list(
                q=(
                    f"name = '{escaped_name}' "
                    "and mimeType = 'application/vnd.google-apps.spreadsheet' "
                    'and trashed = false'
                ),
                fields='files(id, name)',
                pageSize=100,
            )
            .execute()
        )

        files = result.get('files', [])

        # Повторно используем первый найденный документ.
        if files:
            spreadsheet_id = files[0]['id']
            self.logger.info('Таблица найдена в Google Drive')
        else:
            # Если подходящей таблицы нет, создаём новый документ
            # с заранее заданным названием рабочего листа.
            spreadsheet = (
                self.sheets_service
                .spreadsheets()  # pylint: disable=no-member
                .create(
                    body={
                        'properties': {
                            'title': self.SPREADSHEET_NAME,
                        },
                        'sheets': [
                            {
                                'properties': {
                                    'title': self.SHEET_NAME,
                                }
                            }
                        ],
                    },
                    fields='spreadsheetId',
                )
                .execute()
            )

            spreadsheet_id = spreadsheet['spreadsheetId']
            self.logger.info('Создана новая таблица Google Sheets')

        # Сохраняем ID документа локально для следующих запусков.
        self.spreadsheet_id_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        self.spreadsheet_id_path.write_text(
            json.dumps({'spreadsheet_id': spreadsheet_id}),
            encoding='utf-8',
        )

        # Экранируем апострофы в названии листа для A1-диапазона.
        sheet_name = self.SHEET_NAME.replace("'", "''")

        # Записываем заголовки в первую строку листа.
        self.sheets_service.spreadsheets().values().update(  # pylint: disable=no-member
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1:G1",
            valueInputOption='RAW',
            body={'values': [self.HEADERS]},
        ).execute()

        self.logger.info(
            'Google Sheets: https://docs.google.com/spreadsheets/d/%s/edit',
            spreadsheet_id,
        )
        return spreadsheet_id

    def update_statistics(self, records: list[list]) -> None:
        """Полностью обновляет ежедневную статистику в Google Sheets."""
        # При отсутствии статистики не выполняем запросы к Google API.
        if not records:
            self.logger.info('Нет статистики для выгрузки в Google Sheets')
            return

        # Находим существующую таблицу или создаём новую.
        spreadsheet_id = self.get_or_create_spreadsheet()
        # Формируем строки для выгрузки: заголовки и данные по дням.
        values = [self.HEADERS] + records

        sheet_name = self.SHEET_NAME.replace("'", "''")
        sheet_range = f"'{sheet_name}'"

        # Очищаем все семь столбцов перед обновлением.
        # Это предотвращает сохранение устаревших строк
        # при уменьшении количества дней в отчёте.
        self.sheets_service.spreadsheets().values().clear(  # pylint: disable=no-member
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_range}!A:G',
            body={},
        ).execute()

        # Записываем актуальный набор статистики с ячейки A1.
        # RAW сохраняет значения без автоматического разбора формул.
        self.sheets_service.spreadsheets().values().update(  # pylint: disable=no-member
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_range}!A1',
            valueInputOption='RAW',
            body={'values': values},
        ).execute()

        self.logger.info(
            'Статистика выгружена в Google Sheets: %s дней',
            len(records),
        )
        self.logger.info(
            'Открыть таблицу: https://docs.google.com/spreadsheets/d/%s/edit',
            spreadsheet_id,
        )

    @staticmethod
    def get_spreadsheet_url() -> str:
        """Возвращает ссылку на таблицу по сохранённому идентификатору."""
        spreadsheet_path = Path(GOOGLE_SPREADSHEET_ID_PATH)

        # Метод не требует авторизации и обращения к Google API:
        # ссылка формируется по локально сохранённому ID.
        if not spreadsheet_path.exists():
            return 'Таблица пока не создана'

        try:
            data = json.loads(
                spreadsheet_path.read_text(encoding='utf-8'),
            )
            spreadsheet_id = data.get('spreadsheet_id')

            if spreadsheet_id:
                return f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit'

        except (OSError, ValueError, AttributeError):
            # Если файл повреждён или недоступен, фиксируем предупреждение,
            # не прерывая выполнение приложения.
            logging.getLogger('grader_etl').warning(
                'Не удалось прочитать ID Google-таблицы',
            )

        return 'Ссылка на таблицу недоступна'
