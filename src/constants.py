"""Общие константы ETL-приложения.

Централизованные настройки API, исторической загрузки, PostgreSQL,
логирования, локального хранения и внешних сервисов уведомлений.
"""

from datetime import datetime, timezone

# API
# Формат дат, передаваемых в параметрах запросов API.
API_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
# Максимальное время ожидания ответа API в секундах.
API_REQUEST_TIMEOUT = 120

# Параллельная историческая загрузка
# Максимальное количество одновременных запросов к API.
API_CONCURRENT_REQUESTS = 2
# Максимальное число попыток запроса при временных ошибках.
API_MAX_ATTEMPTS = 3

# Периоды загрузки

# Границы полной исторической загрузки (сохранены для последующего запуска).
# HISTORY_START = datetime(2022, 10, 2, tzinfo=timezone.utc)
# HISTORY_END = datetime(2026, 6, 1, tzinfo=timezone.utc)

# Текущий интервал для ограниченной загрузки и проверки ETL.
HISTORY_START = datetime(2023, 5, 1, tzinfo=timezone.utc)
HISTORY_END = datetime(2023, 6, 1, tzinfo=timezone.utc)
# Продолжительность одного периода загрузки в календарных днях.
PERIOD_DAYS = 1
# False — использовать фиксированную верхнюю границу HISTORY_END.
LOAD_UNTIL_NOW = False

# PostgreSQL
# Имя пайплайна для идентификации его состояния в базе данных.
PIPELINE_NAME = 'grader_attempts'
# Имя базы данных по умолчанию.
DEFAULT_DB_NAME = 'grader_etl'
# SQL-скрипт создания таблиц и других объектов базы данных.
DATABASE_INIT_SQL_PATH = 'sql/create_tables.sql'

# Логирование
# Каталог для файлов журналов работы ETL.
LOG_DIRECTORY = 'logs'
# Срок хранения файлов логов в днях.
LOG_RETENTION_DAYS = 3

# Локальные файлы
# Каталог для записей, отклонённых при обработке данных.
REJECTED_DIRECTORY = 'data/rejected'

# Google Sheets
# Файлы OAuth-настроек, токена авторизации и идентификатора таблицы.
GOOGLE_CREDENTIALS_PATH = 'credentials/google_oauth.json'
GOOGLE_TOKEN_PATH = 'credentials/google_token.json'
GOOGLE_SPREADSHEET_ID_PATH = 'credentials/spreadsheet_id.json'

# SMTP
# Максимальное время ожидания SMTP-операции в секундах.
SMTP_TIMEOUT = 15

# Telegram
# Максимальное время ожидания HTTP-запроса к Telegram в секундах.
TELEGRAM_REQUEST_TIMEOUT = 120
