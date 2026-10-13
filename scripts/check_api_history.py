"""Исследование истории API: годы, месяцы и дни.

Методика:
В связи с ограничениями API при выгрузке больших объёмов данных
исследуем доступность истории последовательно, уменьшая интервалы запросов.

1. Сначала проверяем годы, чтобы оценить доступность истории
командой python -m scripts.check_api_history --mode years
2. При тайм-аутах и HTTP 502 уменьшаем интервал до месяца
командой python -m scripts.check_api_history --mode months
3. Первый проблемный месяц проверяем по дням
командой python -m scripts.check_api_history

Ранее наблюдавшиеся результаты:
- 2020–2021: годовые запросы вернули 0 записей.
- 2022–2026: годовые запросы завершались ошибками.
- Январь–сентябрь 2022: месячные запросы вернули 0 записей.
- Октябрь 2022: месячный запрос завершился тайм-аутом.
- 2022-10-01: 0 записей.
- 2022-10-02: 22 записи.

Скрипт используется только для исследования доступности исторических данных.
Он не загружает записи в PostgreSQL и не изменяет состояние основного ETL.
"""

import argparse
import json
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path

import requests

from src.config import ETLConfig
from src.constants import API_DATETIME_FORMAT

# Настройки исследовательских запросов и каталога для JSON-отчётов.
# TIMEOUT: максимальное время подключения и ожидания ответа в секундах.
# Ограничиваем количество попыток и делаем паузы, чтобы не перегружать API.
REPORT_DIR = Path('data/research')
TIMEOUT = (10, 30)
MAX_ATTEMPTS = 2
REQUEST_DELAY = 1

# Результаты предыдущих проверок сохраняем в новых отчётах
# для документирования процесса поиска начала доступной истории.
FIRST_FOUND_DATE = '2022-10-02'

PREVIOUS_FINDINGS = {
    'yearly': {
        '2020-2021': 'Успешные ответы, 0 записей',
        '2022': 'ReadTimeout и HTTP 502',
        '2023-2026': 'HTTP 502',
    },
    'monthly': {
        '2022-01/2022-09': 'Успешные ответы, 0 записей',
        '2022-10': 'ReadTimeout',
    },
    'daily': {
        '2022-10-01': 0,
        '2022-10-02': 22,
    },
}


def get_periods(mode: str) -> Iterator[tuple[datetime, datetime]]:
    """Формирует интервалы для выбранного режима проверки."""
    if mode == 'years':
        # Сначала проверяем историю крупными годовыми интервалами.
        for year in range(2020, 2027):
            yield (
                datetime(year, 1, 1, tzinfo=timezone.utc),
                datetime(year + 1, 1, 1, tzinfo=timezone.utc),
            )

    elif mode == 'months':
        # Уточняем начало истории, проверяя январь–октябрь 2022 года.
        for month in range(1, 11):
            start = datetime(2022, month, 1, tzinfo=timezone.utc)
            end = (
                datetime(2022, month + 1, 1, tzinfo=timezone.utc)
                if month < 12
                else datetime(2023, 1, 1, tzinfo=timezone.utc)
            )
            yield start, end

    elif mode == 'days':
        # Проверяем два соседних дня на границе обнаруженной истории.
        for day in (1, 2):
            start = datetime(2022, 10, day, tzinfo=timezone.utc)
            yield start, start + timedelta(days=1)


def check_period(
    session: requests.Session,
    config: ETLConfig,
    start: datetime,
    end: datetime,
) -> dict:
    """Запрашивает период и возвращает результат проверки."""
    # Единая структура результата позволяет сохранять как успешные
    # запросы, так и ошибки, не теряя сведения о проверенных периодах.
    result = {
        'start': start.isoformat(),
        'end': end.isoformat(),
        'status': 'error',
        'records_count': None,
        'error': None,
        'attempts': 0,
    }

    # Параметры подключения берём из конфигурации проекта,
    # а даты приводим к формату, который ожидает API.
    params = {
        'client': config.api_client,
        'client_key': config.api_client_key,
        'start': start.strftime(API_DATETIME_FORMAT),
        'end': end.strftime(API_DATETIME_FORMAT),
    }

    # Повторяем запрос при временных ошибках, но не более MAX_ATTEMPTS раз.
    for attempt in range(1, MAX_ATTEMPTS + 1):
        result['attempts'] = attempt

        try:
            response = session.get(
                config.api_url,
                params=params,
                timeout=TIMEOUT,
            )
            response.raise_for_status()

            records = response.json()

            # Ожидаем список записей. Ответ другого типа не используем
            # для определения количества попыток за период.
            if not isinstance(records, list):
                result['error'] = 'Неожиданный формат ответа API'
                break

            result['status'] = 'success'
            result['records_count'] = len(records)
            result['error'] = None
            break

        except (requests.RequestException, ValueError) as error:
            result['error'] = str(error)

            # Ошибки 400, 401 и 403 обычно связаны с параметрами
            # запроса или доступом, поэтому повторять их бессмысленно.
            if isinstance(error, requests.HTTPError) and response.status_code in (
                HTTPStatus.BAD_REQUEST,
                HTTPStatus.UNAUTHORIZED,
                HTTPStatus.FORBIDDEN,
            ):
                break

            # При остальных ошибках перед повтором увеличиваем задержку.
            if attempt < MAX_ATTEMPTS:
                time.sleep(REQUEST_DELAY * attempt)

    return result


def save_report(mode: str, results: list[dict]) -> Path:
    """Сохраняет результаты текущего запуска в JSON."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Уникальное имя с датой, временем и микросекундами позволяет
    # хранить результаты нескольких запусков без перезаписи файлов.
    checked_at = datetime.now(timezone.utc)
    filename = f'api_history_{mode}_{checked_at:%Y%m%d_%H%M%S_%f}.json'
    path = REPORT_DIR / filename

    # Вместе с текущими результатами сохраняем метод исследования,
    # сведения об API и ранее полученные результаты.
    report = {
        'checked_at': checked_at.isoformat(),
        'mode': mode,
        'previous_findings': PREVIOUS_FINDINGS,
        'first_previously_found_date': FIRST_FOUND_DATE,
        'results': results,
    }

    path.write_text(
        json.dumps(report, ensure_ascii=False, indent=4),
        encoding='utf-8',
    )

    return path


def main() -> None:
    """Выполняет проверку и сохраняет отчёт."""
    # Режим исследования задаётся через --mode.
    # Без аргумента запускается проверка по дням.
    parser = argparse.ArgumentParser(description='Проверка исторических данных API')
    parser.add_argument(
        '--mode',
        choices=('years', 'months', 'days'),
        default='days',
    )
    args = parser.parse_args()

    config = ETLConfig()
    results = []

    # Session позволяет повторно использовать HTTP-соединение.
    # Результат каждого интервала сохраняем независимо от его статуса.
    with requests.Session() as session:
        for start, end in get_periods(args.mode):
            print(f'\nПроверяем {start.date()} — {end.date()}')

            result = check_period(session, config, start, end)
            results.append(result)

            if result['status'] == 'success':
                print(f'Записей: {result["records_count"]}')
            else:
                print(f'Ошибка: {result["error"]}')

            # Пауза между запросами снижает нагрузку на API.
            time.sleep(REQUEST_DELAY)

    report_path = save_report(args.mode, results)
    print(f'\nОтчёт сохранён: {report_path}')


if __name__ == '__main__':
    main()
