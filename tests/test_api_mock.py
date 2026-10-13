"""Проверка получения данных из API без реальных сетевых запросов.

Тесты проверяют синхронную и асинхронную загрузку данных:
успешные ответы, пустые и некорректные данные, ошибки HTTP,
тайм-ауты, проверку дат и механизм повторных запросов.

Вместо обращения к настоящему API используем monkeypatch,
который подменяет requests.get тестовыми ответами.
"""

import asyncio
from datetime import datetime, timezone

import pytest
import requests

from src.extract import GraderExtractor

# Общий тестовый интервал — один календарный день в UTC.
START = datetime(2023, 5, 31, tzinfo=timezone.utc)
END = datetime(2023, 6, 1, tzinfo=timezone.utc)


class FakeResponse:
    """Имитирует HTTP-ответ API для тестирования без сети."""

    def __init__(self, data, status_code=200):
        self.data = data
        self.status_code = status_code

    def json(self):
        """Возвращает заранее подготовленные тестовые данные."""
        return self.data

    def raise_for_status(self):
        """Имитирует исключение requests при ошибочном HTTP-статусе."""
        if self.status_code >= 400:
            raise requests.HTTPError(
                f'HTTP Error: {self.status_code}',
                response=self,
            )


@pytest.fixture
def extractor():
    """Создаёт экземпляр загрузчика данных для каждого теста."""
    return GraderExtractor()


def test_success(monkeypatch, extractor):
    """Проверяет успешную загрузку записей и параметры запроса."""
    records = [
        {
            'lti_user_id': 'user_001',
            'created_at': '2023-05-31 12:30:00',
        }
    ]

    # Подменяем HTTP-запрос и одновременно проверяем,
    # что Extract передаёт правильные границы периода и тайм-аут.
    def fake_get(url, params, timeout):
        assert params['start'] == '2023-05-31 00:00:00.000000'
        assert params['end'] == '2023-06-01 00:00:00.000000'
        assert timeout > 0
        return FakeResponse(records)

    monkeypatch.setattr(requests, 'get', fake_get)

    assert extractor.from_api(START, END) == records


def test_empty_response(monkeypatch, extractor):
    """Проверяет, что пустой список является допустимым ответом API."""
    # Отсутствие записей за период не должно считаться ошибкой.
    monkeypatch.setattr(
        requests,
        'get',
        lambda *args, **kwargs: FakeResponse([]),
    )

    assert extractor.from_api(START, END) == []


def test_invalid_response(monkeypatch, extractor):
    """Проверяет отклонение ответа API с неправильной структурой."""
    monkeypatch.setattr(
        requests,
        'get',
        lambda *args, **kwargs: FakeResponse({'error': 'invalid'}),
    )

    with pytest.raises(ValueError):
        extractor.from_api(START, END)


def test_http_error(monkeypatch, extractor):
    """Проверяет передачу HTTP-ошибки вызывающему коду."""
    # Имитируем серверную ошибку, не обращаясь к настоящему API.
    monkeypatch.setattr(
        requests,
        'get',
        lambda *args, **kwargs: FakeResponse(
            {'error': 'server error'},
            status_code=500,
        ),
    )

    with pytest.raises(requests.HTTPError):
        extractor.from_api(START, END)


def test_timeout(monkeypatch, extractor):
    """Проверяет обработку тайм-аута при подключении к API."""

    # Имитируем отсутствие соединения, чтобы проверить,
    # что исключение не скрывается внутри Extract.
    def fake_get(*args, **kwargs):
        raise requests.ConnectTimeout('Тайм-аут')

    monkeypatch.setattr(requests, 'get', fake_get)

    with pytest.raises(requests.ConnectTimeout):
        extractor.from_api(START, END)


def test_invalid_period(extractor):
    """Проверяет отклонение периода с неправильными границами."""
    # Конец периода не может предшествовать его началу.
    with pytest.raises(ValueError):
        extractor.from_api(END, START)


def test_record_outside_period(monkeypatch, extractor):
    """Проверяет обнаружение записей за пределами запрошенного периода."""
    # Даже успешный HTTP-ответ не гарантирует корректность данных.
    # Extract должен обнаружить запись с датой вне интервала.
    records = [
        {
            'lti_user_id': 'user_001',
            'created_at': '2023-06-02 12:30:00',
        }
    ]

    monkeypatch.setattr(
        requests,
        'get',
        lambda *args, **kwargs: FakeResponse(records),
    )

    with pytest.raises(ValueError):
        extractor.from_api(START, END)


def test_async_retry_http_502(monkeypatch, extractor):
    """Проверяет повторный запрос после временной ошибки HTTP 502."""
    attempts = 0
    records = [
        {
            'lti_user_id': 'user_001',
            'created_at': '2023-05-31 12:30:00',
        }
    ]

    # Первый запрос завершается ошибкой, второй возвращает записи.
    # Счётчик позволяет проверить реальное количество попыток.
    def fake_get(*args, **kwargs):
        """Имитирует HTTP 502 при первом запросе и успех при повторном."""
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            return FakeResponse(
                {'error': 'Bad Gateway'},
                status_code=502,
            )

        return FakeResponse(records)

    # Подменяем ожидание, чтобы тест не тратил время на реальные паузы.
    async def fake_sleep(seconds):
        """Имитирует ожидание без задержки."""

    monkeypatch.setattr(requests, 'get', fake_get)
    monkeypatch.setattr(asyncio, 'sleep', fake_sleep)

    result = asyncio.run(
        extractor.from_api_async(
            START,
            END,
            asyncio.Semaphore(2),
        )
    )

    assert result == records
    assert attempts == 2


def test_async_retry_timeout(monkeypatch, extractor):
    """Проверяет прекращение повторов после трёх тайм-аутов."""
    attempts = 0

    # Каждый запрос завершается тайм-аутом.
    # Проверяем, что число попыток ограничено max_attempts.
    def fake_get(*args, **kwargs):
        """Имитирует тайм-аут при каждой попытке обращения к API."""
        nonlocal attempts
        attempts += 1
        raise requests.ConnectTimeout('Тайм-аут')

    async def fake_sleep(seconds):
        """Имитирует ожидание без задержки."""

    monkeypatch.setattr(requests, 'get', fake_get)
    monkeypatch.setattr(asyncio, 'sleep', fake_sleep)

    with pytest.raises(requests.ConnectTimeout):
        asyncio.run(
            extractor.from_api_async(
                START,
                END,
                asyncio.Semaphore(2),
                max_attempts=3,
            )
        )

    assert attempts == 3


def test_async_no_retry_http_401(monkeypatch, extractor):
    """Проверяет отсутствие повторов при ошибке авторизации HTTP 401."""
    attempts = 0

    # Повторный запрос с теми же данными авторизации
    # не устранит ошибку, поэтому ожидаем только одну попытку.
    def fake_get(*args, **kwargs):
        """Имитирует ошибку авторизации и подсчитывает число запросов."""
        nonlocal attempts
        attempts += 1

        return FakeResponse(
            {'error': 'Unauthorized'},
            status_code=401,
        )

    monkeypatch.setattr(requests, 'get', fake_get)

    with pytest.raises(requests.HTTPError):
        asyncio.run(
            extractor.from_api_async(
                START,
                END,
                asyncio.Semaphore(2),
            )
        )

    assert attempts == 1
