"""Проверка и преобразование исходных записей API для PostgreSQL.

Модуль реализует этап Transform ETL-процесса:
проверяет обязательные поля, преобразует типы данных
и извлекает параметры LTI из passback_params.

Корректные записи подготавливаются для загрузки в PostgreSQL,
а отклонённые сохраняются отдельно с указанием причины ошибки.
"""

import ast
import logging
from datetime import datetime

from src.constants import API_DATETIME_FORMAT


class GraderTransformer:
    """Проверяет и преобразует учебные попытки, отделяя некорректные записи."""

    # Поля LTI, которые необходимо извлечь из passback_params.
    PASSBACK_FIELDS = (
        'oauth_consumer_key',
        'lis_result_sourcedid',
        'lis_outcome_service_url',
    )

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Настраивает логгер и список отклонённых записей."""
        self.logger = logger or logging.getLogger('grader_etl')
        # Здесь хранятся записи, не прошедшие валидацию
        # при последнем вызове transform_records().
        self.rejected_records: list[dict] = []

    @staticmethod
    def parse_passback_params(value: object) -> dict:
        """Преобразует строковое представление словаря Python в dict."""
        # API передаёт параметры LTI в виде строки.
        if not isinstance(value, str):
            raise ValueError('passback_params должен быть строкой')

        try:
            # Безопасно разбираем строковое представление литерала.
            # В отличие от eval(), literal_eval() не выполняет код.
            result = ast.literal_eval(value)
        except (ValueError, SyntaxError) as error:
            raise ValueError('Некорректный формат passback_params') from error

        # Проверяем, что результат разбора является словарём.
        if not isinstance(result, dict):
            raise ValueError('passback_params должен содержать словарь')

        return result

    def transform_record(self, record: dict) -> dict:
        """Проверяет одну запись API и возвращает данные для PostgreSQL."""
        # Каждая исходная запись должна иметь структуру словаря.
        if not isinstance(record, dict):
            raise ValueError('Запись должна быть словарём')

        # Получаем идентификатор пользователя из поля API.
        user_id = record.get('lti_user_id')

        # Идентификатор должен быть непустой строкой.
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError('Отсутствует корректный user_id')

        # Удаляем пробелы по краям идентификатора.
        user_id = user_id.strip()

        # Разрешены только два типа учебных попыток:
        # run — запуск кода, submit — отправка решения.
        attempt_type = record.get('attempt_type')

        if attempt_type not in ('run', 'submit'):
            raise ValueError('Некорректный attempt_type')

        # Результат проверки может быть True, False или None.
        # Также допускаются числовые значения 0 и 1.
        is_correct = record.get('is_correct')

        if is_correct is not None:
            # Исключаем остальные типы и числовые значения.
            if type(is_correct) not in (bool, int) or is_correct not in (0, 1):
                raise ValueError('Некорректный is_correct')

            # Приводим 0 и 1 к False и True соответственно.
            is_correct = bool(is_correct)

        # Получаем дату и время создания попытки.
        created_at = record.get('created_at')

        if not isinstance(created_at, str):
            raise ValueError('Некорректный created_at')

        try:
            # Преобразуем строку API в объект datetime
            # согласно формату, заданному в constants.py.
            created_at = datetime.strptime(
                created_at,
                API_DATETIME_FORMAT,
            )
        except ValueError as error:
            raise ValueError('Некорректный формат created_at') from error

        # Разбираем строку с параметрами LTI.
        params = self.parse_passback_params(record.get('passback_params'))

        # Формируем запись со структурой, ожидаемой GraderLoader.
        return {
            'user_id': user_id,
            # Извлекаем только необходимые поля passback_params.
            # Отсутствующие ключи получают значение None.
            **{field: params.get(field) for field in self.PASSBACK_FIELDS},
            'is_correct': is_correct,
            'attempt_type': attempt_type,
            'created_at': created_at,
        }

    def transform_records(self, records: list[dict]) -> list[dict]:
        """Преобразует записи и сохраняет причины отклонения."""
        # Ответ API должен содержать список записей.
        if not isinstance(records, list):
            raise ValueError('Ответ API должен быть списком')

        # Подготавливаем результат и очищаем отклонения
        # от предыдущего вызова метода.
        valid_records = []
        self.rejected_records = []

        self.logger.info(
            'Начало преобразования: %s записей',
            len(records),
        )

        # Обрабатываем каждую запись независимо от остальных.
        # Нумерация начинается с 1 для удобства чтения логов.
        for index, record in enumerate(records, start=1):
            try:
                # Корректные записи добавляем в итоговый список.
                valid_records.append(self.transform_record(record))

            except (ValueError, TypeError) as error:
                # Некорректную запись сохраняем вместе с причиной,
                # чтобы её можно было проанализировать отдельно.
                self.rejected_records.append({
                    'record': record,
                    'reason': str(error),
                })

                self.logger.error(
                    'Запись %s отклонена: %s',
                    index,
                    error,
                )

        # Выводим количество принятых и отклонённых записей.
        self.logger.info(
            'Преобразование завершено: корректных — %s, отклонённых — %s',
            len(valid_records),
            len(self.rejected_records),
        )

        return valid_records
