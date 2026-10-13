"""Тесты преобразования и валидации записей API.

Проверяем этап Transform: извлечение вложенных параметров LTI,
приведение типов, проверку обязательных полей и формирование
структуры записи для последующей загрузки в PostgreSQL.

Отдельно проверяем обработку некорректных записей: они не должны
попадать в результат преобразования, а причины отклонения должны
сохраняться в rejected_records.
"""

import pytest

from src.transform import GraderTransformer


@pytest.fixture
def transformer():
    """Создаёт отдельный экземпляр преобразователя для каждого теста."""
    return GraderTransformer()


@pytest.fixture
def valid_record():
    """Возвращает корректную исходную запись в формате ответа API."""
    # passback_params приходит как строковое представление словаря.
    # На этапе Transform из него извлекаются отдельные параметры LTI.
    return {
        'lti_user_id': 'user_001',
        'passback_params': str({
            'oauth_consumer_key': 'test_key',
            'lis_result_sourcedid': 'course_001',
            'lis_outcome_service_url': 'https://example.com/grade',
        }),
        'is_correct': True,
        'attempt_type': 'submit',
        'created_at': '2023-05-31 12:30:00.123456',
    }


def test_transform_record(transformer, valid_record):
    """Проверяет преобразование корректной записи."""
    result = transformer.transform_record(valid_record)

    # Проверяем итоговые поля, соответствующие структуре grader_attempts.
    # Идентификатор пользователя переименовывается, а вложенные
    # параметры LTI извлекаются в отдельные поля.
    assert result['user_id'] == 'user_001'
    assert result['oauth_consumer_key'] == 'test_key'
    assert result['lis_result_sourcedid'] == 'course_001'
    assert result['lis_outcome_service_url'] == 'https://example.com/grade'
    assert result['is_correct'] is True
    assert result['attempt_type'] == 'submit'
    assert result['created_at'].year == 2023


def test_run_record(transformer, valid_record):
    """Для run допускается is_correct=None."""
    # Запуск кода (run) не обязательно сопровождается проверкой решения.
    # Поэтому отсутствие результата проверки является допустимым.
    valid_record['attempt_type'] = 'run'
    valid_record['is_correct'] = None

    result = transformer.transform_record(valid_record)

    assert result['attempt_type'] == 'run'
    assert result['is_correct'] is None


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('lti_user_id', ''),
        ('lti_user_id', None),
        ('attempt_type', 'unknown'),
        ('is_correct', 'yes'),
        ('created_at', 'invalid_date'),
        ('passback_params', 'invalid_dict'),
    ],
)
def test_invalid_record(transformer, valid_record, field, value):
    """Некорректные значения должны отклоняться."""
    # Один параметризованный тест проверяет несколько типов ошибок:
    # отсутствие ID, неизвестный тип попытки, неправильный результат,
    # некорректную дату и невозможность разобрать passback_params.
    valid_record[field] = value

    with pytest.raises(ValueError):
        transformer.transform_record(valid_record)


def test_missing_passback_fields(transformer, valid_record):
    """Отсутствующие поля passback_params заполняются None."""
    # Отдельные параметры LTI могут отсутствовать.
    # Это не должно приводить к отклонению всей записи.
    valid_record['passback_params'] = "{'oauth_consumer_key': 'test_key'}"

    result = transformer.transform_record(valid_record)

    assert result['oauth_consumer_key'] == 'test_key'
    assert result['lis_result_sourcedid'] is None
    assert result['lis_outcome_service_url'] is None


def test_extra_passback_fields(transformer, valid_record):
    """Лишние поля passback_params не попадают в результат."""
    # В PostgreSQL передаём только поля, предусмотренные схемой,
    # игнорируя дополнительные параметры исходного ответа API.
    valid_record['passback_params'] = str({
        'oauth_consumer_key': 'test_key',
        'extra_field': 'unnecessary',
    })

    result = transformer.transform_record(valid_record)

    assert 'extra_field' not in result


def test_rejected_records(transformer, valid_record):
    """Некорректные записи пропускаются и сохраняются отдельно."""
    # Ошибка в одной записи не должна останавливать
    # преобразование всего набора данных.
    records = [
        valid_record,
        {'lti_user_id': None},
    ]

    result = transformer.transform_records(records)

    assert len(result) == 1
    assert len(transformer.rejected_records) == 1
    assert 'reason' in transformer.rejected_records[0]


def test_empty_records(transformer):
    """Пустой список должен возвращать пустой список."""
    # Отсутствие записей за период является допустимой ситуацией.
    assert transformer.transform_records([]) == []


def test_invalid_input(transformer):
    """Вместо списка записей нельзя передавать словарь."""
    # Метод пакетного преобразования ожидает список записей API.
    with pytest.raises(ValueError):
        transformer.transform_records({'data': []})


def test_user_id_strip(transformer, valid_record):
    """Удаляет пробелы по краям идентификатора пользователя."""
    # Нормализация ID предотвращает различия из-за лишних пробелов.
    valid_record['lti_user_id'] = '  user_001  '

    result = transformer.transform_record(valid_record)

    assert result['user_id'] == 'user_001'


def test_missing_passback_params(transformer, valid_record):
    """Отсутствующий passback_params приводит к отклонению записи."""
    # Отсутствие самого поля отличается от отсутствия отдельных
    # необязательных параметров внутри него.
    valid_record.pop('passback_params')

    with pytest.raises(ValueError, match='passback_params'):
        transformer.transform_record(valid_record)


def test_invalid_passback_type(transformer, valid_record):
    """passback_params должен содержать словарь."""
    # Даже если строку удалось разобрать, её содержимое
    # должно быть словарём, а не списком или другим типом.
    valid_record['passback_params'] = "['value_1', 'value_2']"

    with pytest.raises(ValueError, match='словарь'):
        transformer.transform_record(valid_record)


def test_mixed_records(transformer, valid_record):
    """Сохраняет корректные записи и причины отклонения остальных."""
    # Формируем набор из корректной и некорректной записи.
    # Копирование предотвращает изменение исходной valid_record
    invalid_record = valid_record.copy()
    invalid_record['attempt_type'] = 'unknown'

    records = [valid_record, invalid_record]

    result = transformer.transform_records(records)

    assert len(result) == 1
    assert result[0]['user_id'] == 'user_001'

    # Проверяем не только количество отклонённых записей,
    # но и сохранение исходных данных вместе с конкретной причиной
    assert len(transformer.rejected_records) == 1
    assert transformer.rejected_records[0]['record'] == invalid_record
    assert transformer.rejected_records[0]['reason'] == 'Некорректный attempt_type'


def test_rejected_records_reset(transformer, valid_record):
    """Очищает отклонённые записи перед новым набором данных."""
    # Сначала создаём одну отклонённую запись.
    transformer.transform_records([
        {'lti_user_id': None},
    ])

    assert len(transformer.rejected_records) == 1

    # При следующем вызове rejected_records должен содержать
    # ошибки только текущего набора, а не предыдущих загрузок.
    result = transformer.transform_records([valid_record])

    assert len(result) == 1
    assert transformer.rejected_records == []


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        (1, True),
        (0, False),
        (True, True),
        (False, False),
        (None, None),
    ],
)
def test_is_correct_conversion(transformer, valid_record, value, expected):
    """Преобразует значения is_correct в bool или None."""
    # API может возвращать целые числа 1 и 0, а также None.
    # Для PostgreSQL BOOLEAN нормализуем их в True, False или None.
    valid_record['is_correct'] = value

    result = transformer.transform_record(valid_record)

    assert result['is_correct'] is expected


@pytest.mark.parametrize('value', [2, -1, 1.0, '1', 'True'])
def test_invalid_is_correct(transformer, valid_record, value):
    """Отклоняет недопустимые значения is_correct."""
    # Не выполняем неявное приведение произвольных значений к bool,
    # поскольку оно могло бы исказить результат проверки решения.
    valid_record['is_correct'] = value

    with pytest.raises(ValueError, match='Некорректный is_correct'):
        transformer.transform_record(valid_record)
