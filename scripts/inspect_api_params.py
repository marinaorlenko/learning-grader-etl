"""Сохранение исходного ответа API для анализа passback_params.

Вспомогательный исследовательский скрипт позволяет изучить структуру
ответа API до этапа Transform, в частности вложенные поля passback_params.

Полученные данные сохраняются в JSON для локального анализа.
Скрипт не записывает данные в PostgreSQL и не изменяет состояние ETL.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from src.extract import GraderExtractor


def main() -> None:
    """Получает данные API и сохраняет их без преобразования."""
    # Для исследования используем один конкретный день из доступной истории.
    # Границы интервала задаём в UTC, как и в основном ETL
    start = datetime(2023, 5, 15, tzinfo=timezone.utc)
    end = datetime(2023, 5, 16, tzinfo=timezone.utc)

    # Используем основной Extract-компонент проекта,
    # но не передаём полученные записи на этапы Transform и Load.
    extractor = GraderExtractor()
    records = extractor.from_api(start=start, end=end)

    # Исходные ответы сохраняем отдельно от исследовательских отчётов.
    directory = Path('data/debug')
    directory.mkdir(parents=True, exist_ok=True)

    filepath = directory / 'api_raw_2023-05-15.json'

    # Сохраняем ответ целиком, включая вложенные структуры.
    # ensure_ascii=False сохраняет читаемые Unicode-символы,
    # indent=2 форматирует JSON, default=str преобразует
    # нестандартные объекты в строки при необходимости.
    with filepath.open('w', encoding='utf-8') as file:
        json.dump(
            records,
            file,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

    # Выводим количество записей и путь для дальнейшего анализа.
    print(f'Получено записей: {len(records)}')
    print(f'JSON сохранён: {filepath.resolve()}')


if __name__ == '__main__':
    main()
