"""Сохранение отклонённых записей и ошибок API в JSON-файлы.

Модуль сохраняет записи, не прошедшие валидацию на этапе Transform,
и информацию об ошибках запросов на этапе Extract.

Данные группируются по датам периодов загрузки.
При повторном сохранении новые сведения добавляются к существующим.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from src.constants import REJECTED_DIRECTORY


class RejectedRecords:
    """Сохраняет отклонённые записи и ошибки API в локальных JSON-файлах."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Настраивает логирование и каталог для отклонённых данных."""
        self.logger = logger or logging.getLogger('grader_etl')
        # Используем общий каталог для отклонённых записей
        # и сведений об ошибках API.
        self.directory = Path(REJECTED_DIRECTORY)
        # Создаём каталог при первом запуске, если он отсутствует.
        self.directory.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        records: list[dict],
        period_start: datetime,
    ) -> None:
        """Добавляет отклонённые записи в JSON без потери предыдущих данных."""
        # При отсутствии отклонённых записей файл не создаём
        if not records:
            return

        # Имя файла определяется датой начала периода загрузки.
        filename = f'{period_start:%Y-%m-%d}.json'
        filepath = self.directory / filename

        # Для каждой отклонённой записи сохраняем время отклонения,
        # причину ошибки и исходные данные.
        rejected = [
            {
                'rejected_at': datetime.now(timezone.utc).isoformat(),
                'reason': item['reason'],
                'record': item['record'],
            }
            for item in records
        ]

        existing_records = []

        # Если файл за этот день уже существует, читаем его содержимое,
        # чтобы сохранить историю предыдущих отклонений.
        if filepath.exists():
            with filepath.open('r', encoding='utf-8') as file:
                existing_records = json.load(file)

            # Архив должен содержать список отклонённых записей.
            if not isinstance(existing_records, list):
                raise ValueError(f'Некорректный формат архива отклонённых записей: {filepath}')

        # Добавляем новые отклонения к ранее сохранённым.
        existing_records.extend(rejected)

        # Перезаписываем файл объединённым списком записей.
        # ensure_ascii=False сохраняет кириллицу в читаемом виде,
        # default=str преобразует нестандартные объекты в строки.
        with filepath.open('w', encoding='utf-8') as file:
            json.dump(
                existing_records,
                file,
                ensure_ascii=False,
                indent=4,
                default=str,
            )

        self.logger.warning(
            'Отклонённые записи сохранены: %s (добавлено %s шт.)',
            filepath,
            len(rejected),
        )

    def save_api_error(
        self,
        start: datetime,
        end: datetime,
        error: Exception,
    ) -> None:
        """Добавляет информацию об ошибке API в JSON-файл периода."""
        # Ошибки API храним отдельно от отклонённых записей.
        # Имя файла содержит дату начала неудачного периода.
        filepath = self.directory / f'api_errors_{start:%Y-%m-%d}.json'

        # Сохраняем время фиксации ошибки, границы периода,
        # тип исключения и его текст для последующей диагностики.
        entry = {
            'recorded_at': datetime.now(timezone.utc).isoformat(),
            'start': start.isoformat(),
            'end': end.isoformat(),
            'error_type': type(error).__name__,
            'message': str(error),
        }

        existing = []

        # При повторных ошибках за один день сохраняем
        # предыдущие записи, а не заменяем их новыми.
        if filepath.exists():
            existing = json.loads(
                filepath.read_text(encoding='utf-8'),
            )

            if not isinstance(existing, list):
                raise ValueError(f'Некорректный формат файла ошибок: {filepath}')

        # Добавляем новую ошибку в историю.
        existing.append(entry)

        # Записываем обновлённый список в читаемом JSON-формате.
        filepath.write_text(
            json.dumps(existing, ensure_ascii=False, indent=4),
            encoding='utf-8',
        )

        self.logger.warning(
            'Ошибка API сохранена: %s',
            filepath,
        )
