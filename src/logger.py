"""Настройка логирования ETL и удаления устаревших логов.

Модуль создаёт общий логгер приложения с выводом сообщений
в консоль и ежедневный файл.

При инициализации удаляются лог-файлы, срок хранения
которых превышает установленное количество календарных дней.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from src.constants import LOG_DIRECTORY, LOG_RETENTION_DAYS


class GraderLogger:
    """Настраивает логирование ETL и управляет хранением лог-файлов."""

    def __init__(
        self,
        log_dir: str = LOG_DIRECTORY,
        retention_days: int = LOG_RETENTION_DAYS,
    ) -> None:
        """Создаёт каталог логов, очищает старые файлы и настраивает логгер."""
        # Путь к каталогу логов и срок хранения задаются константами
        # проекта, но при необходимости могут быть переопределены.
        self.log_dir = Path(log_dir)
        self.retention_days = retention_days
        # Создаём каталог вместе с родительскими папками,
        # если они ещё не существуют.
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Сначала удаляем устаревшие журналы,
        # затем настраиваем логирование текущего запуска.
        self._cleanup_logs()
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Создаёт общий логгер с выводом в файл и консоль."""
        # Используем единое имя логгера для всех модулей ETL.
        logger = logging.getLogger('grader_etl')
        # Записываем сообщения уровня INFO и выше.
        logger.setLevel(logging.INFO)
        # Отключаем передачу сообщений родительскому логгеру,
        # чтобы избежать дублирования вывода.
        logger.propagate = False

        # Имя файла содержит текущую локальную дату.
        log_file = self.log_dir / f'etl_{datetime.now():%Y-%m-%d}.log'

        # Единый формат сообщений для консоли и файла:
        # дата и время | уровень | текст сообщения.
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )

        # Удаляем и закрываем ранее зарегистрированные обработчики.
        # Это предотвращает дублирование сообщений при повторной
        # инициализации логгера в рамках одного процесса.
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()

        # Файловый обработчик сохраняет сообщения в UTF-8.
        file_handler = logging.FileHandler(
            log_file,
            encoding='utf-8',
        )
        # Консольный обработчик выводит сообщения в стандартный
        # поток logging.StreamHandler (по умолчанию stderr).
        console_handler = logging.StreamHandler()

        # Применяем одинаковый формат и регистрируем обработчики.
        for handler in (file_handler, console_handler):
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _cleanup_logs(self) -> None:
        """Удаляет логи за пределами установленного срока хранения."""
        # Вычисляем самую раннюю дату, которую нужно сохранить.
        # Например, при retention_days=3 сохраняются журналы
        # за сегодня и два предыдущих календарных дня.
        cutoff = datetime.now().date() - timedelta(days=self.retention_days - 1)

        # Рассматриваем только файлы, соответствующие шаблону ETL.
        for file in self.log_dir.glob('etl_*.log'):
            try:
                # Извлекаем дату из имени файла etl_YYYY-MM-DD.log.
                file_date = datetime.strptime(
                    file.stem.removeprefix('etl_'),
                    '%Y-%m-%d',
                ).date()
            except ValueError:
                # Файлы с некорректной датой в названии не удаляем.
                continue

            # Удаляем только журналы старше допустимой даты.
            if file_date < cutoff:
                try:
                    file.unlink()
                except OSError:
                    # Ошибка удаления отдельного файла не должна
                    # прерывать инициализацию ETL.
                    logging.getLogger('grader_etl').warning(
                        'Не удалось удалить устаревший лог-файл: %s',
                        file,
                        exc_info=True,
                    )
