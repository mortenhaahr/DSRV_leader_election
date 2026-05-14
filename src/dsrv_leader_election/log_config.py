import logging
from typing import override

LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

_LEVEL_BY_NAME: dict[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


class TickTimeFilter(logging.Filter):
    tick_time: int

    def __init__(self) -> None:
        super().__init__()
        self.tick_time = 0

    def set_tick(self, tick: int) -> None:
        self.tick_time = tick

    @override
    def filter(self, record: logging.LogRecord) -> bool:
        record.tick_time = self.tick_time
        return True


def configure_logging(log_level: str) -> logging.Logger:
    """
    Configure logging to include tick time in the log format.
    """
    global _tick_time_filter
    _tick_time_filter = TickTimeFilter()
    formatter = logging.Formatter("[Tick %(tick_time)s] [%(levelname)s] %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.addFilter(_tick_time_filter)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(handler)
    level_name = log_level.upper()
    if level_name not in _LEVEL_BY_NAME:
        raise ValueError(f"Invalid log level: {log_level}")
    logger.setLevel(_LEVEL_BY_NAME[level_name])

    return logger


# Module-level TickTimeFilter instance
_tick_time_filter: TickTimeFilter | None = None


def set_tick_time(tick: int) -> None:
    """
    Set the current tick time for logging.
    """
    if _tick_time_filter is not None:
        _tick_time_filter.set_tick(tick)
