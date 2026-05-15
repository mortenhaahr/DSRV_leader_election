import logging
from typing import override

from .simulation_context import simulation_run_context

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
    @override
    def filter(self, record: logging.LogRecord) -> bool:
        record.tick_time = simulation_run_context.current_tick_time()
        return True


def configure_logging(log_level: str) -> logging.Logger:
    """
    Configure logging to include tick time in the log format.
    """
    formatter = logging.Formatter("[Tick %(tick_time)s] [%(levelname)s] %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.addFilter(TickTimeFilter())
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(handler)
    level_name = log_level.upper()
    if level_name not in _LEVEL_BY_NAME:
        raise ValueError(f"Invalid log level: {log_level}")
    logger.setLevel(_LEVEL_BY_NAME[level_name])

    return logger
