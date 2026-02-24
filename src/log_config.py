import logging

LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")


class TickTimeFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.tick_time = 0

    def set_tick(self, tick):
        self.tick_time = tick

    def filter(self, record):
        record.tick_time = self.tick_time
        return True


def configure_logging(log_level: str):
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
    logger.setLevel(getattr(logging, log_level.upper()))
    return logger


# Module-level TickTimeFilter instance
_tick_time_filter: TickTimeFilter | None = None


def set_tick_time(tick: int):
    """
    Set the current tick time for logging.
    """
    if _tick_time_filter is not None:
        _tick_time_filter.set_tick(tick)
