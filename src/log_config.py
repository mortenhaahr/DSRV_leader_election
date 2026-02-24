import logging

def configure_logging():
    """
    Configure logging to include tick time in the log format.
    Returns (logger, tick_filter) tuple.
    """
    class TickTimeFilter(logging.Filter):
        def __init__(self):
            super().__init__()
            self.tick_time = 0
        def set_tick(self, tick):
            self.tick_time = tick
        def filter(self, record):
            record.tick_time = self.tick_time
            return True

    tick_filter = TickTimeFilter()
    formatter = logging.Formatter('[Tick %(tick_time)s] %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.addFilter(tick_filter)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger, tick_filter
