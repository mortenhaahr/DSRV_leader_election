import logging

from src.messages import ElectionMessage

LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

_MESSAGE_FIELDS = (
    "term",
    "candidate_id",
    "voter_id",
    "vote_granted",
    "leader_id",
    "follower_id",
    "success",
)


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


def _message_kv(message: ElectionMessage) -> str:
    parts = [
        f"msg_type={type(message).__name__}",
        f"msg_id={message.msg_id}",
        f"sender={message.sender}",
        f"receiver={message.receiver}",
    ]
    for field in _MESSAGE_FIELDS:
        if hasattr(message, field):
            value = getattr(message, field)
            parts.append(f"{field}={value}")
    return " ".join(parts)


def log_message_event(
    event: str,
    message: ElectionMessage,
    *,
    node_id: int | None = None,
    level: int = logging.INFO,
) -> None:
    logger = logging.getLogger()
    parts = [f"message_event={event}"]
    if node_id is not None:
        parts.append(f"node_id={node_id}")
    parts.append(_message_kv(message))
    logger.log(level, " ".join(parts))
