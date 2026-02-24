import random
from collections import defaultdict, deque
from typing import Any, Dict, Deque, List, Tuple


from enum import Enum, auto
from typing import Protocol

from src.messages import ElectionMessage


class ScheduleAction(Enum):
    DELIVER = auto()
    DELAY = auto()
    DROP = auto()


class Filter(Protocol):
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction: ...


class MessageScheduler:
    """
    Generic scheduler for messages/events, extensible via Filter objects.
    Filters determine if a message should be delivered, delayed, or dropped.
    """

    def __init__(self):
        # delivery_tick -> deque of (message, receiver_id)
        self._scheduled = deque()
        self._filters: List[Filter] = []

    def add_filter(self, filter_obj: Filter):
        """Add a Filter object implementing filter()."""
        self._filters.append(filter_obj)

    def schedule_messages(self, messages: List[ElectionMessage]):
        """Schedule multiple messages for delivery."""
        self._scheduled.extend(messages)

    def deliver_messages(self, current_tick: int) -> List[ElectionMessage]:
        """Return all messages scheduled where no filter delays or drops them."""
        to_deliver = []
        remaining = deque()
        while self._scheduled:
            message = self._scheduled.popleft()
            action = ScheduleAction.DELIVER
            for filter_obj in self._filters:
                result = filter_obj.filter(message, current_tick)
                if result == ScheduleAction.DROP:
                    # TODO: Log dropped message in debug
                    action = ScheduleAction.DROP
                    break
                elif result == ScheduleAction.DELAY:
                    action = ScheduleAction.DELAY
            if action == ScheduleAction.DELIVER:
                to_deliver.append(message)
            elif action == ScheduleAction.DELAY:
                remaining.append(message)
        self._scheduled = remaining
        return to_deliver
