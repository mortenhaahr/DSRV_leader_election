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


class GeneralLatencyFilter:
    """
    Simulates general network latency by randomly delaying messages.
    The delay does not depend on message type or sender/receiver.
    """

    def __init__(self, delay_distribution: tuple[int, int], seed: int):
        self.delay_schedule = {}  # message_id -> deliver_tick
        self.delay_distribution = delay_distribution
        self.random = random.Random(seed)

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if message.msg_id in self.delay_schedule:
            if current_tick >= self.delay_schedule[message.msg_id]:
                del self.delay_schedule[message.msg_id]
                return ScheduleAction.DELIVER
            else:
                return ScheduleAction.DELAY
        else:
            delay = self.random.randint(*self.delay_distribution)
            self.delay_schedule[message.msg_id] = current_tick + delay
            return ScheduleAction.DELAY


class NodeLatencyFilter:
    """
    Simulates network latency for messages sent from a specific node only.
    Other messages are delivered immediately.
    """

    def __init__(self, node_id: int, delay_distribution: tuple[int, int], seed: int):
        self.node_id = node_id
        self._filter = GeneralLatencyFilter(delay_distribution, seed)

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        # Only apply latency if sender matches node_id
        if message.sender == self.node_id:
            return self._filter.filter(message, current_tick)
        else:
            return ScheduleAction.DELIVER


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
