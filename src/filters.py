from enum import Enum, auto
from typing import Protocol
from src.messages import ElectionMessage
import random


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
        msg_id = getattr(message, "message_id", None)
        if msg_id in self.delay_schedule:
            if current_tick >= self.delay_schedule[msg_id]:
                del self.delay_schedule[msg_id]
                return ScheduleAction.DELIVER
            else:
                return ScheduleAction.DELAY
        else:
            delay = self.random.randint(*self.delay_distribution)
            self.delay_schedule[msg_id] = current_tick + delay
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
        if message.sender == self.node_id:
            return self._filter.filter(message, current_tick)
        else:
            return ScheduleAction.DELIVER


class CrashFilter:
    """
    Drops all messages sent or received by a node during a crash interval.
    Messages are dropped if current_tick is in [start_tick, start_tick + duration).
    """

    def __init__(self, node_id: int, start_tick: int, duration: int):
        self.node_id = node_id
        self.start_tick = start_tick
        self.end_tick = start_tick + duration

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if self.start_tick <= current_tick < self.end_tick:
            if message.sender == self.node_id or message.receiver == self.node_id:
                return ScheduleAction.DROP
        return ScheduleAction.DELIVER
