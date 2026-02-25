from __future__ import annotations

from enum import Enum
from typing import Protocol, runtime_checkable
import random

from src.messages import ElectionMessage
from src.simulation_state import SimulationState


# Should propably live in message_scheduler.py, but this avoids recursive imports
class ScheduleAction(Enum):
    DELIVER = 0
    DELAY = 1
    DROP = 2


def prioritize_actions(actions: list[ScheduleAction]) -> ScheduleAction:
    """Return the highest-priority action from a list of ScheduleActions."""
    if not actions:
        return ScheduleAction.DELIVER
    return max(actions, key=lambda a: a.value)


class Filter(Protocol):
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction: ...


@runtime_checkable
class StatefulFilter(Filter, Protocol):
    def set_sim_state(self, sim_state: SimulationState) -> None: ...


class LeaderSenderFilter(StatefulFilter):
    def __init__(self, inner: Filter):
        self.leader_id: int | None = None
        self.inner = inner

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if self.leader_id is not None and message.sender == self.leader_id:
            return ScheduleAction.DROP
        else:
            return self.inner.filter(message, current_tick)

    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.leader_id = sim_state.leader_id


class LeaderReceiverFilter(StatefulFilter):
    def __init__(self, inner: Filter):
        self.leader_id: int | None = None
        self.inner = inner

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if self.leader_id is not None and message.receiver == self.leader_id:
            return ScheduleAction.DROP
        else:
            return self.inner.filter(message, current_tick)

    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.leader_id = sim_state.leader_id


class LeaderSenderReceiverFilter(StatefulFilter):
    def __init__(self, inner: Filter):
        self.leader_id: int | None = None
        self.sender_filter = LeaderSenderFilter(inner)
        self.receiver_filter = LeaderReceiverFilter(inner)

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        sender_action = self.sender_filter.filter(message, current_tick)
        receiver_action = self.receiver_filter.filter(message, current_tick)
        return prioritize_actions([sender_action, receiver_action])

    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.sender_filter.set_sim_state(sim_state)
        self.receiver_filter.set_sim_state(sim_state)


class TimedFilter:
    """
    Applies the inner filter only during a specified tick interval.
    """

    def __init__(
        self,
        inner: Filter,
        start_tick: int,
        duration: int,
    ):
        self.inner = inner
        self.start_tick = start_tick
        self.end_tick = start_tick + duration

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if self.start_tick <= current_tick < self.end_tick:
            return self.inner.filter(message, current_tick)
        else:
            return ScheduleAction.DELIVER


class SenderFilter:
    """
    Applies the inner filter only to messages sent by a specific sender.
    """

    def __init__(
        self,
        inner: Filter,
        sender_id: int,
    ):
        self.inner = inner
        self.sender_id = sender_id

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if message.sender == self.sender_id:
            return self.inner.filter(message, current_tick)
        else:
            return ScheduleAction.DELIVER


class ReceiverFilter:
    """
    Applies the inner filter only to messages received by a specific receiver.
    """

    def __init__(
        self,
        inner: Filter,
        receiver_id: int,
    ):
        self.inner = inner
        self.receiver_id = receiver_id

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if message.receiver == self.receiver_id:
            return self.inner.filter(message, current_tick)
        else:
            return ScheduleAction.DELIVER


class SenderReceiverFilter:
    """
    Applies the inner filter to messages sent or received by a specific node.
    """

    def __init__(
        self,
        inner: Filter,
        node_id: int,
    ):
        self.inner = inner
        self.sender_filter = SenderFilter(inner, sender_id=node_id)
        self.receiver_filter = ReceiverFilter(inner, receiver_id=node_id)

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        sender_action = self.sender_filter.filter(message, current_tick)
        receiver_action = self.receiver_filter.filter(message, current_tick)
        return prioritize_actions([sender_action, receiver_action])


class LatencyFilter:
    """
    Delays messages by a random amount, based on a delay distribution.
    """

    def __init__(self, delay_distribution: tuple[int, int], seed: int):
        self.delay_schedule = {}  # message_id -> deliver_tick
        self.delay_distribution = delay_distribution
        self.random = random.Random(seed)

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        msg_id = message.msg_id
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


class CrashFilter:
    """
    Drops all messages unconditionally.
    """

    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        return ScheduleAction.DROP
