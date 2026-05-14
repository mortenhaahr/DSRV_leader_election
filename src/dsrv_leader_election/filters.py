from __future__ import annotations

import random
from enum import Enum
from typing import Protocol, override

from .messages import ElectionMessage
from .simulation_state import SimulationState


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

    def set_sim_state(self, sim_state: SimulationState) -> None: ...


class LeaderSenderFilter(Filter):
    leader_id: int | None
    inner: Filter

    def __init__(self, inner: Filter):
        self.leader_id = None
        self.inner = inner

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if self.leader_id is not None and message.sender == self.leader_id:
            return self.inner.filter(message, current_tick)
        return ScheduleAction.DELIVER

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.leader_id = sim_state.leader_id


class LeaderReceiverFilter(Filter):
    leader_id: int | None
    inner: Filter

    def __init__(self, inner: Filter):
        self.leader_id = None
        self.inner = inner

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if self.leader_id is not None and message.receiver == self.leader_id:
            return self.inner.filter(message, current_tick)
        return ScheduleAction.DELIVER

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.leader_id = sim_state.leader_id


class LeaderSenderReceiverFilter(Filter):
    leader_id: int | None
    sender_filter: LeaderSenderFilter
    receiver_filter: LeaderReceiverFilter

    def __init__(self, inner: Filter):
        self.leader_id = None
        self.sender_filter = LeaderSenderFilter(inner)
        self.receiver_filter = LeaderReceiverFilter(inner)

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        sender_action = self.sender_filter.filter(message, current_tick)
        receiver_action = self.receiver_filter.filter(message, current_tick)
        return prioritize_actions([sender_action, receiver_action])

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.sender_filter.set_sim_state(sim_state)
        self.receiver_filter.set_sim_state(sim_state)
        self.leader_id = sim_state.leader_id


class TimedFilter(Filter):
    """
    Applies the inner filter only during a specified tick interval.
    """

    inner: Filter
    start_tick: int
    end_tick: int

    def __init__(
        self,
        inner: Filter,
        start_tick: int,
        duration: int,
    ):
        self.inner = inner
        self.start_tick = start_tick
        self.end_tick = start_tick + duration

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if self.start_tick <= current_tick < self.end_tick:
            return self.inner.filter(message, current_tick)
        return ScheduleAction.DELIVER

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.inner.set_sim_state(sim_state)


class SenderFilter(Filter):
    """
    Applies the inner filter only to messages sent by a specific sender.
    """

    inner: Filter
    sender_id: int

    def __init__(
        self,
        inner: Filter,
        sender_id: int,
    ):
        self.inner = inner
        self.sender_id = sender_id

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if message.sender == self.sender_id:
            return self.inner.filter(message, current_tick)
        return ScheduleAction.DELIVER

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.inner.set_sim_state(sim_state)


class ReceiverFilter(Filter):
    """
    Applies the inner filter only to messages received by a specific receiver.
    """

    inner: Filter
    receiver_id: int

    def __init__(
        self,
        inner: Filter,
        receiver_id: int,
    ):
        self.inner = inner
        self.receiver_id = receiver_id

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        if message.receiver == self.receiver_id:
            return self.inner.filter(message, current_tick)
        return ScheduleAction.DELIVER

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.inner.set_sim_state(sim_state)


class SenderReceiverFilter(Filter):
    """
    Applies the inner filter to messages sent or received by a specific node.
    """

    inner: Filter
    sender_filter: SenderFilter
    receiver_filter: ReceiverFilter

    def __init__(
        self,
        inner: Filter,
        node_id: int,
    ):
        self.inner = inner
        self.sender_filter = SenderFilter(inner, sender_id=node_id)
        self.receiver_filter = ReceiverFilter(inner, receiver_id=node_id)

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        sender_action = self.sender_filter.filter(message, current_tick)
        receiver_action = self.receiver_filter.filter(message, current_tick)
        return prioritize_actions([sender_action, receiver_action])

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        self.sender_filter.set_sim_state(sim_state)
        self.receiver_filter.set_sim_state(sim_state)


class LatencyFilter(Filter):
    """
    Delays messages by a random amount, based on a delay distribution.
    """

    delay_schedule: dict[int, int]
    delay_distribution: tuple[int, int]
    random: random.Random

    def __init__(self, delay_distribution: tuple[int, int], seed: int):
        self.delay_schedule = {}  # message_id -> deliver_tick
        self.delay_distribution = delay_distribution
        self.random = random.Random(seed)

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        msg_id = -1 if message.msg_id is None else message.msg_id
        if msg_id in self.delay_schedule:
            if current_tick >= self.delay_schedule[msg_id]:
                del self.delay_schedule[msg_id]
                return ScheduleAction.DELIVER
            return ScheduleAction.DELAY

        delay = self.random.randint(*self.delay_distribution)
        self.delay_schedule[msg_id] = current_tick + delay
        return ScheduleAction.DELAY

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        _ = sim_state
        return None


class CrashFilter(Filter):
    """
    Drops all messages unconditionally.
    """

    @override
    def filter(self, message: ElectionMessage, current_tick: int) -> ScheduleAction:
        _ = message
        _ = current_tick
        return ScheduleAction.DROP

    @override
    def set_sim_state(self, sim_state: SimulationState) -> None:
        _ = sim_state
        return None
