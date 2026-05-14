from __future__ import annotations

from collections import deque

from src.filters import Filter, ScheduleAction, prioritize_actions
from src.log_config import DEBUG, log_message_event
from src.messages import ElectionMessage
from src.simulation_state import SimulationState


class MessageScheduler:
    """
    Generic scheduler for messages/events, extensible via Filter objects.
    Filters determine if a message should be delivered, delayed, or dropped.
    """

    _scheduled: deque[ElectionMessage]
    _filters: list[Filter]
    _sim_state: SimulationState | None

    def __init__(self) -> None:
        self._scheduled = deque[ElectionMessage]()
        self._filters = []
        self._sim_state = None

    def add_filter(self, filter_obj: Filter) -> None:
        """Add a Filter object implementing filter()."""
        self._filters.append(filter_obj)

    def schedule_messages(self, messages: list[ElectionMessage]) -> None:
        """Schedule multiple messages for delivery."""
        for message in messages:
            log_message_event("schedule", message)
        self._scheduled.extend(messages)

    def update_state(self, sim_state: SimulationState) -> None:
        """Update stateful filters with the latest simulation state."""
        self._sim_state = sim_state
        for filter_obj in self._filters:
            filter_obj.set_sim_state(sim_state)

    def deliver_messages(self, current_tick: int) -> list[ElectionMessage]:
        """Return all messages scheduled where no filter delays or drops them."""
        to_deliver: list[ElectionMessage] = []
        remaining: deque[ElectionMessage] = deque()

        while self._scheduled:
            message = self._scheduled.popleft()
            actions = [
                filter_obj.filter(message, current_tick) for filter_obj in self._filters
            ]
            action = prioritize_actions(actions)
            if action == ScheduleAction.DROP:
                log_message_event("drop", message, level=DEBUG)
            elif action == ScheduleAction.DELIVER:
                to_deliver.append(message)
            elif action == ScheduleAction.DELAY:
                log_message_event("delay", message, level=DEBUG)
                remaining.append(message)

        self._scheduled = remaining
        return to_deliver
