from __future__ import annotations

from collections import deque
from typing import List
import logging

from src.messages import ElectionMessage
from src.filters import Filter, ScheduleAction, StatefulFilter, prioritize_actions
from src.simulation_state import SimulationState


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

    def update_state(self, sim_state: SimulationState) -> None:
        """Update stateful filters with the latest simulation state."""
        for filter_obj in self._filters:
            if isinstance(filter_obj, StatefulFilter):
                filter_obj.set_sim_state(sim_state)

    def deliver_messages(self, current_tick: int) -> List[ElectionMessage]:
        """Return all messages scheduled where no filter delays or drops them."""
        logger = logging.getLogger()
        to_deliver = []
        remaining = deque()
        while self._scheduled:
            message = self._scheduled.popleft()
            actions = [
                filter_obj.filter(message, current_tick) for filter_obj in self._filters
            ]
            action = prioritize_actions(actions)
            if action == ScheduleAction.DROP:
                logger.debug(f"Dropping message {message}")
            elif action == ScheduleAction.DELIVER:
                to_deliver.append(message)
            elif action == ScheduleAction.DELAY:
                remaining.append(message)
        self._scheduled = remaining
        return to_deliver
