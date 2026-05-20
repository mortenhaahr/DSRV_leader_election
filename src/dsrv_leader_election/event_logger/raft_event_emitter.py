from __future__ import annotations

import logging
from dataclasses import dataclass

from ..messages import (
    AppendEntries,
    AppendEntriesResponse,
    ElectionMessage,
    RequestVote,
    RequestVoteResponse,
)
from .rv_logger import EventLogger
from .tc_types import TCData, TypedTCData


@dataclass
class RaftEventEmitter:
    event_logger: EventLogger | None = None

    def is_enabled(self) -> bool:
        return self.event_logger is not None

    def _log_console(
        self,
        event_name: str,
        payload: dict[str, TCData],
        *,
        level: int,
    ) -> None:
        logger = logging.getLogger()
        kv = " ".join(f"{k}={v}" for k, v in payload.items() if k != "event")

        logger.log(level, "event=%s %s", event_name, kv)

    def emit_map(
        self,
        event_name: str,
        payload: dict[str, TCData],
        *,
        level: int = logging.INFO,
    ) -> None:
        self._log_console(event_name, payload, level=level)
        if self.event_logger is None:
            return
        value = TypedTCData("Map", payload)
        self.event_logger.emit(event_name, value)
        # Also emit on a per-node topic for any event that carries a node_id,
        # using the "{event_name}_node_{node_id}" var-name convention so that
        # consumers can subscribe to a single node's stream of events.
        node_id = payload.get("node_id")
        if isinstance(node_id, int):
            self.event_logger.emit(f"{event_name}_node_{node_id}", value)

    def emit_message_event(
        self,
        event_name: str,
        message: ElectionMessage,
        *,
        tick: int,
        node_id: int | None = None,
        extra: dict[str, TCData] | None = None,
        level: int = logging.INFO,
    ) -> None:

        payload: dict[str, TCData] = {
            "event": event_name,
            "tick": tick,
            "msg_type": type(message).__name__,
            "msg_id": -1 if message.msg_id is None else message.msg_id,
            "sender": message.sender,
            "receiver": message.receiver,
        }

        if isinstance(message, RequestVote):
            payload["term"] = message.term
            payload["candidate_id"] = message.candidate_id
        elif isinstance(message, RequestVoteResponse):
            payload["term"] = message.term
            payload["voter_id"] = message.voter_id
            payload["vote_granted"] = message.vote_granted
        elif isinstance(message, AppendEntries):
            payload["term"] = message.term
            payload["leader_id"] = message.leader_id
        elif isinstance(message, AppendEntriesResponse):
            payload["term"] = message.term
            payload["follower_id"] = message.follower_id
            payload["success"] = message.success

        if node_id is not None:
            payload["node_id"] = node_id

        if extra is not None:
            payload.update(extra)

        self.emit_map(event_name, payload, level=level)
