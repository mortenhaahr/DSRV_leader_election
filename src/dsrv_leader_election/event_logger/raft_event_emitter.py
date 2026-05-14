from __future__ import annotations

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

    def emit_map(self, event_name: str, payload: dict[str, TCData]) -> None:
        if self.event_logger is None:
            return
        self.event_logger.emit(event_name, TypedTCData("Map", payload))

    def emit_message_event(
        self,
        event_name: str,
        message: ElectionMessage,
        *,
        tick: int,
        node_id: int | None = None,
        extra: dict[str, TCData] | None = None,
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

        self.emit_map(event_name, payload)
