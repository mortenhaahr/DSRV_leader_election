from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Union


# -----------------
# Election messages
# -----------------


class ElectionMessage:
    # TODO: Add from sender to receiver... and support for broadcast messages
    pass


@dataclass(frozen=True, slots=True)
class RequestVote(ElectionMessage):
    term: int
    candidate_id: int


@dataclass(frozen=True, slots=True)
class RequestVoteResponse(ElectionMessage):
    term: int
    voter_id: int
    vote_granted: bool


@dataclass(frozen=True, slots=True)
class AppendEntries(ElectionMessage):
    term: int
    leader_id: int


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse(ElectionMessage):
    term: int
    follower_id: int
    success: bool


# -------------
# Event messages
# -------------


@dataclass(frozen=True, slots=True)
class FailureEvent:
    action: Literal["crash", "recover"]


@dataclass(frozen=True, slots=True)
class ClockTickEvent:
    action: Literal["tick"]
    sim_tick: int
    sim_time_s: float


AnyMessage = Union[
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    FailureEvent,
    ClockTickEvent,
]
