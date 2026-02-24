from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Union


# -----------------
# Election messages
# -----------------


class ElectionMessage:
    def __init__(self, sender: int, receiver: int):
        self.sender = sender
        self.receiver = receiver


@dataclass(frozen=True, slots=True)
class RequestVote(ElectionMessage):
    term: int
    candidate_id: int
    sender: int
    receiver: int


@dataclass(frozen=True, slots=True)
class RequestVoteResponse(ElectionMessage):
    term: int
    voter_id: int
    vote_granted: bool
    sender: int
    receiver: int


@dataclass(frozen=True, slots=True)
class AppendEntries(ElectionMessage):
    term: int
    leader_id: int
    sender: int
    receiver: int


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse(ElectionMessage):
    term: int
    follower_id: int
    success: bool
    sender: int
    receiver: int


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
