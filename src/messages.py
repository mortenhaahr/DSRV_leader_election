from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Union


# -----------------
# Election messages
# -----------------


class ElectionMessage:
    """
    Base class for all election-related messages in the Raft protocol.
    """

    def __init__(self, sender: int, receiver: int):
        self.sender = sender
        self.receiver = receiver


@dataclass(frozen=True, slots=True)
class RequestVote(ElectionMessage):
    """
    Sent by a candidate to all other nodes to request votes during an election.
    """

    term: int
    candidate_id: int
    sender: int
    receiver: int


@dataclass(frozen=True, slots=True)
class RequestVoteResponse(ElectionMessage):
    """
    Indicates whether the vote was granted for the given term and candidate.
    """

    term: int
    voter_id: int
    vote_granted: bool
    sender: int
    receiver: int


@dataclass(frozen=True, slots=True)
class AppendEntries(ElectionMessage):
    """
    Sent by the leader to all followers to maintain leadership.
    In a full Raft implementation, this would also replicate log entries.
    Here, it acts as a heartbeat for leader election only.
    """

    term: int
    leader_id: int
    sender: int
    receiver: int


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse(ElectionMessage):
    """
    Sent by a follower in response to an AppendEntries message from the leader.
    Indicates whether the follower accepted the leader's authority for the given term.
    """

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
    """
    Simulation event to indicate a node crash or recovery.
    Not part of the Raft protocol, but used for testing fault tolerance.
    """

    action: Literal["crash", "recover"]


@dataclass(frozen=True, slots=True)
class ClockTickEvent:
    """
    Simulation event to advance the logical clock and trigger node ticks.
    Not part of the Raft protocol, but used to drive the simulation.
    """

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
