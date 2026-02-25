from __future__ import annotations

from dataclasses import dataclass

# -----------------
# Election messages
# -----------------


class ElectionMessage:
    """
    Base class for all election-related messages in the Raft protocol.
    Adds a unique msg_id to each message.
    """

    _id_counter = 0

    @classmethod
    def next_id(cls) -> int:
        cls._id_counter += 1
        return cls._id_counter

    def __init__(self, sender: int, receiver: int, msg_id: int | None = None):
        self.sender = sender
        self.receiver = receiver
        self.msg_id = msg_id if msg_id is not None else self.next_id()


@dataclass(frozen=True, slots=True)
class RequestVote(ElectionMessage):
    """
    Sent by a candidate to all other nodes to request votes during an election.
    """

    term: int
    candidate_id: int
    sender: int
    receiver: int
    msg_id: int | None = None

    def __post_init__(self):
        # Bypass frozen to set msg_id if not provided
        if self.msg_id is None:
            object.__setattr__(self, "msg_id", ElectionMessage.next_id())


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
    msg_id: int | None = None

    def __post_init__(self):
        if self.msg_id is None:
            object.__setattr__(self, "msg_id", ElectionMessage.next_id())


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
    msg_id: int | None = None

    def __post_init__(self):
        if self.msg_id is None:
            object.__setattr__(self, "msg_id", ElectionMessage.next_id())


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
    msg_id: int | None = None

    def __post_init__(self):
        if self.msg_id is None:
            object.__setattr__(self, "msg_id", ElectionMessage.next_id())
