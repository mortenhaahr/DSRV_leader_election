from __future__ import annotations

from enum import Enum
import random
from typing import Any, List, cast

from src.messages import AppendEntries, ElectionMessage, RequestVote


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    """Base Raft node containing shared state and transition helpers."""

    role = Role.FOLLOWER

    def __init__(
        self,
        node_id: int,
        seed: int,
        deadline_range: tuple[int, int] = (150, 300),
        cluster_size: int = 3,
    ):
        self.node_id = node_id
        self.state = self.role
        self.cluster_size = cluster_size
        self.current_term = 0
        self.current_time_ms = 0
        self.last_heartbeat_ms = 0
        self.last_heartbeat_sent_ms = 0
        self.rng = random.Random(seed)
        self.deadline_range = deadline_range
        self.heartbeat_interval_ms = max(50, self.deadline_range[0] // 2)
        self.next_deadline = self.rng.randint(*self.deadline_range)
        self.votes_received = 0
        self.votes_from: set[int] = set()

    def handle_tick(self, tick_time_ms: int) -> List[ElectionMessage]:
        """Update internal time and execute role-specific tick behavior."""
        self.current_time_ms = tick_time_ms
        return self._handle_tick()

    def _handle_tick(self) -> List[ElectionMessage]:
        raise NotImplementedError()

    def _transition_to(self, node_class: type[RaftNode]) -> None:
        dynamic_self = cast(Any, self)
        dynamic_self.__class__ = node_class
        self.state = node_class.role

    def _reset_election_deadline(self) -> None:
        self.next_deadline = self.rng.randint(*self.deadline_range)

    def _quorum_size(self) -> int:
        return (self.cluster_size // 2) + 1

    def _become_follower(self, term: int | None = None) -> None:
        if term is not None and term > self.current_term:
            self.current_term = term
        self._transition_to(Follower)
        self.votes_received = 0
        self.votes_from.clear()
        self.last_heartbeat_ms = self.current_time_ms
        self._reset_election_deadline()

    def _become_candidate(self) -> List[ElectionMessage]:
        self._transition_to(Candidate)
        self.current_term += 1
        self.last_heartbeat_ms = self.current_time_ms
        self._reset_election_deadline()
        self.votes_received = 1  # Vote for self
        self.votes_from = {self.node_id}
        return [RequestVote(term=self.current_term, candidate_id=self.node_id)]

    def _become_leader(self) -> List[ElectionMessage]:
        self._transition_to(Leader)
        self.last_heartbeat_sent_ms = self.current_time_ms
        return [AppendEntries(term=self.current_term, leader_id=self.node_id)]


class Follower(RaftNode):
    """Follower role: starts an election when timeout expires."""

    role = Role.FOLLOWER

    def _handle_tick(self) -> List[ElectionMessage]:
        if self.current_time_ms - self.last_heartbeat_ms >= self.next_deadline:
            return self._become_candidate()
        return []


class Candidate(RaftNode):
    """Candidate role: becomes leader on quorum or restarts election on timeout."""

    role = Role.CANDIDATE

    def _handle_tick(self) -> List[ElectionMessage]:
        if self.votes_received >= self._quorum_size():
            return self._become_leader()

        if self.current_time_ms - self.last_heartbeat_ms >= self.next_deadline:
            return self._become_candidate()

        return []


class Leader(RaftNode):
    """Leader role: sends periodic heartbeats."""

    role = Role.LEADER

    def _handle_tick(self) -> List[ElectionMessage]:
        if (
            self.current_time_ms - self.last_heartbeat_sent_ms
            >= self.heartbeat_interval_ms
        ):
            self.last_heartbeat_sent_ms = self.current_time_ms
            return [AppendEntries(term=self.current_term, leader_id=self.node_id)]

        return []
