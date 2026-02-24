from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
import random
from typing import List

from src.messages import AppendEntries, ElectionMessage, RequestVote


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class _RoleBehavior(ABC):
    @property
    @abstractmethod
    def role(self) -> Role:
        pass

    @abstractmethod
    def handle_tick(self, node: RaftNode) -> List[ElectionMessage]:
        pass


class _FollowerBehavior(_RoleBehavior):
    def __init__(self, last_heartbeat_ms: int, next_deadline: int):
        self.last_heartbeat_ms = last_heartbeat_ms
        self.next_deadline = next_deadline

    @property
    def role(self) -> Role:
        return Role.FOLLOWER

    def handle_tick(self, node: RaftNode) -> List[ElectionMessage]:
        if node.current_time_ms - self.last_heartbeat_ms >= self.next_deadline:
            return node._become_candidate()
        return []


class _CandidateBehavior(_RoleBehavior):
    def __init__(
        self,
        last_heartbeat_ms: int,
        next_deadline: int,
        votes_received: int,
        votes_from: set[int],
    ):
        self.last_heartbeat_ms = last_heartbeat_ms
        self.next_deadline = next_deadline
        self.votes_received = votes_received
        self.votes_from = votes_from

    @property
    def role(self) -> Role:
        return Role.CANDIDATE

    def handle_tick(self, node: RaftNode) -> List[ElectionMessage]:
        if self.votes_received >= node._quorum_size():
            return node._become_leader()

        if node.current_time_ms - self.last_heartbeat_ms >= self.next_deadline:
            return node._become_candidate()

        return []


class _LeaderBehavior(_RoleBehavior):
    def __init__(self, heartbeat_interval_ms: int, last_heartbeat_sent_ms: int):
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.last_heartbeat_sent_ms = last_heartbeat_sent_ms

    @property
    def role(self) -> Role:
        return Role.LEADER

    def handle_tick(self, node: RaftNode) -> List[ElectionMessage]:
        if (
            node.current_time_ms - self.last_heartbeat_sent_ms
            >= self.heartbeat_interval_ms
        ):
            self.last_heartbeat_sent_ms = node.current_time_ms
            return [AppendEntries(term=node.current_term, leader_id=node.node_id)]

        return []


class RaftNode:
    """Raft node that delegates role-specific behavior to a composed state object."""

    def __init__(
        self,
        node_id: int,
        seed: int,
        deadline_range: tuple[int, int] = (150, 300),
        cluster_size: int = 3,
    ):
        self.node_id = node_id
        self.cluster_size = cluster_size
        self.current_term = 0
        self.current_time_ms = 0
        self.rng = random.Random(seed)
        self.deadline_range = deadline_range
        self._behavior: _RoleBehavior = _FollowerBehavior(
            last_heartbeat_ms=self.current_time_ms,
            next_deadline=self._draw_election_deadline(),
        )
        self.state = self._behavior.role

    def handle_tick(self, tick_time_ms: int) -> List[ElectionMessage]:
        """Update internal time and execute role-specific tick behavior."""
        self.current_time_ms = tick_time_ms
        return self._behavior.handle_tick(self)

    def _transition_to(self, behavior: _RoleBehavior) -> None:
        self._behavior = behavior
        self.state = behavior.role

    def _draw_election_deadline(self) -> int:
        return self.rng.randint(*self.deadline_range)

    def _quorum_size(self) -> int:
        return (self.cluster_size // 2) + 1

    def _become_follower(self, term: int | None = None) -> None:
        if term is not None and term > self.current_term:
            self.current_term = term
        self._transition_to(
            _FollowerBehavior(
                last_heartbeat_ms=self.current_time_ms,
                next_deadline=self._draw_election_deadline(),
            )
        )

    def _become_candidate(self) -> List[ElectionMessage]:
        self.current_term += 1
        self._transition_to(
            _CandidateBehavior(
                last_heartbeat_ms=self.current_time_ms,
                next_deadline=self._draw_election_deadline(),
                votes_received=1,
                votes_from={self.node_id},
            )
        )
        return [RequestVote(term=self.current_term, candidate_id=self.node_id)]

    def _become_leader(self) -> List[ElectionMessage]:
        self._transition_to(
            _LeaderBehavior(
                heartbeat_interval_ms=max(50, self.deadline_range[0] // 2),
                last_heartbeat_sent_ms=self.current_time_ms,
            )
        )
        return [AppendEntries(term=self.current_term, leader_id=self.node_id)]
