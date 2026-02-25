from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum

import logging
import random
from typing import List

from src.messages import (
    AppendEntries,
    AppendEntriesResponse,
    ElectionMessage,
    RequestVote,
    RequestVoteResponse,
)
from src.log_config import DEBUG, INFO, log_message_event


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

    @abstractmethod
    def handle_message(
        self, node: RaftNode, message: ElectionMessage
    ) -> List[ElectionMessage]:
        pass


class _FollowerBehavior(_RoleBehavior):
    def __init__(
        self,
        last_heartbeat_ms: int,
        next_deadline: int,
        voted_for: int | None = None,
    ):
        self.last_heartbeat_ms = last_heartbeat_ms
        self.next_deadline = next_deadline
        self.voted_for = voted_for

    @property
    def role(self) -> Role:
        return Role.FOLLOWER

    def handle_tick(self, node: RaftNode) -> List[ElectionMessage]:
        if node.current_time_ms - self.last_heartbeat_ms >= self.next_deadline:
            return node._become_candidate()
        return []

    def _handle_message_request_vote(
        self, node: RaftNode, message: RequestVote
    ) -> List[ElectionMessage]:
        if message.term < node.current_term:
            return [
                RequestVoteResponse(
                    term=node.current_term,
                    voter_id=node.node_id,
                    vote_granted=False,
                    sender=node.node_id,
                    receiver=message.sender,
                )
            ]

        if message.term > node.current_term:
            node.current_term = message.term
            self.voted_for = None

        grant_vote = self.voted_for in (None, message.candidate_id)
        if grant_vote:
            self.voted_for = message.candidate_id
            self.last_heartbeat_ms = node.current_time_ms
            self.next_deadline = node._draw_election_deadline()

        return [
            RequestVoteResponse(
                term=node.current_term,
                voter_id=node.node_id,
                vote_granted=grant_vote,
                sender=node.node_id,
                receiver=message.sender,
            )
        ]

    def _handle_message_append_entries(
        self, node: RaftNode, message: AppendEntries
    ) -> List[ElectionMessage]:
        if message.term < node.current_term:
            return [
                AppendEntriesResponse(
                    term=node.current_term,
                    follower_id=node.node_id,
                    success=False,
                    sender=node.node_id,
                    receiver=message.sender,
                )
            ]

        if message.term > node.current_term:
            node.current_term = message.term
            self.voted_for = None

        self.last_heartbeat_ms = node.current_time_ms
        self.next_deadline = node._draw_election_deadline()
        return [
            AppendEntriesResponse(
                term=node.current_term,
                follower_id=node.node_id,
                success=True,
                sender=node.node_id,
                receiver=message.sender,
            )
        ]

    def handle_message(
        self, node: RaftNode, message: ElectionMessage
    ) -> List[ElectionMessage]:
        if isinstance(message, RequestVote):
            return self._handle_message_request_vote(node, message)

        if isinstance(message, AppendEntries):
            return self._handle_message_append_entries(node, message)

        # TODO: Error response for unexpected message types in this role
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

    def _handle_message_request_vote_response(
        self, node: RaftNode, message: RequestVoteResponse
    ) -> List[ElectionMessage]:
        if message.term > node.current_term:
            node._become_follower(term=message.term)
            return []

        if message.term < node.current_term:
            return []

        if message.vote_granted and message.voter_id not in self.votes_from:
            self.votes_from.add(message.voter_id)
            self.votes_received += 1
            logger = logging.getLogger()
            logger.log(
                INFO,
                "node_event=vote_received node_id=%s voter_id=%s votes_received=%s quorum=%s",
                node.node_id,
                message.voter_id,
                self.votes_received,
                node._quorum_size(),
            )
            if self.votes_received >= node._quorum_size():
                return node._become_leader()

        return []

    def _handle_message_request_vote(
        self, node: RaftNode, message: RequestVote
    ) -> List[ElectionMessage]:
        if message.term > node.current_term:
            node._become_follower(term=message.term)
            return node.handle_message(message)

        return [
            RequestVoteResponse(
                term=node.current_term,
                voter_id=node.node_id,
                vote_granted=False,
                sender=node.node_id,
                receiver=message.sender,
            )
        ]

    def _handle_message_append_entries(
        self, node: RaftNode, message: AppendEntries
    ) -> List[ElectionMessage]:
        if message.term >= node.current_term:
            node._become_follower(term=message.term)
            return node.handle_message(message)

        # Send failure response to lower term AppendEntries
        return [
            AppendEntriesResponse(
                term=node.current_term,
                follower_id=node.node_id,
                success=False,
                sender=node.node_id,
                receiver=message.sender,
            )
        ]

    def handle_message(
        self, node: RaftNode, message: ElectionMessage
    ) -> List[ElectionMessage]:
        if isinstance(message, RequestVoteResponse):
            return self._handle_message_request_vote_response(node, message)

        if isinstance(message, RequestVote):
            return self._handle_message_request_vote(node, message)

        if isinstance(message, AppendEntries):
            return self._handle_message_append_entries(node, message)

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
            # Send AppendEntries to all other nodes
            return [
                AppendEntries(
                    term=node.current_term,
                    leader_id=node.node_id,
                    sender=node.node_id,
                    receiver=other_id,
                )
                for other_id in range(node.cluster_size)
                if other_id != node.node_id
            ]

        return []

    def _handle_message_request_vote(
        self, node: RaftNode, message: RequestVote
    ) -> List[ElectionMessage]:
        if message.term > node.current_term:
            node._become_follower(term=message.term)
            return node.handle_message(message)

        return [
            RequestVoteResponse(
                term=node.current_term,
                voter_id=node.node_id,
                vote_granted=False,
                sender=node.node_id,
                receiver=message.sender,
            )
        ]

    def _handle_message_append_entries(
        self, node: RaftNode, message: AppendEntries
    ) -> List[ElectionMessage]:
        if message.term > node.current_term:
            node._become_follower(term=message.term)
            return node.handle_message(message)

        # Send failure response to lower term AppendEntries
        return [
            AppendEntriesResponse(
                term=node.current_term,
                follower_id=node.node_id,
                success=False,
                sender=node.node_id,
                receiver=message.sender,
            )
        ]

    def _handle_message_append_entries_response(
        self, node: RaftNode, message: AppendEntriesResponse
    ) -> List[ElectionMessage]:
        if message.term > node.current_term:
            node._become_follower(term=message.term)
            return []
        return []

    def handle_message(
        self, node: RaftNode, message: ElectionMessage
    ) -> List[ElectionMessage]:
        if isinstance(message, RequestVote):
            return self._handle_message_request_vote(node, message)

        if isinstance(message, AppendEntries):
            return self._handle_message_append_entries(node, message)

        if isinstance(message, AppendEntriesResponse):
            return self._handle_message_append_entries_response(node, message)

        return []


class RaftNode:
    """Raft node that delegates role-specific behavior to a composed state object."""

    def __init__(
        self,
        node_id: int,
        seed: int,
        deadline_range: tuple[int, int],
        heartbeat_interval_ms: int,
        cluster_size: int,
    ):
        self.node_id = node_id
        self.cluster_size = cluster_size
        self.current_term = 0
        self.current_time_ms = 0
        self.rng = random.Random(seed)
        self.deadline_range = deadline_range
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self._behavior: _RoleBehavior = _FollowerBehavior(
            last_heartbeat_ms=self.current_time_ms,
            next_deadline=self._draw_election_deadline(),
        )
        self.state = self._behavior.role

    def handle_tick(self, tick_time_ms: int) -> List[ElectionMessage]:
        """Update internal time and execute role-specific tick behavior."""
        self.current_time_ms = tick_time_ms
        messages = self._behavior.handle_tick(self)
        if messages:
            for msg in messages:
                log_message_event(
                    "generate",
                    msg,
                    node_id=self.node_id,
                    level=DEBUG,
                )
        return messages

    def handle_message(self, message: ElectionMessage) -> List[ElectionMessage]:
        """Dispatch an incoming election message to the current role behavior."""
        messages = self._behavior.handle_message(self, message)
        if messages:
            for msg in messages:
                log_message_event(
                    "generate",
                    msg,
                    node_id=self.node_id,
                    level=DEBUG,
                )
        return messages

    def _transition_to(self, behavior: _RoleBehavior) -> None:
        logger = logging.getLogger()
        logger.log(
            INFO,
            "node_event=transition node_id=%s from_role=%s to_role=%s term=%s",
            self.node_id,
            self.state.value,
            behavior.role.value,
            self.current_term,
        )
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
                voted_for=None,
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
        # Send RequestVote to all other nodes
        return [
            RequestVote(
                term=self.current_term,
                candidate_id=self.node_id,
                sender=self.node_id,
                receiver=other_id,
            )
            for other_id in range(self.cluster_size)
            if other_id != self.node_id
        ]

    def _become_leader(self) -> List[ElectionMessage]:
        leader_behavior = _LeaderBehavior(
            heartbeat_interval_ms=self.heartbeat_interval_ms,
            last_heartbeat_sent_ms=self.current_time_ms,
        )
        # When sending initial AppendEntries, update last_heartbeat_sent_ms to current_time_ms + heartbeat_interval
        leader_behavior.last_heartbeat_sent_ms = (
            self.current_time_ms + self.heartbeat_interval_ms
        )
        self._transition_to(leader_behavior)
        # Send initial AppendEntries to all other nodes as heartbeat
        return [
            AppendEntries(
                term=self.current_term,
                leader_id=self.node_id,
                sender=self.node_id,
                receiver=other_id,
            )
            for other_id in range(self.cluster_size)
            if other_id != self.node_id
        ]
