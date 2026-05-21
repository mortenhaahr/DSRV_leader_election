from __future__ import annotations

import logging
import random
from abc import ABC, abstractmethod
from enum import Enum
from typing import override

from .event_logger.raft_event_emitter import RaftEventEmitter
from .log_config import DEBUG, INFO
from .messages import (
    AppendEntries,
    AppendEntriesResponse,
    ElectionMessage,
    RequestVote,
    RequestVoteResponse,
)

_RAFT_MSG_EVENT_NAMES: dict[str, str] = {
    "RequestVote": "request_vote",
    "RequestVoteResponse": "request_vote_response",
    "AppendEntries": "append_entries",
    "AppendEntriesResponse": "append_entries_response",
}


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class _RoleBehaviour(ABC):
    @property
    @abstractmethod
    def role(self) -> Role:
        pass

    @abstractmethod
    def handle_tick(self, node: RaftNode) -> list[ElectionMessage]:
        pass

    @abstractmethod
    def handle_message(
        self, node: RaftNode, message: ElectionMessage
    ) -> list[ElectionMessage]:
        pass


class _FollowerBehaviour(_RoleBehaviour):
    last_heartbeat_ms: int
    next_deadline: int
    voted_for: int | None

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
    @override
    def role(self) -> Role:
        return Role.FOLLOWER

    @override
    def handle_tick(self, node: RaftNode) -> list[ElectionMessage]:
        if node.current_time_ms - self.last_heartbeat_ms >= self.next_deadline:
            return node.become_candidate()
        return []

    def _handle_message_request_vote(
        self, node: RaftNode, message: RequestVote
    ) -> list[ElectionMessage]:
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
            self.next_deadline = node.draw_election_deadline()

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
    ) -> list[ElectionMessage]:
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
        self.next_deadline = node.draw_election_deadline()
        return [
            AppendEntriesResponse(
                term=node.current_term,
                follower_id=node.node_id,
                success=True,
                sender=node.node_id,
                receiver=message.sender,
            )
        ]

    @override
    def handle_message(
        self, node: RaftNode, message: ElectionMessage
    ) -> list[ElectionMessage]:
        if isinstance(message, RequestVote):
            return self._handle_message_request_vote(node, message)

        if isinstance(message, AppendEntries):
            return self._handle_message_append_entries(node, message)

        # TODO: Error response for unexpected message types in this role
        return []


class _CandidateBehaviour(_RoleBehaviour):
    last_heartbeat_ms: int
    next_deadline: int
    votes_received: int
    votes_from: set[int]

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
    @override
    def role(self) -> Role:
        return Role.CANDIDATE

    @override
    def handle_tick(self, node: RaftNode) -> list[ElectionMessage]:
        if self.votes_received >= node.quorum_size():
            return node.become_leader()

        if node.current_time_ms - self.last_heartbeat_ms >= self.next_deadline:
            return node.become_candidate()

        return []

    def _handle_message_request_vote_response(
        self, node: RaftNode, message: RequestVoteResponse
    ) -> list[ElectionMessage]:
        if message.term > node.current_term:
            node.become_follower(term=message.term)
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
                node.quorum_size(),
            )
            if self.votes_received >= node.quorum_size():
                return node.become_leader()

        return []

    def _handle_message_request_vote(
        self, node: RaftNode, message: RequestVote
    ) -> list[ElectionMessage]:
        if message.term > node.current_term:
            node.become_follower(term=message.term)
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
    ) -> list[ElectionMessage]:
        if message.term >= node.current_term:
            node.become_follower(term=message.term)
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

    @override
    def handle_message(
        self, node: RaftNode, message: ElectionMessage
    ) -> list[ElectionMessage]:
        if isinstance(message, RequestVoteResponse):
            return self._handle_message_request_vote_response(node, message)

        if isinstance(message, RequestVote):
            return self._handle_message_request_vote(node, message)

        if isinstance(message, AppendEntries):
            return self._handle_message_append_entries(node, message)

        return []


class _LeaderBehaviour(_RoleBehaviour):
    heartbeat_interval_ms: int
    last_heartbeat_sent_ms: int

    def __init__(self, heartbeat_interval_ms: int, last_heartbeat_sent_ms: int):
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.last_heartbeat_sent_ms = last_heartbeat_sent_ms

    @property
    @override
    def role(self) -> Role:
        return Role.LEADER

    @override
    def handle_tick(self, node: RaftNode) -> list[ElectionMessage]:
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
    ) -> list[ElectionMessage]:
        if message.term > node.current_term:
            node.become_follower(term=message.term)
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
    ) -> list[ElectionMessage]:
        if message.term > node.current_term:
            node.become_follower(term=message.term)
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
    ) -> list[ElectionMessage]:
        if message.term > node.current_term:
            node.become_follower(term=message.term)
            return []
        return []

    @override
    def handle_message(
        self, node: RaftNode, message: ElectionMessage
    ) -> list[ElectionMessage]:
        if isinstance(message, RequestVote):
            return self._handle_message_request_vote(node, message)

        if isinstance(message, AppendEntries):
            return self._handle_message_append_entries(node, message)

        if isinstance(message, AppendEntriesResponse):
            return self._handle_message_append_entries_response(node, message)

        return []


class RaftNode:
    """Raft node that delegates role-specific behaviour to a composed state object."""

    node_id: int
    cluster_size: int
    current_term: int
    current_time_ms: int
    rng: random.Random
    deadline_range: tuple[int, int]
    heartbeat_interval_ms: int
    _behaviour: _RoleBehaviour
    state: Role
    event_emitter: RaftEventEmitter

    def __init__(
        self,
        node_id: int,
        seed: int,
        deadline_range: tuple[int, int],
        heartbeat_interval_ms: int,
        cluster_size: int,
        event_emitter: RaftEventEmitter | None = None,
    ):

        self.node_id = node_id
        self.cluster_size = cluster_size
        self.current_term = 0
        self.event_emitter = event_emitter or RaftEventEmitter()

        self.current_time_ms = 0
        self.rng = random.Random(seed)
        self.deadline_range = deadline_range
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self._behaviour = _FollowerBehaviour(
            last_heartbeat_ms=self.current_time_ms,
            next_deadline=self.draw_election_deadline(),
        )
        self.state = self._behaviour.role
        self.event_emitter.emit_map(
            "node_initialized",
            {
                "event": "node_initialized",
                "tick": self.current_time_ms,
                "node_id": self.node_id,
                "term": self.current_term,
                "role": self.state.value,
                "cluster_size": self.cluster_size,
            },
        )

    def _emit_generated_messages(self, messages: list[ElectionMessage]) -> None:
        if not messages:
            return
        for msg in messages:
            event_name = _RAFT_MSG_EVENT_NAMES.get(
                type(msg).__name__, "message_generated"
            )
            self.event_emitter.emit_message_event(
                event_name,
                msg,
                tick=self.current_time_ms,
                node_id=self.node_id,
                level=DEBUG,
            )

    def handle_tick(self, tick_time_ms: int) -> list[ElectionMessage]:
        """Update internal time and execute role-specific tick behaviour."""
        self.current_time_ms = tick_time_ms
        messages = self._behaviour.handle_tick(self)
        self._emit_generated_messages(messages)
        return messages

    def handle_message(self, message: ElectionMessage) -> list[ElectionMessage]:
        """Dispatch an incoming election message to the current role behaviour."""
        self.event_emitter.emit_message_event(
            "message_received",
            message,
            tick=self.current_time_ms,
            node_id=self.node_id,
        )
        messages = self._behaviour.handle_message(self, message)
        self._emit_generated_messages(messages)
        return messages

    def _transition_to(self, behaviour: _RoleBehaviour) -> None:
        logger = logging.getLogger()
        from_role = self.state.value
        to_role = behaviour.role.value
        logger.log(
            INFO,
            "node_event=transition node_id=%s from_role=%s to_role=%s term=%s",
            self.node_id,
            from_role,
            to_role,
            self.current_term,
        )
        self.event_emitter.emit_map(
            "node_role_transition",
            {
                "event": "node_role_transition",
                "tick": self.current_time_ms,
                "node_id": self.node_id,
                "from_role": from_role,
                "to_role": to_role,
                "term": self.current_term,
            },
        )
        self._behaviour = behaviour
        self.state = behaviour.role

    def draw_election_deadline(self) -> int:
        return self.rng.randint(*self.deadline_range)

    def quorum_size(self) -> int:
        return (self.cluster_size // 2) + 1

    def become_follower(self, term: int | None = None) -> None:
        previous_term = self.current_term
        if term is not None and term > self.current_term:
            self.current_term = term
        if self.current_term != previous_term:
            self.event_emitter.emit_map(
                "node_term_changed",
                {
                    "event": "node_term_changed",
                    "tick": self.current_time_ms,
                    "node_id": self.node_id,
                    "from_term": previous_term,
                    "to_term": self.current_term,
                },
            )

        self._transition_to(
            _FollowerBehaviour(
                last_heartbeat_ms=self.current_time_ms,
                next_deadline=self.draw_election_deadline(),
                voted_for=None,
            )
        )

    def become_candidate(self) -> list[ElectionMessage]:
        previous_term = self.current_term
        self.current_term += 1
        self.event_emitter.emit_map(
            "node_term_changed",
            {
                "event": "node_term_changed",
                "tick": self.current_time_ms,
                "node_id": self.node_id,
                "from_term": previous_term,
                "to_term": self.current_term,
            },
        )

        self._transition_to(
            _CandidateBehaviour(
                last_heartbeat_ms=self.current_time_ms,
                next_deadline=self.draw_election_deadline(),
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

    def become_leader(self) -> list[ElectionMessage]:

        leader_behaviour = _LeaderBehaviour(
            heartbeat_interval_ms=self.heartbeat_interval_ms,
            last_heartbeat_sent_ms=self.current_time_ms,
        )
        # When sending initial AppendEntries, update last_heartbeat_sent_ms to current_time_ms + heartbeat_interval
        leader_behaviour.last_heartbeat_sent_ms = (
            self.current_time_ms + self.heartbeat_interval_ms
        )
        self._transition_to(leader_behaviour)
        self.event_emitter.emit_map(
            "leader_elected",
            {
                "event": "leader_elected",
                "tick": self.current_time_ms,
                "node_id": self.node_id,
                "term": self.current_term,
            },
        )
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
