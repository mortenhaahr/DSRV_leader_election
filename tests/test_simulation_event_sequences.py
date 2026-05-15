from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from dsrv_leader_election.config_loader import (
    coerce_float,
    coerce_int,
    load_simulation_from_file,
)
from dsrv_leader_election.event_logger.mock_logger import (
    EmittedLogMessage,
    MockEventLogger,
)
from dsrv_leader_election.event_logger.raft_event_emitter import RaftEventEmitter
from dsrv_leader_election.event_logger.tc_types import TCData
from dsrv_leader_election.event_logger.topic_mapping import TopicMapping
from dsrv_leader_election.simulation import Simulation


def _run_config(config_filename: str) -> tuple[Simulation, list[EmittedLogMessage]]:
    config_path = Path("tests/fixtures/configs") / config_filename

    logger = MockEventLogger(topic_mapping=TopicMapping({}))
    emitter = RaftEventEmitter(event_logger=logger)

    simulation = load_simulation_from_file(str(config_path), event_emitter=emitter)
    simulation.run()
    return simulation, logger.emitted_messages


def _event_payloads(
    messages: list[EmittedLogMessage],
    event_name: str,
) -> list[dict[str, TCData]]:
    payloads: list[dict[str, TCData]] = []
    for message in messages:
        data = message.value.data
        if isinstance(data, dict):
            event_value = data.get("event")
            if event_value == event_name:
                payloads.append(data)
    return payloads


def _event_counts(messages: list[EmittedLogMessage]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for message in messages:
        data = message.value.data
        if isinstance(data, dict):
            event_name = data.get("event")
            if isinstance(event_name, str):
                counts[event_name] += 1
    return counts


def _transition_payloads(messages: list[EmittedLogMessage]) -> list[dict[str, TCData]]:
    return _event_payloads(messages, "node_role_transition")


def _transition_sequence(
    messages: list[EmittedLogMessage],
) -> list[tuple[int, int, str, str, int]]:
    sequence: list[tuple[int, int, str, str, int]] = []
    for payload in _transition_payloads(messages):
        sequence.append(
            (
                coerce_int(payload["tick"], "tick"),
                coerce_int(payload["node_id"], "node_id"),
                str(payload["from_role"]),
                str(payload["to_role"]),
                coerce_int(payload["term"], "term"),
            )
        )
    return sequence


@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_example_config_runs_emit_expected_lifecycle_events(
    config_filename: str,
) -> None:
    simulation, messages = _run_config(config_filename)

    counts = _event_counts(messages)
    started = _event_payloads(messages, "simulation_started")
    finished = _event_payloads(messages, "simulation_finished")

    assert counts["simulation_started"] == 1
    assert counts["simulation_finished"] == 1
    assert counts["node_initialized"] == simulation.num_nodes

    expected_tick_events = (
        int(coerce_float(simulation.duration_s, "duration_s") * 1000)
        // simulation.tick_ms
    )
    assert counts["simulation_tick"] == expected_tick_events

    assert coerce_int(started[0]["tick"], "started.tick") == 0
    assert coerce_int(finished[0]["tick"], "finished.tick") == int(
        coerce_float(simulation.duration_s, "duration_s") * 1000
    )


def test_system_crash_scenario_drops_messages_only_during_crash_window() -> None:
    _, messages = _run_config("system_crash.json")

    dropped = _event_payloads(messages, "message_dropped")
    assert dropped, "Timed crash filter should drop some messages"
    assert all(250 <= coerce_int(payload["tick"], "tick") < 350 for payload in dropped)


def test_leader_crash_timed_scenario_causes_re_election_and_latency() -> None:
    _, messages = _run_config("leader_crash_timed.json")

    delayed = _event_payloads(messages, "message_delayed")
    dropped = _event_payloads(messages, "message_dropped")
    leaders = _event_payloads(messages, "leader_elected")

    assert delayed, "Latency filter should delay messages"
    assert dropped, "Leader crash window should drop leader traffic"
    assert len(leaders) >= 2, "Leader isolation should force at least one re-election"
    assert all(250 <= coerce_int(payload["tick"], "tick") < 550 for payload in dropped)


def test_detailed_filters_scenario_crashes_receiver_2_and_applies_latency() -> None:
    _, messages = _run_config("detailed_filters.json")

    dropped = _event_payloads(messages, "message_dropped")
    delayed = _event_payloads(messages, "message_delayed")

    assert dropped, "Receiver crash filter should drop messages"
    assert delayed, "Latency filters should delay messages"
    assert all(coerce_int(payload["receiver"], "receiver") == 2 for payload in dropped)


@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_received_messages_are_preceded_by_delivery_event(config_filename: str) -> None:
    _, messages = _run_config(config_filename)

    delivered_msg_ids_seen: set[int] = set()
    received_count = 0

    for emitted in messages:
        data = emitted.value.data
        if not isinstance(data, dict):
            continue

        event_value = data.get("event")
        if not isinstance(event_value, str):
            continue

        if event_value == "message_delivered":
            delivered_msg_ids_seen.add(coerce_int(data["msg_id"], "msg_id"))

        if event_value == "message_received":
            received_count += 1
            msg_id = coerce_int(data["msg_id"], "msg_id")
            assert msg_id in delivered_msg_ids_seen, (
                f"message_received with msg_id={msg_id} appeared before a matching "
                "message_delivered event"
            )

    assert received_count > 0, "Expected to observe at least one received message"


@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_role_transition_sequences_are_valid_per_node(config_filename: str) -> None:
    _, messages = _run_config(config_filename)

    transitions = _transition_payloads(messages)
    allowed_transitions = {
        ("follower", "candidate"),
        ("candidate", "leader"),
        ("candidate", "follower"),
        ("candidate", "candidate"),
        ("leader", "follower"),
    }

    transitions_by_node: dict[int, list[dict[str, TCData]]] = {}
    for payload in transitions:
        node_id = coerce_int(payload["node_id"], "node_id")
        transitions_by_node.setdefault(node_id, []).append(payload)

    assert transitions_by_node, "Expected at least one role transition"

    for node_id, node_transitions in transitions_by_node.items():
        first_from_role = str(node_transitions[0]["from_role"])
        assert first_from_role == "follower", (
            f"Node {node_id} must start transitions from follower, got {first_from_role}"
        )

        previous_to_role: str | None = None
        for payload in node_transitions:
            from_role = str(payload["from_role"])
            to_role = str(payload["to_role"])

            assert (from_role, to_role) in allowed_transitions, (
                f"Invalid transition for node {node_id}: {from_role}->{to_role}"
            )

            if previous_to_role is not None:
                assert from_role == previous_to_role, (
                    f"Broken transition chain for node {node_id}: "
                    f"expected from_role={previous_to_role}, got {from_role}"
                )
            previous_to_role = to_role


@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_leader_elected_has_matching_transition_to_leader(config_filename: str) -> None:
    _, messages = _run_config(config_filename)

    transitions_to_leader: set[tuple[int, int, int]] = set()
    for payload in _transition_payloads(messages):
        to_role = str(payload["to_role"])
        if to_role == "leader":
            transitions_to_leader.add(
                (
                    coerce_int(payload["node_id"], "node_id"),
                    coerce_int(payload["term"], "term"),
                    coerce_int(payload["tick"], "tick"),
                )
            )

    leader_elected_payloads = _event_payloads(messages, "leader_elected")
    assert leader_elected_payloads, "Expected at least one leader_elected event"

    for payload in leader_elected_payloads:
        key = (
            coerce_int(payload["node_id"], "node_id"),
            coerce_int(payload["term"], "term"),
            coerce_int(payload["tick"], "tick"),
        )
        assert key in transitions_to_leader, (
            "leader_elected must correspond to a matching node_role_transition to leader "
            f"for node={key[0]} term={key[1]} tick={key[2]}"
        )


def test_system_crash_role_transition_story_matches_cli_behavior() -> None:
    _, messages = _run_config("system_crash.json")

    assert _transition_sequence(messages) == [
        # Node 1 times out waiting for leader heartbeats and starts an election.
        (159, 1, "follower", "candidate", 1),
        # Node 1 receives enough votes in term 1 and wins leadership.
        (160, 1, "candidate", "leader", 1),
    ]


def test_leader_crash_timed_role_transition_story_matches_cli_behavior() -> None:
    _, messages = _run_config("leader_crash_timed.json")

    assert _transition_sequence(messages) == [
        # Node 1 reaches election timeout first and becomes a candidate for term 1.
        (208, 1, "follower", "candidate", 1),
        # Before leader heartbeats stabilize the cluster, node 0 also times out and campaigns.
        (232, 0, "follower", "candidate", 1),
        # Node 1 secures quorum and becomes leader in term 1.
        (232, 1, "candidate", "leader", 1),
        # During the timed leader-message crash window, node 0 times out again and restarts election in term 2.
        (420, 0, "candidate", "candidate", 2),
        # Node 0 eventually gathers votes and becomes the new leader in term 2.
        (459, 0, "candidate", "leader", 2),
        # Node 1 observes higher-term authority/traffic and steps down from leader to follower.
        (513, 1, "leader", "follower", 2),
    ]


def test_detailed_filters_role_transition_story_matches_cli_behavior() -> None:
    _, messages = _run_config("detailed_filters.json")

    assert _transition_sequence(messages) == [
        # Node 1 times out first and starts election for term 1.
        (149, 1, "follower", "candidate", 1),
        # Node 1 wins term 1 and becomes leader.
        (151, 1, "candidate", "leader", 1),
        # Because receiver 2 is crashed by filter, node 2 does not receive stable heartbeats and starts campaigning.
        (179, 2, "follower", "candidate", 1),
        # Node 2 times out again without quorum and restarts election in a higher term.
        (377, 2, "candidate", "candidate", 2),
        # Node 1 sees the higher term and steps down to follower.
        (377, 1, "leader", "follower", 2),
    ]
