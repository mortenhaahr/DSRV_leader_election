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

pytestmark = pytest.mark.simulations

RANDOM_SEEDS = [1, 7, 42, 333, 1356, 986751, 798942, 233354, 396725, 845406]


def _run_config(
    config_filename: str,
    *,
    seed: int | None = None,
) -> tuple[Simulation, list[EmittedLogMessage]]:
    config_path = Path("tests/fixtures/configs") / config_filename

    logger = MockEventLogger(topic_mapping=TopicMapping({}))
    emitter = RaftEventEmitter(event_logger=logger)

    simulation = load_simulation_from_file(
        str(config_path),
        event_emitter=emitter,
        override_seed=seed,
    )
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


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
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
    seed: int,
) -> None:
    simulation, messages = _run_config(config_filename, seed=seed)

    counts = _event_counts(messages)
    started = _event_payloads(messages, "simulation_started")
    finished = _event_payloads(messages, "simulation_finished")

    assert counts["simulation_started"] == 1, f"seed={seed} config={config_filename}"
    assert counts["simulation_finished"] == 1, f"seed={seed} config={config_filename}"
    assert counts["node_initialized"] == simulation.num_nodes, (
        f"seed={seed} config={config_filename} expected node_initialized={simulation.num_nodes} "
        f"got {counts['node_initialized']}"
    )

    expected_tick_events = (
        int(coerce_float(simulation.duration_s, "duration_s") * 1000)
        // simulation.tick_ms
    )
    assert counts["simulation_tick"] == expected_tick_events, (
        f"seed={seed} config={config_filename} expected simulation_tick={expected_tick_events} "
        f"got {counts['simulation_tick']}"
    )

    assert coerce_int(started[0]["tick"], "started.tick") == 0, (
        f"seed={seed} config={config_filename} expected simulation_started tick=0"
    )
    assert coerce_int(finished[0]["tick"], "finished.tick") == int(
        coerce_float(simulation.duration_s, "duration_s") * 1000
    ), f"seed={seed} config={config_filename} unexpected simulation_finished tick"


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
def test_system_crash_scenario_drops_messages_only_during_crash_window(
    seed: int,
) -> None:
    _, messages = _run_config("system_crash.json", seed=seed)

    dropped = _event_payloads(messages, "message_dropped")
    assert dropped, f"seed={seed} expected timed crash filter to drop messages"
    assert all(
        250 <= coerce_int(payload["tick"], "tick") < 350 for payload in dropped
    ), f"seed={seed} expected message_dropped ticks within [250,350)"


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
def test_leader_crash_timed_scenario_causes_re_election_and_latency(seed: int) -> None:
    _, messages = _run_config("leader_crash_timed.json", seed=seed)

    delayed = _event_payloads(messages, "message_delayed")
    dropped = _event_payloads(messages, "message_dropped")
    leaders = _event_payloads(messages, "leader_elected")
    transitions = _transition_sequence(messages)

    assert delayed, f"seed={seed} expected latency filter to delay messages"
    assert dropped, f"seed={seed} expected leader crash window to drop leader traffic"
    assert len(leaders) >= 1, (
        f"seed={seed} expected at least one leader_elected event, got {len(leaders)}"
    )
    assert all(
        250 <= coerce_int(payload["tick"], "tick") < 550 for payload in dropped
    ), f"seed={seed} expected message_dropped ticks within [250,550)"

    has_multiple_leaders = len(leaders) >= 2
    has_forced_stepdown = any(
        from_role == "leader" and to_role == "follower"
        for _, _, from_role, to_role, _ in transitions
    )
    assert has_multiple_leaders or has_forced_stepdown, (
        f"seed={seed} expected either re-election (>=2 leaders) or forced leader stepdown"
    )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
def test_detailed_filters_scenario_crashes_receiver_2_and_applies_latency(
    seed: int,
) -> None:
    _, messages = _run_config("detailed_filters.json", seed=seed)

    dropped = _event_payloads(messages, "message_dropped")
    delayed = _event_payloads(messages, "message_delayed")

    assert dropped, f"seed={seed} expected receiver crash filter to drop messages"
    assert all(
        coerce_int(payload["receiver"], "receiver") == 2 for payload in dropped
    ), f"seed={seed} expected all dropped messages to target receiver=2"

    if delayed:
        return

    # For some seeds, no messages fall into the configured latency filter windows.
    scheduled = _event_payloads(messages, "message_scheduled")
    has_latency_eligible_non_crashed_scheduled_message = any(
        (
            (
                50 <= coerce_int(payload["tick"], "tick") < 150
                and coerce_int(payload["sender"], "sender") == 1
            )
            or (
                200 <= coerce_int(payload["tick"], "tick") < 250
                and (
                    coerce_int(payload["sender"], "sender") == 3
                    or coerce_int(payload["receiver"], "receiver") == 3
                )
            )
        )
        and coerce_int(payload["receiver"], "receiver") != 2
        for payload in scheduled
    )
    assert not has_latency_eligible_non_crashed_scheduled_message, (
        f"seed={seed} saw no message_delayed events despite scheduling latency-eligible "
        "messages that were not dropped by receiver-2 crash"
    )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_received_messages_are_preceded_by_delivery_event(
    config_filename: str,
    seed: int,
) -> None:
    _, messages = _run_config(config_filename, seed=seed)

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
                f"seed={seed} config={config_filename} message_received with msg_id={msg_id} "
                "appeared before a matching message_delivered event"
            )

    assert received_count > 0, (
        f"seed={seed} config={config_filename} expected at least one received message"
    )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_role_transition_sequences_are_valid_per_node(
    config_filename: str,
    seed: int,
) -> None:
    _, messages = _run_config(config_filename, seed=seed)

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

    assert transitions_by_node, (
        f"seed={seed} config={config_filename} expected at least one role transition"
    )

    for node_id, node_transitions in transitions_by_node.items():
        first_from_role = str(node_transitions[0]["from_role"])
        assert first_from_role == "follower", (
            f"seed={seed} config={config_filename} node={node_id} must start transitions "
            f"from follower, got {first_from_role}"
        )

        previous_to_role: str | None = None
        for payload in node_transitions:
            from_role = str(payload["from_role"])
            to_role = str(payload["to_role"])

            assert (from_role, to_role) in allowed_transitions, (
                f"seed={seed} config={config_filename} invalid transition for node {node_id}: "
                f"{from_role}->{to_role}"
            )

            if previous_to_role is not None:
                assert from_role == previous_to_role, (
                    f"seed={seed} config={config_filename} broken transition chain for "
                    f"node {node_id}: expected from_role={previous_to_role}, got {from_role}"
                )
            previous_to_role = to_role


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_leader_elected_has_matching_transition_to_leader(
    config_filename: str,
    seed: int,
) -> None:
    _, messages = _run_config(config_filename, seed=seed)

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
    assert leader_elected_payloads, (
        f"seed={seed} config={config_filename} expected at least one leader_elected event"
    )

    for payload in leader_elected_payloads:
        key = (
            coerce_int(payload["node_id"], "node_id"),
            coerce_int(payload["term"], "term"),
            coerce_int(payload["tick"], "tick"),
        )
        assert key in transitions_to_leader, (
            f"seed={seed} config={config_filename} leader_elected must correspond to a "
            "matching node_role_transition to leader "
            f"for node={key[0]} term={key[1]} tick={key[2]}"
        )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
def test_system_crash_role_transitions(seed: int) -> None:
    _, messages = _run_config("system_crash.json", seed=seed)

    sequence = _transition_sequence(messages)
    assert sequence, f"seed={seed} expected at least one role transition"

    # Elections should start via follower->candidate and eventually produce a leader.
    assert any(
        from_role == "follower" and to_role == "candidate"
        for _, _, from_role, to_role, _ in sequence
    ), f"seed={seed} expected at least one follower->candidate transition"
    assert any(to_role == "leader" for _, _, _, to_role, _ in sequence), (
        f"seed={seed} expected at least one transition to leader"
    )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
def test_leader_crash_timed_role_transitions(seed: int) -> None:
    _, messages = _run_config("leader_crash_timed.json", seed=seed)

    sequence = _transition_sequence(messages)

    leader_transitions = [
        step for step in sequence if step[2] == "candidate" and step[3] == "leader"
    ]
    assert len(leader_transitions) >= 1, (
        f"seed={seed} expected at least one candidate->leader transition, "
        f"got {len(leader_transitions)}"
    )

    has_multiple_leaders = len(leader_transitions) >= 2
    has_forced_stepdown = any(
        from_role == "leader" and to_role == "follower"
        for _, _, from_role, to_role, _ in sequence
    )
    assert has_multiple_leaders or has_forced_stepdown, (
        f"seed={seed} expected either >=2 leader transitions or a leader->follower stepdown"
    )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
def test_detailed_filters_role_transitions(seed: int) -> None:
    _, messages = _run_config("detailed_filters.json", seed=seed)

    sequence = _transition_sequence(messages)

    # Receiver-2 crash and timed latency should lead to repeated candidacy attempts.
    assert any(
        from_role == "candidate" and to_role == "candidate"
        for _, _, from_role, to_role, _ in sequence
    ), f"seed={seed} expected at least one candidate->candidate transition"

    # Depending on timing, either a leader steps down, or a new leader still emerges later.
    has_leader_stepdown = any(
        from_role == "leader" and to_role == "follower"
        for _, _, from_role, to_role, _ in sequence
    )
    has_leader_election = any(
        from_role == "candidate" and to_role == "leader"
        for _, _, from_role, to_role, _ in sequence
    )
    assert has_leader_stepdown or has_leader_election, (
        f"seed={seed} expected leader stepdown or at least one leader election"
    )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_lifecycle_invariants(
    config_filename: str,
    seed: int,
) -> None:
    simulation, messages = _run_config(config_filename, seed=seed)

    counts = _event_counts(messages)
    started = _event_payloads(messages, "simulation_started")
    finished = _event_payloads(messages, "simulation_finished")

    assert counts["simulation_started"] == 1, (
        f"generated_seed={seed} config={config_filename}"
    )
    assert counts["simulation_finished"] == 1, (
        f"generated_seed={seed} config={config_filename}"
    )
    assert counts["node_initialized"] == simulation.num_nodes, (
        f"generated_seed={seed} config={config_filename} expected node_initialized={simulation.num_nodes} "
        f"got {counts['node_initialized']}"
    )
    assert coerce_int(started[0]["tick"], "started.tick") == 0, (
        f"generated_seed={seed} config={config_filename} expected start tick 0"
    )
    assert coerce_int(finished[0]["tick"], "finished.tick") == int(
        coerce_float(simulation.duration_s, "duration_s") * 1000
    ), f"generated_seed={seed} config={config_filename} unexpected finish tick"


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_transition_consistency(
    config_filename: str,
    seed: int,
) -> None:
    _, messages = _run_config(config_filename, seed=seed)

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

    assert transitions_by_node, (
        f"generated_seed={seed} config={config_filename} expected at least one role transition"
    )

    for node_id, node_transitions in transitions_by_node.items():
        previous_to_role: str | None = None
        for payload in node_transitions:
            from_role = str(payload["from_role"])
            to_role = str(payload["to_role"])
            assert (from_role, to_role) in allowed_transitions, (
                f"generated_seed={seed} config={config_filename} invalid transition "
                f"for node {node_id}: {from_role}->{to_role}"
            )
            if previous_to_role is not None:
                assert from_role == previous_to_role, (
                    f"generated_seed={seed} config={config_filename} broken chain for node {node_id}: "
                    f"expected from_role={previous_to_role}, got {from_role}"
                )
            previous_to_role = to_role


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_transition_ticks_are_non_decreasing_per_node(
    config_filename: str,
    seed: int,
) -> None:
    _, messages = _run_config(config_filename, seed=seed)

    transitions_by_node: dict[int, list[int]] = {}
    for payload in _transition_payloads(messages):
        node_id = coerce_int(payload["node_id"], "node_id")
        transitions_by_node.setdefault(node_id, []).append(
            coerce_int(payload["tick"], "tick")
        )

    for node_id, ticks in transitions_by_node.items():
        assert ticks == sorted(ticks), (
            f"seed={seed} config={config_filename} node={node_id} transition ticks "
            f"must be non-decreasing, got {ticks}"
        )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_transition_terms_are_non_decreasing_per_node(
    config_filename: str,
    seed: int,
) -> None:
    _, messages = _run_config(config_filename, seed=seed)

    transitions_by_node: dict[int, list[int]] = {}
    for payload in _transition_payloads(messages):
        node_id = coerce_int(payload["node_id"], "node_id")
        transitions_by_node.setdefault(node_id, []).append(
            coerce_int(payload["term"], "term")
        )

    for node_id, terms in transitions_by_node.items():
        assert terms == sorted(terms), (
            f"seed={seed} config={config_filename} node={node_id} transition terms "
            f"must be non-decreasing, got {terms}"
        )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
def test_candidate_to_leader_transition_has_matching_leader_elected_event_same_tick(
    config_filename: str,
    seed: int,
) -> None:
    _, messages = _run_config(config_filename, seed=seed)

    transition_to_leader_keys = {
        (
            coerce_int(payload["node_id"], "node_id"),
            coerce_int(payload["term"], "term"),
            coerce_int(payload["tick"], "tick"),
        )
        for payload in _transition_payloads(messages)
        if str(payload["from_role"]) == "candidate"
        and str(payload["to_role"]) == "leader"
    }

    leader_elected_keys = {
        (
            coerce_int(payload["node_id"], "node_id"),
            coerce_int(payload["term"], "term"),
            coerce_int(payload["tick"], "tick"),
        )
        for payload in _event_payloads(messages, "leader_elected")
    }

    assert transition_to_leader_keys, (
        f"seed={seed} config={config_filename} expected at least one candidate->leader transition"
    )
    assert transition_to_leader_keys.issubset(leader_elected_keys), (
        f"seed={seed} config={config_filename} expected leader_elected event for each "
        "candidate->leader transition"
    )
