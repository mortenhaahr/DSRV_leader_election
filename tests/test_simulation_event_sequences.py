from __future__ import annotations

import re
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

# Matches per-node var names such as "node_role_transition_node_0" or
# "message_generated_node_1".  Used to exclude the duplicated per-node
# emissions from helpers that should only see the generic event stream.
_PER_NODE_VAR_RE = re.compile(r"_node_\d+$")


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
        if _PER_NODE_VAR_RE.search(message.var):
            continue
        data = message.value.data
        if isinstance(data, dict):
            event_value = data.get("event")
            if event_value == event_name:
                payloads.append(data)
    return payloads


def _event_counts(messages: list[EmittedLogMessage]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for message in messages:
        if _PER_NODE_VAR_RE.search(message.var):
            continue
        data = message.value.data
        if isinstance(data, dict):
            event_name = data.get("event")
            if isinstance(event_name, str):
                counts[event_name] += 1
    return counts


def _transition_payloads(messages: list[EmittedLogMessage]) -> list[dict[str, TCData]]:
    return _event_payloads(messages, "node_role_transition")


def _all_payloads(
    messages: list[EmittedLogMessage],
) -> list[dict[str, TCData]]:
    """Return every event payload in emission order, preserving interleaving.

    Unlike the type-filtered helpers, this preserves the relative ordering of
    node_role_transition and message_generated events within the same tick,
    which is required to correctly correlate a node's current role with the
    messages it produces.
    """
    return [
        message.value.data
        for message in messages
        if isinstance(message.value.data, dict)
        and not _PER_NODE_VAR_RE.search(message.var)
    ]


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


def _message_event_sequence(
    messages: list[EmittedLogMessage],
) -> list[tuple[str, str, int, int, int, int]]:
    sequence: list[tuple[str, str, int, int, int, int]] = []
    for message in messages:
        if _PER_NODE_VAR_RE.search(message.var):
            continue
        data = message.value.data
        if not isinstance(data, dict):
            continue

        event_name = data.get("event")
        if event_name not in {
            "message_generated",
            "message_scheduled",
            "message_delivered",
            "message_received",
            "message_dropped",
            "message_delayed",
        }:
            continue

        sequence.append(
            (
                str(event_name),
                str(data.get("msg_type")),
                coerce_int(data.get("msg_id"), "msg_id"),
                coerce_int(data.get("sender"), "sender"),
                coerce_int(data.get("receiver"), "receiver"),
                coerce_int(data.get("tick"), "tick"),
            )
        )
    return sequence


# TC counterparts: test_tc_lifecycle_assertions, test_tc_lifecycle_tick_assertions,
#                  test_tc_simulation_tick_assertions
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


# No TC counterpart
def test_message_ids_are_isolated_between_runs() -> None:
    seed = 42
    _, messages_a = _run_config("system_crash.json", seed=seed)
    _, messages_b = _run_config("system_crash.json", seed=seed)

    sequence_a = _message_event_sequence(messages_a)
    sequence_b = _message_event_sequence(messages_b)

    assert sequence_a, "expected non-empty message sequence for first run"
    assert sequence_b, "expected non-empty message sequence for second run"
    assert sequence_a[0][2] == 1, "expected first run to start message ids at 1"
    assert sequence_b[0][2] == 1, "expected second run to start message ids at 1"


# No TC counterpart
def test_running_same_simulation_instance_twice_raises_runtime_error() -> None:
    simulation, _ = _run_config("system_crash.json", seed=42)

    with pytest.raises(RuntimeError, match="only be run once"):
        simulation.run()


# No TC counterpart
def test_system_crash_exact_message_event_prefix_for_seed_42() -> None:
    seed = 42
    _, messages = _run_config("system_crash.json", seed=seed)

    sequence = _message_event_sequence(messages)
    expected_prefix = [
        ("message_generated", "RequestVote", 1, 1, 0, 159),
        ("message_generated", "RequestVote", 2, 1, 2, 159),
        ("message_scheduled", "RequestVote", 1, 1, 0, 158),
        ("message_scheduled", "RequestVote", 2, 1, 2, 158),
        ("message_delivered", "RequestVote", 1, 1, 0, 159),
        ("message_received", "RequestVote", 1, 1, 0, 159),
        ("message_generated", "RequestVoteResponse", 3, 0, 1, 159),
        ("message_delivered", "RequestVote", 2, 1, 2, 159),
        ("message_received", "RequestVote", 2, 1, 2, 159),
        ("message_generated", "RequestVoteResponse", 4, 2, 1, 159),
        ("message_scheduled", "RequestVoteResponse", 3, 0, 1, 159),
        ("message_scheduled", "RequestVoteResponse", 4, 2, 1, 159),
        ("message_delivered", "RequestVoteResponse", 3, 0, 1, 160),
        ("message_received", "RequestVoteResponse", 3, 0, 1, 160),
        ("message_generated", "AppendEntries", 5, 1, 0, 160),
        ("message_generated", "AppendEntries", 6, 1, 2, 160),
        ("message_delivered", "RequestVoteResponse", 4, 2, 1, 160),
        ("message_received", "RequestVoteResponse", 4, 2, 1, 160),
    ]

    assert sequence[: len(expected_prefix)] == expected_prefix, (
        f"seed={seed} expected exact initial message event sequence prefix"
    )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
# TC counterpart: test_tc_system_crash_drop_window_assertions
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
# TC counterpart: test_tc_leader_crash_assertions
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
# TC counterpart: test_tc_detailed_filters_assertions
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
# TC counterpart: test_tc_delivery_payload_assertions
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
# TC counterpart: test_tc_leader_crash_assertions (transition_allowed output)
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
# TC counterpart: test_tc_leader_transition_assertions
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
# TC counterpart: test_tc_system_crash_role_transition_presence_assertions
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
# TC counterpart: test_tc_leader_crash_transition_presence_assertions
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
# TC counterpart: test_tc_detailed_filters_transition_pattern_assertions
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
# TC counterparts: test_tc_lifecycle_assertions, test_tc_lifecycle_tick_assertions
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
# TC counterpart: test_tc_leader_crash_assertions (transition_allowed output)
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
# No TC counterpart
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
# No TC counterpart
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
# TC counterpart: test_tc_leader_transition_assertions
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


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
@pytest.mark.parametrize(
    "config_filename",
    [
        "system_crash.json",
        "leader_crash_timed.json",
        "detailed_filters.json",
    ],
)
# TC counterpart: test_tc_role_specific_behavior
def test_role_specific_behavior_per_role(
    config_filename: str,
    seed: int,
) -> None:
    """Check one key responsibility per role, using from_role as the discriminator.

    - Follower:   only valid exit is to become a candidate.
    - Candidate:  always operates in a strictly positive term.
    - Leader:     only valid exit is to step down to follower.
    """
    _, messages = _run_config(config_filename, seed=seed)

    transitions = _transition_payloads(messages)
    assert transitions, (
        f"seed={seed} config={config_filename} expected at least one role transition"
    )

    for payload in transitions:
        from_role = str(payload["from_role"])
        to_role = str(payload["to_role"])
        term = coerce_int(payload["term"], "term")
        node_id = coerce_int(payload["node_id"], "node_id")
        tick = coerce_int(payload["tick"], "tick")

        if from_role == "follower":
            # follower: only exit is to become a candidate (election timeout)
            assert to_role == "candidate", (
                f"seed={seed} config={config_filename} node={node_id} tick={tick}: "
                f"follower must exit to candidate, got {to_role!r}"
            )
        elif from_role == "candidate":
            # candidate: always operates in a strictly positive term
            assert term > 0, (
                f"seed={seed} config={config_filename} node={node_id} tick={tick}: "
                f"candidate must have term > 0, got term={term}"
            )
        elif from_role == "leader":
            # leader: only exit is to step down to follower
            assert to_role == "follower", (
                f"seed={seed} config={config_filename} node={node_id} tick={tick}: "
                f"leader must exit to follower, got {to_role!r}"
            )
        else:
            pytest.fail(
                f"seed={seed} config={config_filename} node={node_id} tick={tick}: unknown from_role {from_role!r}"
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
# TC counterpart: test_tc_node_specific_behavior
def test_node_0_message_types_per_role(
    config_filename: str,
    seed: int,
) -> None:
    """Check that node 0 only generates messages appropriate to its current role.

    Events are walked in emission order so that every node_role_transition for
    node 0 is processed before the message_generated events that follow it
    within the same tick, giving an accurate picture of the role at the time
    each message was produced.

    - Follower:   only sends RequestVoteResponse / AppendEntriesResponse;
                  never initiates elections (RequestVote) or heartbeats (AppendEntries).
    - Candidate:  sends RequestVote and response messages; never sends AppendEntries.
    - Leader:     sends AppendEntries and response messages; never sends RequestVote.
    """
    _, messages = _run_config(config_filename, seed=seed)

    node_0_role = "follower"  # all nodes start as followers
    node_0_messages_seen = False

    for payload in _all_payloads(messages):
        event = str(payload.get("event", ""))
        raw_node_id = payload.get("node_id")
        if raw_node_id is None:
            continue
        node_id = coerce_int(raw_node_id, "node_id")

        if event == "node_role_transition" and node_id == 0:
            node_0_role = str(payload["to_role"])

        elif event == "message_generated" and node_id == 0:
            node_0_messages_seen = True
            msg_type = str(payload.get("msg_type", ""))
            tick = coerce_int(payload.get("tick"), "tick")

            if node_0_role == "follower":
                # follower: never initiates an election or heartbeat
                assert msg_type not in {"RequestVote", "AppendEntries"}, (
                    f"seed={seed} config={config_filename} tick={tick}: follower node 0 generated {msg_type!r}"
                )
            elif node_0_role == "candidate":
                # candidate: solicits votes but never sends heartbeats
                assert msg_type != "AppendEntries", (
                    f"seed={seed} config={config_filename} tick={tick}: candidate node 0 generated {msg_type!r}"
                )
            elif node_0_role == "leader":
                # leader: sends heartbeats but never solicits votes
                assert msg_type != "RequestVote", (
                    f"seed={seed} config={config_filename} tick={tick}: leader node 0 generated {msg_type!r}"
                )
            else:
                pytest.fail(
                    f"seed={seed} config={config_filename} tick={tick}: node 0 in unknown role {node_0_role!r}"
                )

    assert node_0_messages_seen, (
        f"seed={seed} config={config_filename}: expected at least one message_generated from node 0"
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
# TC counterpart: test_tc_node_0_role_history
def test_node_0_role_history_matches_transition_from_role(
    config_filename: str,
    seed: int,
) -> None:
    """Check that from_role in each node 0 transition matches the to_role of the preceding one.

    Events are read from the per-node stream (message.var ==
    'node_role_transition_node_0') so the list is already filtered to node 0
    and in emission order — exactly what the DSRV spec sees.

    Two invariants are verified:
    - First transition: from_role must be 'follower' (every node starts there).
    - Subsequent transitions: from_role must equal the to_role reported by the
      immediately preceding transition event.
    """
    _, messages = _run_config(config_filename, seed=seed)

    transitions = [
        message.value.data
        for message in messages
        if message.var == "node_role_transition_node_0"
        and isinstance(message.value.data, dict)
    ]

    if not transitions:
        # Node 0 may legitimately remain a follower for the whole simulation
        # (e.g. another node wins the election before node 0 times out).
        # The invariant is vacuously satisfied with no transitions to check.
        return

    # First transition must originate from the initial follower role.
    first_from = str(transitions[0]["from_role"])
    first_tick = coerce_int(transitions[0]["tick"], "tick")
    assert first_from == "follower", (
        f"seed={seed} config={config_filename} tick={first_tick}: "
        f"first node 0 transition from_role expected 'follower', got {first_from!r}"
    )

    # Every subsequent transition: from_role must equal the previous to_role.
    for prev, curr in zip(transitions, transitions[1:]):
        prev_to = str(prev["to_role"])
        curr_from = str(curr["from_role"])
        curr_tick = coerce_int(curr["tick"], "tick")
        assert curr_from == prev_to, (
            f"seed={seed} config={config_filename} tick={curr_tick}: "
            f"node 0 transition from_role {curr_from!r} != previous to_role {prev_to!r}"
        )

