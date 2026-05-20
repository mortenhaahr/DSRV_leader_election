# pyright: reportMissingTypeStubs=false

from __future__ import annotations

from collections.abc import Callable

import pytest

from dsrv_leader_election.config_loader import load_simulation_from_file
from dsrv_leader_election.event_logger.mqtt_logger import MqttLogger
from dsrv_leader_election.event_logger.raft_event_emitter import RaftEventEmitter
from dsrv_leader_election.testing.mqtt_test_support import MqttMessageStream
from dsrv_leader_election.testing.trustworthiness_checker_test_support import (
    await_checker_ready,
    decode_checker_payload,
    load_example_topic_mapping,
)

pytestmark = [pytest.mark.mqtt, pytest.mark.end_to_end]

DEFAULT_SEED = 42
ASSERTION_TIMEOUT_S = 15.0
COMMON_CONFIGS = (
    "system_crash.json",
    "leader_crash_timed.json",
    "detailed_filters.json",
)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "config_filename" in metafunc.fixturenames:
        metafunc.parametrize("config_filename", COMMON_CONFIGS)


# After the simulation finishes, the TC may still be forwarding its last outputs.
# 2 s of idle time is ample for the TC to flush remaining messages.
_IDLE_DRAIN_S = 2.0


def _assert_all_true(
    stream: MqttMessageStream,
    *,
    timeout_s: float,
    allow_empty: bool = False,
) -> None:
    received = False
    for payload in stream.receive(timeout_s=timeout_s, idle_timeout_s=_IDLE_DRAIN_S):
        received = True
        value = decode_checker_payload(payload)
        assert isinstance(value, bool), f"Expected bool, got {type(value).__name__}"
        assert value is True, "Expected True"
    if not allow_empty:
        assert received, "Expected at least one checker output"


# def _assert_any_true(
#     stream: MqttMessageStream,
#     *,
#     timeout_s: float,
#     allow_empty: bool = False,
# ) -> None:
#     seen_true = False
#     for payload in stream.receive(timeout_s=timeout_s, idle_timeout_s=_IDLE_DRAIN_S):
#         value = decode_checker_payload(payload)
#         if isinstance(value, bool) and value is True:
#             seen_true = True
#     if not allow_empty:
#         assert seen_true, "Expected at least one True output"


def _start_checker(
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
    spec_path_in_container: str,
    input_topics_path_in_container: str,
) -> object:
    checker = trustworthiness_checker_container_factory(
        spec_path_in_container,
        input_topics_path_in_container,
        None,
    )
    await_checker_ready(checker)
    return checker


def _run_simulation(
    *,
    broker: str,
    port: int,
    config_filename: str,
    seed: int = DEFAULT_SEED,
) -> None:
    # publish_qos=1 (the default) prevents dropped messages by ensuring the broker
    # ACKs each message before the next is sent, naturally pacing throughput.
    logger = MqttLogger(
        broker=broker,
        port=port,
        topic_mapping=load_example_topic_mapping(),
    )
    simulation = load_simulation_from_file(
        f"tests/fixtures/configs/{config_filename}",
        event_emitter=RaftEventEmitter(logger),
        override_seed=seed,
    )
    simulation.run()


# event_sequences counterparts: test_example_config_runs_emit_expected_lifecycle_events,
#                               test_lifecycle_invariants
def test_tc_lifecycle_assertions(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/lifecycle_assertions.dsrv",
        "/tc-fixtures/input_topics_lifecycle.json",
    )

    broker, port = mqtt_broker
    with (
        mqtt_subscriber_factory("started_ok") as started_ok,
        mqtt_subscriber_factory("finished_ok") as finished_ok,
        mqtt_subscriber_factory("node_initialized_ok") as node_initialized_ok,
    ):
        _run_simulation(broker=broker, port=port, config_filename=config_filename)

        _assert_all_true(started_ok, timeout_s=ASSERTION_TIMEOUT_S)
        _assert_all_true(finished_ok, timeout_s=ASSERTION_TIMEOUT_S)
        _assert_all_true(node_initialized_ok, timeout_s=ASSERTION_TIMEOUT_S)


# event_sequences counterparts: test_lifecycle_invariants,
#                               test_example_config_runs_emit_expected_lifecycle_events
def test_tc_lifecycle_tick_assertions(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/lifecycle_tick_assertions.dsrv",
        "/tc-fixtures/input_topics_lifecycle_tick.json",
    )

    broker, port = mqtt_broker
    with (
        mqtt_subscriber_factory("started_tick_zero") as started_tick_zero,
        mqtt_subscriber_factory(
            "finished_tick_non_negative"
        ) as finished_tick_non_negative,
    ):
        _run_simulation(broker=broker, port=port, config_filename=config_filename)

        _assert_all_true(started_tick_zero, timeout_s=ASSERTION_TIMEOUT_S)
        _assert_all_true(finished_tick_non_negative, timeout_s=ASSERTION_TIMEOUT_S)


# event_sequences counterpart: test_lifecycle_invariants
# Checks that simulation_tick values increment by 1 on every consecutive sample.
# Uses DSRV past-indexing: tick[1] is the previous tick value; the first sample
# always passes (no history yet), all subsequent samples check tick >= prev + 1.
def test_tc_tick_increasing(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/lifecycle_increasing.dsrv",
        "/tc-fixtures/lifecycle_increasing.json",
    )

    broker, port = mqtt_broker
    with mqtt_subscriber_factory("increasing") as stream:
        _run_simulation(broker=broker, port=port, config_filename=config_filename)
        _assert_all_true(stream, timeout_s=ASSERTION_TIMEOUT_S)


# event_sequences counterpart: test_example_config_runs_emit_expected_lifecycle_events
def test_tc_simulation_tick_assertions(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/simulation_tick_assertions.dsrv",
        "/tc-fixtures/input_topics_simulation_tick.json",
    )

    broker, port = mqtt_broker
    with mqtt_subscriber_factory("simulation_tick_ok") as simulation_tick_ok:
        _run_simulation(broker=broker, port=port, config_filename=config_filename)
        _assert_all_true(simulation_tick_ok, timeout_s=ASSERTION_TIMEOUT_S)


# event_sequences counterpart: test_role_specific_behavior_per_role
def test_tc_role_specific_behavior(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    """TC counterpart to test_role_specific_behavior_per_role.

    Runs the role_specific_behavior_assertions spec against live MQTT events and
    asserts that every emitted role_behavior_ok output is True.

    The spec checks one key responsibility per role using an if-else chain on
    from_role in node_role_transition events:
      - follower:   only exits to candidate
      - candidate:  always operates in term > 0
      - leader:     only exits to follower
    """
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/role_specific_behavior_assertions.dsrv",
        "/tc-fixtures/input_topics_role_specific_behavior.json",
    )

    broker, port = mqtt_broker
    with mqtt_subscriber_factory("role_behavior_ok") as role_behavior_ok:
        _run_simulation(broker=broker, port=port, config_filename=config_filename)
        _assert_all_true(role_behavior_ok, timeout_s=ASSERTION_TIMEOUT_S)


# event_sequences counterpart: test_node_0_message_types_per_role
def test_tc_node_specific_behavior(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    """TC counterpart to test_node_0_message_types_per_role.

    The simulation emits every node-specific event on both the shared generic
    topic and a per-node topic (e.g. raft/node/role_transition/0).  This test
    uses only the per-node topics for node 0, so the checker receives an
    already-filtered stream and needs no node_id guard.

    The spec tracks node 0's role via an auxiliary variable updated on
    node_role_transition_node_0 events, then checks every
    message_generated_node_0 event against the allowed message types:

      - follower:   never RequestVote or AppendEntries
      - candidate:  never AppendEntries
      - leader:     never RequestVote
    """
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/node_specific_behavior_assertions.dsrv",
        "/tc-fixtures/input_topics_node_specific_behavior.json",
    )

    broker, port = mqtt_broker
    with mqtt_subscriber_factory("role_behavior_ok") as role_behavior_ok:
        _run_simulation(broker=broker, port=port, config_filename=config_filename)
        _assert_all_true(role_behavior_ok, timeout_s=ASSERTION_TIMEOUT_S)


# event_sequences counterpart: test_node_0_role_history_matches_transition_from_role
def test_tc_node_0_role_history(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    """TC counterpart to test_node_0_role_history_matches_transition_from_role.

    Subscribes exclusively to the per-node topic raft/node/role_transition/0.
    For each arriving transition event the spec checks:

      - First event (no [1] predecessor): from_role == 'follower'.
      - All subsequent events: from_role == to_role of the previous event,
        verified using DSRV past-indexing (node_role_transition_node_0[1]).
    """
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/node_role_history_assertions.dsrv",
        "/tc-fixtures/input_topics_node_role_history.json",
    )

    broker, port = mqtt_broker
    with mqtt_subscriber_factory("role_history_ok") as role_history_ok:
        _run_simulation(broker=broker, port=port, config_filename=config_filename)
        # allow_empty=True: node 0 may stay a follower for the whole simulation
        # in some scenarios, producing no role_history_ok outputs at all.
        _assert_all_true(
            role_history_ok, timeout_s=ASSERTION_TIMEOUT_S, allow_empty=True
        )


# event_sequences counterpart: test_node_0_per_message_behavior
def test_tc_node_0_per_message_behavior(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    """TC counterpart to test_node_0_per_message_behavior.

    Uses node_per_message_behavior_assertions.dsrv.  Each message_generated
    event carries a 'role' field so the spec is single-stream and stateless:
    no past-indexing, no auxiliary variables.

      - follower_ok:  no RequestVote/AppendEntries; term > 0.
      - candidate_ok: no AppendEntries; term > 0.
      - leader_ok:    no RequestVote; term > 0.
      - node_0_ok:    conjunction of the three.
    """
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/node_per_message_behavior_assertions.dsrv",
        "/tc-fixtures/input_topics_node_specific_behavior.json",
    )

    broker, port = mqtt_broker
    with (
        mqtt_subscriber_factory("follower_ok") as follower_ok,
        mqtt_subscriber_factory("candidate_ok") as candidate_ok,
        mqtt_subscriber_factory("leader_ok") as leader_ok,
        mqtt_subscriber_factory("node_0_ok") as node_0_ok,
    ):
        _run_simulation(broker=broker, port=port, config_filename=config_filename)
        _assert_all_true(follower_ok, timeout_s=ASSERTION_TIMEOUT_S)
        _assert_all_true(candidate_ok, timeout_s=ASSERTION_TIMEOUT_S, allow_empty=True)
        _assert_all_true(leader_ok, timeout_s=ASSERTION_TIMEOUT_S, allow_empty=True)
        _assert_all_true(node_0_ok, timeout_s=ASSERTION_TIMEOUT_S)


# event_sequences counterpart: test_node_0_historic_behavior
def test_tc_node_0_historic_behavior(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    """TC counterpart to test_node_0_historic_behavior.

    Uses node_historic_behavior_assertions.dsrv.  [1] past-indexing compares
    each message to its predecessor on the per-node stream.  On the first
    message [1] is Deferred and no output is produced.

      - follower_term_ok:      consecutive follower messages have non-decreasing term.
      - candidate_broadcast_ok: consecutive same-election RequestVotes target
                                different receivers.
      - leader_broadcast_ok:   consecutive same-epoch AppendEntries target
                                different receivers.
      - node_0_ok:             conjunction of the three.

    allow_empty=True is used where the antecedent may never be satisfied
    (e.g. node 0 stays a follower the whole simulation, or sends only one
    message before the simulation ends).
    """
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/node_historic_behavior_assertions.dsrv",
        "/tc-fixtures/input_topics_node_specific_behavior.json",
    )

    broker, port = mqtt_broker
    with (
        mqtt_subscriber_factory("follower_term_ok") as follower_term_ok,
        mqtt_subscriber_factory("candidate_broadcast_ok") as candidate_broadcast_ok,
        mqtt_subscriber_factory("leader_broadcast_ok") as leader_broadcast_ok,
        mqtt_subscriber_factory("node_0_ok") as node_0_ok,
    ):
        _run_simulation(broker=broker, port=port, config_filename=config_filename)
        _assert_all_true(
            follower_term_ok, timeout_s=ASSERTION_TIMEOUT_S, allow_empty=True
        )
        _assert_all_true(
            candidate_broadcast_ok, timeout_s=ASSERTION_TIMEOUT_S, allow_empty=True
        )
        _assert_all_true(
            leader_broadcast_ok, timeout_s=ASSERTION_TIMEOUT_S, allow_empty=True
        )
        _assert_all_true(node_0_ok, timeout_s=ASSERTION_TIMEOUT_S, allow_empty=True)


def test_tc_map_get_on_deferred_now_works(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    """Verifies that Map.get on a past-indexed stream no longer crashes the TC.

    The spec (node_role_history_map_get_deferred.dsrv) uses:

        if is_defined(node_role_transition_node_0[1]) then
            Map.get(node_role_transition_node_0[1], "to_role")

    This previously crashed the TC at startup because stream_lift1 did not
    short-circuit on Deferred inputs before invoking Map.get's inner function.

    TC fix (commit 8430d5e):
      - stream_lift1/stream_lift2 now propagate Deferred before calling the
        inner function, so Map.get(Deferred, key) returns Deferred safely.
      - if_stm now selects only the matching branch's result, so Deferred from
        a non-selected branch no longer reaches the output.

    The spec is semantically identical to node_role_history_assertions.dsrv;
    this test exists solely to confirm the TC fix holds end-to-end.
    """
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/node_role_history_map_get_deferred.dsrv",
        "/tc-fixtures/input_topics_node_role_history.json",
    )

    broker, port = mqtt_broker
    with mqtt_subscriber_factory("role_history_ok") as role_history_ok:
        _run_simulation(broker=broker, port=port, config_filename=config_filename)
        _assert_all_true(
            role_history_ok, timeout_s=ASSERTION_TIMEOUT_S, allow_empty=True
        )
