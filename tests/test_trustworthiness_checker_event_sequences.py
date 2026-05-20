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
      - follower  (FR-4): only exits to candidate
      - candidate (CR-1): always operates in term > 0
      - leader    (LR-4): only exits to follower
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

    Tracks node 0's role via an auxiliary variable updated on
    node_role_transition events, then checks every message_generated event
    from node 0 against the allowed message types for that role:

      - follower  (FR-5): never RequestVote or AppendEntries
      - candidate (CR-3): never AppendEntries
      - leader    (LR-2): never RequestVote

    The spec uses is_defined() to guard stream-specific access and
    default() with past-indexing to carry the role forward between
    node_role_transition events.
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
