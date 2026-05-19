# pyright: reportMissingTypeStubs=false

from __future__ import annotations

from collections.abc import Callable

import pytest

from dsrv_leader_election.config_loader import load_simulation_from_file
from dsrv_leader_election.event_logger.mqtt_logger import MqttLogger
from dsrv_leader_election.event_logger.raft_event_emitter import RaftEventEmitter
from dsrv_leader_election.testing.mqtt_test_support import assert_eventually
from dsrv_leader_election.testing.trustworthiness_checker_test_support import (
    await_checker_ready,
    decode_checker_payload,
    load_example_topic_mapping,
)

pytestmark = [pytest.mark.mqtt, pytest.mark.end_to_end]

DEFAULT_SEED = 42
ASSERTION_TIMEOUT_S = 8.0
COMMON_CONFIGS = (
    "system_crash.json",
    "leader_crash_timed.json",
    "detailed_filters.json",
)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "config_filename" in metafunc.fixturenames:
        metafunc.parametrize("config_filename", COMMON_CONFIGS)


def _assert_all_true(payloads: list[str], *, allow_empty: bool = False) -> None:
    if allow_empty and not payloads:
        return
    assert payloads, "Expected at least one checker assertion output"
    values = [decode_checker_payload(payload) for payload in payloads]
    assert all(isinstance(value, bool) for value in values)
    assert all(value is True for value in values)


# def _assert_any_true(payloads: list[str], *, allow_empty: bool = False) -> None:
#     if allow_empty and not payloads:
#         return
#     assert payloads, "Expected at least one checker assertion output"
#     values = [decode_checker_payload(payload) for payload in payloads]
#     assert all(isinstance(value, bool) for value in values)
#     assert any(value is True for value in values)


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
    mqtt_subscriber_factory: Callable[[str], list[str]],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/lifecycle_assertions.dsrv",
        "/tc-fixtures/input_topics_lifecycle.json",
    )

    started_ok = mqtt_subscriber_factory("started_ok")
    finished_ok = mqtt_subscriber_factory("finished_ok")
    node_initialized_ok = mqtt_subscriber_factory("node_initialized_ok")

    broker, port = mqtt_broker
    _run_simulation(broker=broker, port=port, config_filename=config_filename)

    assert_eventually(lambda: bool(started_ok), timeout_s=ASSERTION_TIMEOUT_S)
    assert_eventually(lambda: bool(finished_ok), timeout_s=ASSERTION_TIMEOUT_S)
    assert_eventually(lambda: bool(node_initialized_ok), timeout_s=ASSERTION_TIMEOUT_S)

    _assert_all_true(started_ok)
    _assert_all_true(finished_ok)
    _assert_all_true(node_initialized_ok)


# event_sequences counterparts: test_lifecycle_invariants,
#                               test_example_config_runs_emit_expected_lifecycle_events
def test_tc_lifecycle_tick_assertions(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/lifecycle_tick_assertions.dsrv",
        "/tc-fixtures/input_topics_lifecycle_tick.json",
    )

    started_tick_zero = mqtt_subscriber_factory("started_tick_zero")
    finished_tick_non_negative = mqtt_subscriber_factory("finished_tick_non_negative")

    broker, port = mqtt_broker
    _run_simulation(broker=broker, port=port, config_filename=config_filename)

    assert_eventually(lambda: bool(started_tick_zero), timeout_s=ASSERTION_TIMEOUT_S)
    assert_eventually(
        lambda: bool(finished_tick_non_negative), timeout_s=ASSERTION_TIMEOUT_S
    )

    _assert_all_true(started_tick_zero)
    _assert_all_true(finished_tick_non_negative)


# event_sequences counterpart: test_example_config_runs_emit_expected_lifecycle_events
def test_tc_simulation_tick_assertions(
    config_filename: str,
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> None:
    _ = _start_checker(
        trustworthiness_checker_container_factory,
        "/tc-fixtures/simulation_tick_assertions.dsrv",
        "/tc-fixtures/input_topics_simulation_tick.json",
    )

    simulation_tick_ok = mqtt_subscriber_factory("simulation_tick_ok")

    broker, port = mqtt_broker
    _run_simulation(broker=broker, port=port, config_filename=config_filename)

    assert_eventually(lambda: bool(simulation_tick_ok), timeout_s=ASSERTION_TIMEOUT_S)
    _assert_all_true(simulation_tick_ok)
