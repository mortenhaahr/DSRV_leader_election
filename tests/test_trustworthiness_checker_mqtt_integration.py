# pyright: reportMissingTypeStubs=false

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pytest

from dsrv_leader_election.config_loader import load_simulation_from_file
from dsrv_leader_election.event_logger.mqtt_logger import MqttLogger
from dsrv_leader_election.event_logger.raft_event_emitter import RaftEventEmitter
from dsrv_leader_election.testing.mqtt_test_support import assert_eventually
from dsrv_leader_election.testing.trustworthiness_checker_test_support import (
    await_checker_ready,
    decode_checker_payload,
    load_example_topic_mapping
)

pytestmark = [pytest.mark.mqtt, pytest.mark.end_to_end]

EVENT_WAIT_TIMEOUT_S = 6.0


def _decoded_event_payloads(
    payloads: list[str],
    event_name: str,
) -> list[dict[str, object]]:
    decoded: list[dict[str, object]] = []
    for payload in payloads:
        value = decode_checker_payload(payload)
        if isinstance(value, dict):
            value_dict = cast(dict[str, object], value)
            if value_dict.get("event") == event_name:
                decoded.append(value_dict)
    return decoded


@pytest.fixture
def trustworthiness_checker_container(
    trustworthiness_checker_container_factory: Callable[[str, str, str | None], object],
) -> object:
    return trustworthiness_checker_container_factory(
        "/tc-fixtures/raft_events_passthrough.dsrv",
        "/tc-fixtures/input_topics.json",
        "/tc-fixtures/output_topics.json",
    )


def test_trustworthiness_checker_receives_simulation_lifecycle_events_via_mqtt(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
    trustworthiness_checker_container: object,
) -> None:
    broker, port = mqtt_broker

    started_echo_payloads = mqtt_subscriber_factory("tc/out/simulation_started")
    finished_echo_payloads = mqtt_subscriber_factory("tc/out/simulation_finished")

    await_checker_ready(trustworthiness_checker_container)

    logger = MqttLogger(
        broker=broker,
        port=port,
        topic_mapping=load_example_topic_mapping(),
    )
    simulation = load_simulation_from_file(
        "tests/fixtures/configs/system_crash.json",
        event_emitter=RaftEventEmitter(logger),
    )

    simulation.run()

    assert_eventually(
        lambda: bool(started_echo_payloads),
        timeout_s=EVENT_WAIT_TIMEOUT_S,
        message="Expected checker output for simulation_started event",
    )
    assert_eventually(
        lambda: bool(finished_echo_payloads),
        timeout_s=EVENT_WAIT_TIMEOUT_S,
        message="Expected checker output for simulation_finished event",
    )

    started_values = _decoded_event_payloads(
        started_echo_payloads, "simulation_started"
    )
    finished_values = _decoded_event_payloads(
        finished_echo_payloads,
        "simulation_finished",
    )

    assert started_values, "Expected at least one simulation_started payload"
    assert finished_values, "Expected at least one simulation_finished payload"


def test_trustworthiness_checker_receives_leader_elected_events_via_mqtt(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
    trustworthiness_checker_container: object,
) -> None:
    broker, port = mqtt_broker

    leader_elected_echo_payloads = mqtt_subscriber_factory("tc/out/leader_elected")

    await_checker_ready(trustworthiness_checker_container)

    logger = MqttLogger(
        broker=broker,
        port=port,
        topic_mapping=load_example_topic_mapping(),
    )
    simulation = load_simulation_from_file(
        "tests/fixtures/configs/leader_crash_timed.json",
        event_emitter=RaftEventEmitter(logger),
    )

    simulation.run()

    assert_eventually(
        lambda: len(leader_elected_echo_payloads) >= 1,
        timeout_s=EVENT_WAIT_TIMEOUT_S,
        message="Expected checker output for leader_elected events",
    )

    decoded_payloads: list[dict[str, object]] = []
    for payload in leader_elected_echo_payloads:
        value = decode_checker_payload(payload)
        assert isinstance(value, dict)
        decoded_payloads.append(cast(dict[str, object], value))

    assert all(payload.get("event") == "leader_elected" for payload in decoded_payloads)
