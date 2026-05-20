# pyright: reportMissingTypeStubs=false

from __future__ import annotations

from collections.abc import Callable
from typing import cast

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

EVENT_WAIT_TIMEOUT_S = 6.0


def _collect_event_payloads(
    stream: MqttMessageStream,
    event_name: str,
    *,
    timeout_s: float,
) -> list[dict[str, object]]:
    decoded: list[dict[str, object]] = []
    for raw in stream.receive(timeout_s=timeout_s):
        value = decode_checker_payload(raw)
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
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container: object,
) -> None:
    broker, port = mqtt_broker
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

    with (
        mqtt_subscriber_factory("tc/out/simulation_started") as started_stream,
        mqtt_subscriber_factory("tc/out/simulation_finished") as finished_stream,
    ):
        simulation.run()

        started_values = _collect_event_payloads(
            started_stream, "simulation_started", timeout_s=EVENT_WAIT_TIMEOUT_S
        )
        finished_values = _collect_event_payloads(
            finished_stream, "simulation_finished", timeout_s=EVENT_WAIT_TIMEOUT_S
        )

    assert started_values, "Expected at least one simulation_started payload"
    assert finished_values, "Expected at least one simulation_finished payload"


def test_trustworthiness_checker_receives_leader_elected_events_via_mqtt(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
    trustworthiness_checker_container: object,
) -> None:
    broker, port = mqtt_broker
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

    with mqtt_subscriber_factory("tc/out/leader_elected") as leader_stream:
        simulation.run()

        decoded_payloads: list[dict[str, object]] = []
        for raw in leader_stream.receive(timeout_s=EVENT_WAIT_TIMEOUT_S):
            value = decode_checker_payload(raw)
            assert isinstance(value, dict)
            decoded_payloads.append(cast(dict[str, object], value))

    assert decoded_payloads, "Expected checker output for leader_elected events"
    assert all(payload.get("event") == "leader_elected" for payload in decoded_payloads)
