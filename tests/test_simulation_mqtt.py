# pyright: reportMissingTypeStubs=false

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from dsrv_leader_election.config_loader import load_simulation_from_file
from dsrv_leader_election.event_logger.mqtt_logger import MqttLogger
from dsrv_leader_election.event_logger.raft_event_emitter import RaftEventEmitter
from dsrv_leader_election.event_logger.topic_mapping import TopicMapping
from dsrv_leader_election.testing.mqtt_test_support import MqttMessageStream

pytestmark = pytest.mark.mqtt


def _load_example_topic_mapping() -> TopicMapping:
    mapping_path = Path("tests/fixtures/event_topics/raft_event_topics.json")
    with open(mapping_path, "r", encoding="utf-8") as file_obj:
        return TopicMapping.from_json(file_obj.read())


# TC counterpart: test_tc_lifecycle_assertions
def test_simulation_publishes_lifecycle_events_to_mqtt_topics(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
) -> None:
    broker, port = mqtt_broker
    topic_mapping = _load_example_topic_mapping()

    started_topic = topic_mapping["simulation_started"]
    finished_topic = topic_mapping["simulation_finished"]

    logger = MqttLogger(broker=broker, port=port, topic_mapping=topic_mapping)
    simulation = load_simulation_from_file(
        "tests/fixtures/configs/system_crash.json",
        event_emitter=RaftEventEmitter(logger),
    )

    with (
        mqtt_subscriber_factory(started_topic) as started_stream,
        mqtt_subscriber_factory(finished_topic) as finished_stream,
    ):
        simulation.run()

        started = next(started_stream.receive(timeout_s=3.0), None)
        assert started is not None, (
            "Expected simulation_started payload on mapped MQTT topic"
        )
        assert json.loads(started)["event"] == "simulation_started"

        finished_all = list(finished_stream.receive(timeout_s=3.0))
        assert finished_all, "Expected simulation_finished payload on mapped MQTT topic"
        assert json.loads(finished_all[-1])["event"] == "simulation_finished"


# TC counterparts: test_tc_leader_crash_assertions, test_tc_leader_transition_assertions,
#                  test_tc_leader_crash_transition_presence_assertions
def test_simulation_publishes_re_election_events_in_leader_crash_scenario(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
) -> None:
    broker, port = mqtt_broker
    topic_mapping = _load_example_topic_mapping()

    leader_topic = topic_mapping["leader_elected"]
    transition_topic = topic_mapping["node_role_transition"]

    logger = MqttLogger(broker=broker, port=port, topic_mapping=topic_mapping)
    simulation = load_simulation_from_file(
        "tests/fixtures/configs/leader_crash_timed.json",
        event_emitter=RaftEventEmitter(logger),
    )

    with (
        mqtt_subscriber_factory(leader_topic) as leader_stream,
        mqtt_subscriber_factory(transition_topic) as transition_stream,
    ):
        simulation.run()

        leader_payloads = list(leader_stream.receive(timeout_s=3.0))
        transition_payloads = list(transition_stream.receive(timeout_s=3.0))

    assert len(leader_payloads) >= 2, (
        "Expected at least two leader_elected messages due to re-election"
    )
    assert len(transition_payloads) >= len(leader_payloads), (
        "Expected node_role_transition events for leader elections"
    )

    leader_events = cast(
        list[dict[str, object]], [json.loads(payload) for payload in leader_payloads]
    )
    assert all(event.get("event") == "leader_elected" for event in leader_events)


# No TC counterpart (tests MQTT infrastructure fallback, not simulation event content)
def test_simulation_uses_event_name_as_fallback_mqtt_topic(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
) -> None:
    broker, port = mqtt_broker

    logger = MqttLogger(broker=broker, port=port, topic_mapping=TopicMapping({}))
    simulation = load_simulation_from_file(
        "tests/fixtures/configs/system_crash.json",
        event_emitter=RaftEventEmitter(logger),
    )

    with mqtt_subscriber_factory("simulation_started") as started_stream:
        simulation.run()

        payload = next(started_stream.receive(timeout_s=3.0), None)

    assert payload is not None, (
        "Expected fallback publication on simulation_started topic"
    )
    assert json.loads(payload)["event"] == "simulation_started"
