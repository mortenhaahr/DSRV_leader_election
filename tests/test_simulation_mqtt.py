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
from dsrv_leader_election.testing.mqtt_test_support import assert_eventually

pytestmark = pytest.mark.mqtt


def _load_example_topic_mapping() -> TopicMapping:
    mapping_path = Path("tests/fixtures/event_topics/raft_event_topics.json")
    with open(mapping_path, "r", encoding="utf-8") as file_obj:
        return TopicMapping.from_json(file_obj.read())


def test_simulation_publishes_lifecycle_events_to_mqtt_topics(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
) -> None:
    broker, port = mqtt_broker
    topic_mapping = _load_example_topic_mapping()

    started_topic = topic_mapping["simulation_started"]
    finished_topic = topic_mapping["simulation_finished"]
    started_payloads = mqtt_subscriber_factory(started_topic)
    finished_payloads = mqtt_subscriber_factory(finished_topic)

    logger = MqttLogger(broker=broker, port=port, topic_mapping=topic_mapping)
    simulation = load_simulation_from_file(
        "tests/fixtures/configs/system_crash.json",
        event_emitter=RaftEventEmitter(logger),
    )

    simulation.run()

    assert_eventually(
        lambda: bool(started_payloads),
        message="Expected simulation_started payload on mapped MQTT topic",
    )
    assert_eventually(
        lambda: bool(finished_payloads),
        message="Expected simulation_finished payload on mapped MQTT topic",
    )

    assert json.loads(started_payloads[0])["event"] == "simulation_started"
    assert json.loads(finished_payloads[-1])["event"] == "simulation_finished"


def test_simulation_publishes_re_election_events_in_leader_crash_scenario(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
) -> None:
    broker, port = mqtt_broker
    topic_mapping = _load_example_topic_mapping()

    leader_topic = topic_mapping["leader_elected"]
    transition_topic = topic_mapping["node_role_transition"]
    leader_payloads = mqtt_subscriber_factory(leader_topic)
    transition_payloads = mqtt_subscriber_factory(transition_topic)

    logger = MqttLogger(broker=broker, port=port, topic_mapping=topic_mapping)
    simulation = load_simulation_from_file(
        "tests/fixtures/configs/leader_crash_timed.json",
        event_emitter=RaftEventEmitter(logger),
    )

    simulation.run()

    assert_eventually(
        lambda: len(leader_payloads) >= 2,
        message="Expected at least two leader_elected messages due to re-election",
    )
    assert_eventually(
        lambda: len(transition_payloads) >= len(leader_payloads),
        message="Expected node_role_transition events for leader elections",
    )

    leader_events = cast(
        list[dict[str, object]], [json.loads(payload) for payload in leader_payloads]
    )
    assert all(event.get("event") == "leader_elected" for event in leader_events)


def test_simulation_uses_event_name_as_fallback_mqtt_topic(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
) -> None:
    broker, port = mqtt_broker

    started_payloads = mqtt_subscriber_factory("simulation_started")

    logger = MqttLogger(broker=broker, port=port, topic_mapping=TopicMapping({}))
    simulation = load_simulation_from_file(
        "tests/fixtures/configs/system_crash.json",
        event_emitter=RaftEventEmitter(logger),
    )

    simulation.run()

    assert_eventually(
        lambda: bool(started_payloads),
        message="Expected fallback publication on simulation_started topic",
    )
    assert json.loads(started_payloads[0])["event"] == "simulation_started"
