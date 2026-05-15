# pyright: reportMissingTypeStubs=false

import json
from collections.abc import Callable

import pytest

from dsrv_leader_election.event_logger.mqtt_logger import MqttLogger
from dsrv_leader_election.event_logger.tc_types import TypedTCData
from dsrv_leader_election.event_logger.topic_mapping import TopicMapping
from dsrv_leader_election.testing.mqtt_test_support import assert_eventually

pytestmark = pytest.mark.mqtt


def test_emit_publishes_payload_to_mapped_topic(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
) -> None:
    broker, port = mqtt_broker
    topic = "raft/events/generated"
    received_payloads = mqtt_subscriber_factory(topic)

    logger = MqttLogger(
        broker=broker,
        port=port,
        topic_mapping=TopicMapping({"leader_id": topic}),
    )
    logger.emit("leader_id", TypedTCData("Int", 7))

    assert_eventually(
        lambda: bool(received_payloads),
        message="Expected to receive at least one MQTT message",
    )
    assert json.loads(received_payloads[0]) == 7


def test_emit_uses_var_name_as_fallback_topic(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], list[str]],
) -> None:
    broker, port = mqtt_broker
    fallback_topic = "status"
    received_payloads = mqtt_subscriber_factory(fallback_topic)

    logger = MqttLogger(
        broker=broker,
        port=port,
        topic_mapping=TopicMapping({}),
    )
    logger.emit(fallback_topic, TypedTCData("Str", "leader-elected"))

    assert_eventually(
        lambda: bool(received_payloads),
        message="Expected to receive payload on fallback topic",
    )
    assert json.loads(received_payloads[0]) == "leader-elected"
