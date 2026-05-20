# pyright: reportMissingTypeStubs=false

import json
from collections.abc import Callable

import pytest

from dsrv_leader_election.event_logger.mqtt_logger import MqttLogger
from dsrv_leader_election.event_logger.tc_types import TypedTCData
from dsrv_leader_election.event_logger.topic_mapping import TopicMapping
from dsrv_leader_election.testing.mqtt_test_support import MqttMessageStream

pytestmark = pytest.mark.mqtt


def test_emit_publishes_payload_to_mapped_topic(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
) -> None:
    broker, port = mqtt_broker
    topic = "raft/events/generated"

    logger = MqttLogger(
        broker=broker,
        port=port,
        topic_mapping=TopicMapping({"leader_id": topic}),
    )

    with mqtt_subscriber_factory(topic) as stream:
        logger.emit("leader_id", TypedTCData("Int", 7))
        payload = next(stream.receive(timeout_s=3.0), None)

    assert payload is not None, "Expected to receive at least one MQTT message"
    assert json.loads(payload) == 7


def test_emit_uses_var_name_as_fallback_topic(
    mqtt_broker: tuple[str, int],
    mqtt_subscriber_factory: Callable[[str], MqttMessageStream],
) -> None:
    broker, port = mqtt_broker
    fallback_topic = "status"

    logger = MqttLogger(
        broker=broker,
        port=port,
        topic_mapping=TopicMapping({}),
    )

    with mqtt_subscriber_factory(fallback_topic) as stream:
        logger.emit(fallback_topic, TypedTCData("Str", "leader-elected"))
        payload = next(stream.receive(timeout_s=3.0), None)

    assert payload is not None, "Expected to receive payload on fallback topic"
    assert json.loads(payload) == "leader-elected"
