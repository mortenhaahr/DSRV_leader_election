# pyright: reportMissingTypeStubs=false

import json
import re
import threading
import time
from collections.abc import Callable
from typing import Protocol, SupportsInt, runtime_checkable

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.enums import CallbackAPIVersion
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from dsrv_leader_election.event_logger.mqtt_logger import MqttLogger
from dsrv_leader_election.event_logger.tc_types import TypedTCData
from dsrv_leader_election.event_logger.topic_mapping import TopicMapping


@runtime_checkable
class _HasIsFailure(Protocol):
    is_failure: bool


def _connect_succeeded(reason_code: object) -> bool:
    if isinstance(reason_code, _HasIsFailure):
        return not reason_code.is_failure
    if isinstance(reason_code, SupportsInt):
        return int(reason_code) == 0
    return False


def assert_eventually(
    predicate: Callable[[], bool],
    *,
    timeout_s: float = 3.0,
    interval_s: float = 0.05,
    message: str = "Condition was not met in time",
) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval_s)
    assert predicate(), message


@pytest.fixture(scope="module")
def mqtt_container(request: pytest.FixtureRequest) -> DockerContainer:
    container = DockerContainer("eclipse-mosquitto:latest")
    container = container.with_exposed_ports(1883)
    container = container.waiting_for(
        LogMessageWaitStrategy(re.compile(r"mosquitto version \d+\.\d+\.\d+ running"))
    )
    container = container.start()

    def stop_container() -> None:
        container.stop()

    request.addfinalizer(stop_container)

    return container


@pytest.fixture
def mqtt_broker(mqtt_container: DockerContainer) -> tuple[str, int]:
    return mqtt_container.get_container_host_ip(), int(
        mqtt_container.get_exposed_port(1883)
    )


@pytest.fixture
def mqtt_subscriber_factory(
    request: pytest.FixtureRequest, mqtt_broker: tuple[str, int]
) -> Callable[[str], list[str]]:
    broker, port = mqtt_broker
    subscribers: list[mqtt.Client] = []

    def make_subscriber(topic: str) -> list[str]:
        received_payloads: list[str] = []
        connected = threading.Event()
        subscribed = threading.Event()

        subscriber = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)

        def on_connect(
            client: mqtt.Client,
            _userdata: object,
            _flags: object,
            reason_code: object,
            _properties: object = None,
        ) -> None:
            assert _connect_succeeded(reason_code), (
                f"Subscriber connection failed with reason_code={reason_code}"
            )
            connected.set()
            _ = client.subscribe(topic)

        def on_subscribe(
            _client: mqtt.Client,
            _userdata: object,
            _mid: object,
            _reason_codes: object,
            _properties: object = None,
        ) -> None:
            subscribed.set()

        def on_message(
            _client: mqtt.Client, _userdata: object, msg: mqtt.MQTTMessage
        ) -> None:
            received_payloads.append(msg.payload.decode("utf-8"))

        subscriber.on_connect = on_connect
        subscriber.on_subscribe = on_subscribe
        subscriber.on_message = on_message

        connect_res = subscriber.connect(broker, port)
        assert connect_res == 0
        _ = subscriber.loop_start()

        assert connected.wait(timeout=3.0), "Subscriber did not connect in time"
        assert subscribed.wait(timeout=3.0), "Subscriber did not subscribe in time"

        subscribers.append(subscriber)
        return received_payloads

    def cleanup() -> None:
        for subscriber in subscribers:
            _ = subscriber.loop_stop()
            _ = subscriber.disconnect()

    request.addfinalizer(cleanup)

    return make_subscriber


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
