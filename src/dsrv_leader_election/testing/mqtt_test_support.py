# pyright: reportMissingTypeStubs=false

from __future__ import annotations

import queue as _queue
import re
import threading
import time
from collections.abc import Callable, Iterator
from typing import Protocol, SupportsInt, runtime_checkable

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.enums import CallbackAPIVersion
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy


@runtime_checkable
class _HasIsFailure(Protocol):
    is_failure: bool


def _connect_succeeded(reason_code: object) -> bool:
    if isinstance(reason_code, _HasIsFailure):
        return not reason_code.is_failure
    if isinstance(reason_code, SupportsInt):
        return int(reason_code) == 0
    return False


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


class MqttMessageStream:
    """Context manager that subscribes to an MQTT topic and provides a blocking iterator over messages."""

    def __init__(self, broker: str, port: int, topic: str) -> None:
        self._queue: _queue.Queue[str | None] = _queue.Queue()
        connected = threading.Event()
        subscribed = threading.Event()

        self._client: mqtt.Client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2
        )

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
            self._queue.put(msg.payload.decode("utf-8"))

        self._client.on_connect = on_connect
        self._client.on_subscribe = on_subscribe
        self._client.on_message = on_message

        connect_res = self._client.connect(broker, port)
        assert connect_res == 0
        _ = self._client.loop_start()

        assert connected.wait(timeout=3.0), "Subscriber did not connect in time"
        assert subscribed.wait(timeout=3.0), "Subscriber did not subscribe in time"

    def __enter__(self) -> "MqttMessageStream":
        return self

    def __exit__(self, *_args: object) -> None:
        self._queue.put(None)  # unblock any blocked receive()
        _rc1 = self._client.loop_stop()
        _rc2 = self._client.disconnect()

    def receive(
        self,
        *,
        timeout_s: float = 3.0,
        idle_timeout_s: float | None = None,
    ) -> Iterator[str]:
        """Yield messages as they arrive.

        Stops when ``timeout_s`` elapses from the start of the call, or when
        no message has arrived within ``idle_timeout_s`` after the most recent
        one (whichever occurs first). ``idle_timeout_s`` is useful after the
        publisher has already finished so that receive returns quickly rather
        than waiting for the full ``timeout_s``.
        """
        now = time.time()
        deadline = now + timeout_s
        idle_deadline = now + idle_timeout_s if idle_timeout_s is not None else None

        while True:
            now = time.time()
            remaining = deadline - now
            if remaining <= 0:
                return
            if idle_deadline is not None and now >= idle_deadline:
                return
            poll = min(remaining, 0.1)
            if idle_deadline is not None:
                poll = min(poll, max(idle_deadline - now, 0.0))
            try:
                item = self._queue.get(timeout=poll)
            except _queue.Empty:
                continue
            if item is None:
                return
            if idle_timeout_s is not None:
                idle_deadline = time.time() + idle_timeout_s
            yield item


@pytest.fixture
def mqtt_subscriber_factory(
    mqtt_broker: tuple[str, int],
) -> Callable[[str], "MqttMessageStream"]:
    broker, port = mqtt_broker

    def make_stream(topic: str) -> MqttMessageStream:
        return MqttMessageStream(broker, port, topic)

    return make_stream
