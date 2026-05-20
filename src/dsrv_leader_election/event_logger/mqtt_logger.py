from dataclasses import dataclass
from typing import override

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode

from .rv_logger import EventLogger
from .tc_types import TypedTCData, VarName
from .topic_mapping import TopicMapping


class MqttLoggerException(Exception): ...


@dataclass
class MqttLogger(EventLogger):
    def __init__(
        self,
        broker: str,
        port: int,
        topic_mapping: TopicMapping,
        publish_qos: int = 1,
    ):
        super().__init__(topic_mapping)
        self._client: mqtt.Client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2
        )
        self._publish_qos: int = publish_qos

        connect_res = self._client.connect(broker, port)
        if connect_res != MQTTErrorCode.MQTT_ERR_SUCCESS:
            raise MqttLoggerException(
                f"Failed to connect to MQTT broker: {connect_res}"
            )
        loop_res = self._client.loop_start()
        if loop_res != MQTTErrorCode.MQTT_ERR_SUCCESS:
            raise MqttLoggerException(f"Failed to connect to MQTT broker: {loop_res}")

    def __del__(self):
        _ = self._client.loop_stop()
        _ = self._client.disconnect()

    @override
    def emit(self, var: VarName, value: TypedTCData) -> None:
        topic = self.topic_mapping[var]
        json_value = value.to_json()
        mqtt_msg_info = self._client.publish(topic, json_value, qos=self._publish_qos)
        mqtt_msg_info.wait_for_publish(timeout=2.0)
        if not mqtt_msg_info.is_published():
            raise MqttLoggerException("Failed to publish MQTT message")
