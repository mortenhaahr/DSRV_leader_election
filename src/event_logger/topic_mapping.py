# TODO: no support for type mapping since we are not supporting ROS
# at the moment

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast

from event_logger.tc_types import VarName

type TopicName = str


@dataclass(frozen=True)
class TopicMapping:
    mapping: dict[VarName, TopicName]

    def __getitem__(self, key: VarName) -> TopicName:
        try:
            return self.mapping[key]
        except KeyError:
            # Directly use the key name as the topic name if not found
            return key

    def __iter__(self) -> Iterator[VarName]:
        return iter(self.mapping)

    @staticmethod
    def from_json(s: str) -> "TopicMapping":
        loaded_data = cast(object, json.loads(s))

        if not isinstance(loaded_data, dict):
            raise ValueError("data must be an object")

        raw_mapping = cast(dict[object, object], loaded_data)

        res: dict[VarName, TopicName] = {}
        for raw_key, raw_value in raw_mapping.items():
            if not isinstance(raw_key, str):
                raise ValueError("key must be a string")
            if not isinstance(raw_value, dict):
                raise ValueError("value must be an object with a 'topic' key")

            typed_value = cast(dict[str, object], raw_value)
            topic = typed_value.get("topic")

            if not isinstance(topic, str):
                raise ValueError("topic must be a string")
            res[raw_key] = topic

        return TopicMapping(res)
