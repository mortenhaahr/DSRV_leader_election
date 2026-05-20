# TODO: no support for type mapping since we are not supporting ROS
# at the moment

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast

from .tc_types import VarName

type TopicName = str


@dataclass(frozen=True)
class TopicMapping:
    mapping: dict[VarName, TopicName]

    def __getitem__(self, key: VarName) -> TopicName:
        if key in self.mapping:
            return self.mapping[key]

        # Auto-derive a per-node topic from a "{base_var}_node_{N}" key.
        # e.g. "node_role_transition_node_0" -> "raft/node/role_transition/0"
        # when "node_role_transition" -> "raft/node/role_transition" is mapped.
        if "_node_" in key:
            base, _, node_str = key.rpartition("_node_")
            if node_str.isdigit() and base in self.mapping:
                return f"{self.mapping[base]}/{node_str}"

        # Fallback: use the key itself as the topic name
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
