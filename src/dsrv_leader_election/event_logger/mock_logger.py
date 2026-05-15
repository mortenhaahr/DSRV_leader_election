from dataclasses import dataclass, field
from typing import override

from .rv_logger import EventLogger
from .tc_types import TypedTCData, VarName
from .topic_mapping import TopicMapping

type TopicName = str


@dataclass(frozen=True)
class EmittedLogMessage:
    var: VarName
    topic: TopicName
    value: TypedTCData


@dataclass
class MockEventLogger(EventLogger):
    emitted_messages: list[EmittedLogMessage] = field(default_factory=list)

    @override
    def emit(self, var: VarName, value: TypedTCData) -> None:
        self.emitted_messages.append(
            EmittedLogMessage(var=var, topic=self.topic_mapping[var], value=value)
        )

    def clear(self) -> None:
        self.emitted_messages.clear()

    def last(self) -> EmittedLogMessage | None:
        if not self.emitted_messages:
            return None
        return self.emitted_messages[-1]


mock_event_logger = MockEventLogger(topic_mapping=TopicMapping({}))
