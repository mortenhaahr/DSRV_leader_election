from abc import ABC, abstractmethod
from dataclasses import dataclass

from .tc_types import TypedTCData, VarName
from .topic_mapping import TopicMapping


# Should be RAII: build and connect on __init__, disconnect
# TODO: nice interface based on context manager?
@dataclass
class EventLogger(ABC):
    topic_mapping: TopicMapping

    @abstractmethod
    def emit(self, var: VarName, value: TypedTCData) -> None: ...
