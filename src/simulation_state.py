from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SimulationState:
    leader_id: int | None
