from __future__ import annotations


class SimulationRunContext:
    """Context manager that owns global per-run simulation state."""

    _message_id_counter: int
    _saved_message_id_counters: list[int]
    _tick_time: int
    _saved_tick_times: list[int]

    def __init__(self) -> None:
        self._message_id_counter = 0
        self._saved_message_id_counters = []
        self._tick_time = 0
        self._saved_tick_times = []

    def __enter__(self) -> None:
        self._saved_message_id_counters.append(self._message_id_counter)
        self._saved_tick_times.append(self._tick_time)
        self._message_id_counter = 0
        self._tick_time = 0

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._message_id_counter = self._saved_message_id_counters.pop()
        self._tick_time = self._saved_tick_times.pop()

    def next_message_id(self) -> int:
        self._message_id_counter += 1
        return self._message_id_counter

    def set_tick_time(self, tick: int) -> None:
        self._tick_time = tick

    def current_tick_time(self) -> int:
        return self._tick_time


simulation_run_context = SimulationRunContext()
