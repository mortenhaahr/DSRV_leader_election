from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import cast

from .event_logger.raft_event_emitter import RaftEventEmitter
from .filters import Filter
from .json_parser import json_parse_config_file
from .simulation import Simulation

_logger = logging.getLogger(__name__)

_DEFAULT_DURATION_S = 2.0
_DEFAULT_NUM_NODES = 5
_DEFAULT_TICK_MS = 1
_DEFAULT_HEARTBEAT_INTERVAL_MS = 50.0
_DEFAULT_SEED = 42
_DEFAULT_LOG_LEVEL = "INFO"
_DEFAULT_NODE_TIMEOUT_RANGE_MS = (150, 300)


def coerce_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str)):
        return int(value)
    raise ValueError(f"{field} must be an integer")


def coerce_float(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a number")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    raise ValueError(f"{field} must be a number")


def coerce_timeout_range(
    value: object, field: str = "node_timeout_range_ms"
) -> tuple[int, int]:
    values: tuple[object, object]
    if isinstance(value, tuple):
        tuple_values = cast(tuple[object, ...], value)
        if len(tuple_values) != 2:
            raise ValueError(f"{field} must contain exactly two values")
        values = (tuple_values[0], tuple_values[1])
    elif isinstance(value, list):
        list_values = cast(list[object], value)
        if len(list_values) != 2:
            raise ValueError(f"{field} must contain exactly two values")
        values = (list_values[0], list_values[1])
    else:
        raise ValueError(f"{field} must be a list or tuple")

    start = coerce_int(values[0], f"{field}[0]")
    end = coerce_int(values[1], f"{field}[1]")
    if start >= end:
        raise ValueError(f"{field} START must be < END")
    return (start, end)


def _base_config_from_args(raw_args: Mapping[str, object]) -> dict[str, object]:
    seed_value = raw_args.get("seed")
    return {
        "duration_s": raw_args.get("duration_s", _DEFAULT_DURATION_S),
        "num_nodes": raw_args.get("num_nodes", _DEFAULT_NUM_NODES),
        "tick_ms": raw_args.get("tick_ms", _DEFAULT_TICK_MS),
        "heartbeat_interval_ms": raw_args.get(
            "heartbeat_interval_ms", _DEFAULT_HEARTBEAT_INTERVAL_MS
        ),
        "seed": _DEFAULT_SEED if seed_value is None else seed_value,
        "log_level": raw_args.get("log_level", _DEFAULT_LOG_LEVEL),
        "node_timeout_range_ms": raw_args.get(
            "node_timeout_range_ms", _DEFAULT_NODE_TIMEOUT_RANGE_MS
        ),
        "filters": [],
    }


def _validated_simulation_from_mapping(
    config: Mapping[str, object],
    *,
    event_emitter: RaftEventEmitter | None = None,
) -> Simulation:
    raw_filters = config.get("filters", [])
    if not isinstance(raw_filters, list):
        raise ValueError("filters must be a list")

    return Simulation(
        seed=coerce_int(config.get("seed"), "seed"),
        num_nodes=coerce_int(config.get("num_nodes"), "num_nodes"),
        duration_s=coerce_float(config.get("duration_s"), "duration_s"),
        tick_ms=coerce_int(config.get("tick_ms"), "tick_ms"),
        heartbeat_interval_ms=coerce_int(
            config.get("heartbeat_interval_ms"), "heartbeat_interval_ms"
        ),
        node_timeout_limits=coerce_timeout_range(
            config.get("node_timeout_range_ms", _DEFAULT_NODE_TIMEOUT_RANGE_MS)
        ),
        filters=cast(list[Filter], raw_filters),
        event_emitter=event_emitter,
        log_level=str(config.get("log_level", _DEFAULT_LOG_LEVEL)),
    )


def load_simulation_from_file(
    file_path: str,
    *,
    defaults: Mapping[str, object] | None = None,
    event_emitter: RaftEventEmitter | None = None,
    override_seed: int | None = None,
) -> Simulation:
    base = dict(defaults) if defaults is not None else _base_config_from_args({})
    base_seed = (
        override_seed
        if override_seed is not None
        else coerce_int(base.get("seed", _DEFAULT_SEED), "seed")
    )

    parsed_config = json_parse_config_file(file_path, seed=base_seed)

    if "seed" in parsed_config:
        file_seed = coerce_int(parsed_config["seed"], "seed")
        if file_seed != base_seed:
            if override_seed is not None:
                _logger.warning(
                    "Seed is provided in both CLI and config file; using CLI seed value"
                )
            else:
                parsed_config = json_parse_config_file(file_path, seed=file_seed)

    merged = dict(base)
    for key, value in parsed_config.items():
        if key in merged:
            merged[key] = value
    merged["filters"] = parsed_config.get("filters", [])
    if override_seed is not None:
        merged["seed"] = override_seed

    return _validated_simulation_from_mapping(merged, event_emitter=event_emitter)


def load_simulation_from_args(
    raw_args: Mapping[str, object],
    *,
    event_emitter: RaftEventEmitter | None = None,
) -> Simulation:
    base = _base_config_from_args(raw_args)

    cli_seed_raw = raw_args.get("seed")
    cli_seed = None if cli_seed_raw is None else coerce_int(cli_seed_raw, "seed")

    json_path = raw_args.get("json")
    if isinstance(json_path, str) and json_path:
        return load_simulation_from_file(
            json_path,
            defaults=base,
            event_emitter=event_emitter,
            override_seed=cli_seed,
        )

    return _validated_simulation_from_mapping(base, event_emitter=event_emitter)
