from __future__ import annotations

import json
from typing import Any
from jsonschema.exceptions import ValidationError
from dataclasses import dataclass

from src.filters import (
    Filter,
    TimedFilter,
    SenderFilter,
    ReceiverFilter,
    SenderReceiverFilter,
    LatencyFilter,
    CrashFilter,
)
from src.json_validator import validate_filter_config


def parse_filters(filters_spec: list[dict[str, Any]], seed: int = 0) -> list[Filter]:
    """
    Build the list of Filter objects from a validated list of filter specs.

    - filters_spec: list of filter spec dicts (each must have a "type" key)
    - seed: base seed used for some Filters (incremented per Filter)

    Raises:
        - ValueError for semantic issues (e.g. latency delay_ms has lo > hi, unknown filter type)
    """

    next_seed = seed

    def parse_one_filter(spec: dict[str, Any]) -> Filter:
        nonlocal next_seed

        t = spec["type"]

        if t == "timed":
            inner = parse_one_filter(spec["inner"])
            return TimedFilter(
                inner=inner,
                start_tick=spec["start_tick"],
                duration=spec["duration"],
            )

        if t == "sender":
            inner = parse_one_filter(spec["inner"])
            return SenderFilter(
                inner=inner,
                sender_id=spec["sender_id"],
            )

        if t == "receiver":
            inner = parse_one_filter(spec["inner"])
            return ReceiverFilter(
                inner=inner,
                receiver_id=spec["receiver_id"],
            )

        if t == "sender_receiver":
            inner = parse_one_filter(spec["inner"])
            return SenderReceiverFilter(
                inner=inner,
                node_id=spec["node_id"],
            )

        if t == "latency":
            lo, hi = spec["delay_ms"]
            if lo > hi:
                raise ValueError(
                    f"latency.delay_ms must be [min,max] with min<=max, got {spec['delay_ms']}"
                )
            f = LatencyFilter(delay_distribution=(lo, hi), seed=next_seed)
            next_seed += 1
            return f

        if t == "crash":
            return CrashFilter()

        # Should be unreachable if schema validation ran
        raise ValueError(f"Unknown filter type: {t}")

    return [parse_one_filter(spec) for spec in filters_spec]


def json_parse_config_dict(
    config_dict: dict[str, Any], seed: int = 0
) -> dict[str, Any]:
    """
    Parse the JSON config dict and return the config dict with filters parsed to Filter objects.

    Raises:
        - jsonschema.ValidationError if the input dict doesn't match FILTER_SCHEMA
        - ValueError for semantic issues in filters (e.g. latency delay_ms has lo > hi, unknown filter type)
    """
    validate_filter_config(config_dict)
    config_dict["filters"] = parse_filters(config_dict["filters"], seed=seed)

    return config_dict


def json_parse_config_file(file_path: str, seed: int = 0) -> dict[str, Any]:
    """
    Parse the JSON config file and return the config dict with filters parsed to Filter objects.

    Raises:
        - FileNotFoundError if file_path doesn't exist
        - json.JSONDecodeError if file contents is not valid JSON
        - jsonschema.ValidationError if the parsed JSON doesn't match FILTER_SCHEMA
        - ValueError for semantic issues in filters (e.g. latency delay_ms has lo > hi, unknown filter type)
    """
    with open(file_path, "r") as f:
        sim_config = json.load(f)
    return json_parse_config_dict(sim_config, seed=seed)


def json_parse_config_str(json_str: str, seed: int = 0) -> dict[str, Any]:
    """
    Parse the JSON config and return the config dict with filters parsed to Filter objects.

    Raises:
        - json.JSONDecodeError if json_str is not valid JSON
        - jsonschema.ValidationError if the parsed JSON doesn't match FILTER_SCHEMA
        - ValueError for semantic issues in filters (e.g. latency delay_ms has lo > hi, unknown filter type)
    """
    sim_config = json.loads(json_str)
    return json_parse_config_dict(sim_config, seed=seed)


if __name__ == "__main__":
    example = {
        "duration_s": 1.0,
        "num_nodes": 7,
        "tick_ms": 1,
        "heartbeat_interval_ms": 10.0,
        "seed": 42,
        "log_level": "WARNING",
        "node_timeout_range_ms": [10, 20],
        "filters": [
            {"type": "crash"},
            {"type": "latency", "delay_ms": [5, 15]},
        ],
    }

    print(json_parse_config_str(json.dumps(example)))
