from __future__ import annotations

import json
from typing import cast

from .filters import (
    CrashFilter,
    Filter,
    LatencyFilter,
    LeaderReceiverFilter,
    LeaderSenderFilter,
    LeaderSenderReceiverFilter,
    ReceiverFilter,
    SenderFilter,
    SenderReceiverFilter,
    TimedFilter,
)
from .json_validator import validate_filter_config


def _require_int(spec: dict[str, object], key: str) -> int:
    value = spec[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value


def _require_filter_spec(spec: dict[str, object], key: str) -> dict[str, object]:
    value = spec[key]
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return cast(dict[str, object], value)


def parse_filters(filters_spec: list[dict[str, object]], seed: int = 0) -> list[Filter]:
    """
    Build the list of Filter objects from a validated list of filter specs.

    - filters_spec: list of filter spec dicts (each must have a "type" key)
    - seed: base seed used for some Filters (incremented per Filter)

    Raises:
        - ValueError for semantic issues (e.g. latency delay_ms has lo > hi, unknown filter type)
    """

    next_seed = seed

    def parse_one_filter(spec: dict[str, object]) -> Filter:
        nonlocal next_seed

        filter_type_obj = spec.get("type")
        if not isinstance(filter_type_obj, str):
            raise ValueError("filter type must be a string")
        filter_type = filter_type_obj

        if filter_type == "timed":
            inner = parse_one_filter(_require_filter_spec(spec, "inner"))
            return TimedFilter(
                inner=inner,
                start_tick=_require_int(spec, "start_tick"),
                duration=_require_int(spec, "duration"),
            )

        if filter_type == "sender":
            inner = parse_one_filter(_require_filter_spec(spec, "inner"))
            return SenderFilter(
                inner=inner,
                sender_id=_require_int(spec, "sender_id"),
            )

        if filter_type == "receiver":
            inner = parse_one_filter(_require_filter_spec(spec, "inner"))
            return ReceiverFilter(
                inner=inner,
                receiver_id=_require_int(spec, "receiver_id"),
            )

        if filter_type == "sender_receiver":
            inner = parse_one_filter(_require_filter_spec(spec, "inner"))
            return SenderReceiverFilter(
                inner=inner,
                node_id=_require_int(spec, "node_id"),
            )

        if filter_type == "leader_sender":
            inner = parse_one_filter(_require_filter_spec(spec, "inner"))
            return LeaderSenderFilter(inner=inner)

        if filter_type == "leader_receiver":
            inner = parse_one_filter(_require_filter_spec(spec, "inner"))
            return LeaderReceiverFilter(inner=inner)

        if filter_type == "leader_msg":
            inner = parse_one_filter(_require_filter_spec(spec, "inner"))
            return LeaderSenderReceiverFilter(inner=inner)

        if filter_type == "latency":
            delay_obj = spec.get("delay_ms")
            if not isinstance(delay_obj, list):
                raise ValueError("latency.delay_ms must be [min,max]")

            delay_values = cast(list[object], delay_obj)
            if len(delay_values) != 2:
                raise ValueError("latency.delay_ms must be [min,max]")

            lo_raw, hi_raw = delay_values

            if (
                isinstance(lo_raw, bool)
                or not isinstance(lo_raw, int)
                or isinstance(hi_raw, bool)
                or not isinstance(hi_raw, int)
            ):
                raise ValueError("latency.delay_ms entries must be integers")
            lo, hi = lo_raw, hi_raw
            if lo > hi:
                raise ValueError(
                    f"latency.delay_ms must be [min,max] with min<=max, got {delay_obj}"
                )
            latency_filter = LatencyFilter(delay_distribution=(lo, hi), seed=next_seed)
            next_seed += 1
            return latency_filter

        if filter_type == "crash":
            return CrashFilter()

        # Should be unreachable if schema validation ran
        raise ValueError(f"Unknown filter type: {filter_type}")

    return [parse_one_filter(spec) for spec in filters_spec]


def json_parse_config_dict(
    config_dict: dict[str, object], seed: int = 0
) -> dict[str, object]:
    """
    Parse the JSON config dict and return the config dict with filters parsed to Filter objects.

    Raises:
        - ValidationError if the input dict does not match the expected schema
        - ValueError for semantic issues in filters (e.g. latency delay_ms has lo > hi)
    """
    validate_filter_config(config_dict)

    raw_filters = config_dict.get("filters")
    if not isinstance(raw_filters, list):
        raise ValueError("filters must be a list")

    typed_filters = cast(list[dict[str, object]], raw_filters)
    config_dict["filters"] = parse_filters(typed_filters, seed=seed)
    return config_dict


def json_parse_config_file(file_path: str, seed: int = 0) -> dict[str, object]:
    """
    Parse the JSON config file and return the config dict with filters parsed to Filter objects.

    Raises:
        - FileNotFoundError if file_path doesn't exist
        - json.JSONDecodeError if file contents is not valid JSON
        - jsonschema.ValidationError if the parsed JSON doesn't match FILTER_SCHEMA
        - ValueError for semantic issues in filters (e.g. latency delay_ms has lo > hi, unknown filter type)
    """
    with open(file_path, "r", encoding="utf-8") as file_obj:
        loaded = cast(object, json.load(file_obj))

    if not isinstance(loaded, dict):
        raise ValueError("configuration file must contain a JSON object")

    return json_parse_config_dict(cast(dict[str, object], loaded), seed=seed)
