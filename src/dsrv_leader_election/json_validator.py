from __future__ import annotations

from typing import cast

from .log_config import LOG_LEVELS


class ValidationError(ValueError):
    pass


_FILTER_TYPES = {
    "timed",
    "sender",
    "receiver",
    "sender_receiver",
    "leader_sender",
    "leader_receiver",
    "leader_msg",
    "latency",
    "crash",
}


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _path(path: str, segment: str) -> str:
    if not path:
        return segment
    if segment.startswith("["):
        return f"{path}{segment}"
    return f"{path}.{segment}"


def _error(path: str, message: str) -> None:
    if path:
        raise ValidationError(f"{path}: {message}")
    raise ValidationError(message)


def _expect_dict(value: object, path: str) -> dict[str, object]:
    if not isinstance(value, dict):
        _error(path, f"expected object, got {type(value).__name__}")
    return cast(dict[str, object], value)


def _expect_list(value: object, path: str) -> list[object]:
    if not isinstance(value, list):
        _error(path, f"expected array, got {type(value).__name__}")
    return cast(list[object], value)


def _expect_string(value: object, path: str) -> str:
    if not isinstance(value, str):
        _error(path, f"expected string, got {type(value).__name__}")
    return cast(str, value)


def _expect_int(value: object, path: str, *, min_value: int | None = None) -> int:
    if not _is_int(value):
        _error(path, f"expected integer, got {type(value).__name__}")
    int_value = cast(int, value)
    if min_value is not None and int_value < min_value:
        _error(path, f"expected integer >= {min_value}, got {int_value}")
    return int_value


def _expect_number(
    value: object, path: str, *, min_exclusive: float | None = None
) -> float:
    if not _is_number(value):
        _error(path, f"expected number, got {type(value).__name__}")
    number_value = float(cast(int | float, value))
    if min_exclusive is not None and number_value <= min_exclusive:
        _error(path, f"expected number > {min_exclusive}, got {number_value}")
    return number_value


def _check_keys(
    obj: dict[str, object],
    path: str,
    *,
    required: set[str],
    optional: set[str] | None = None,
) -> None:
    optional = optional or set()
    missing = required.difference(obj.keys())
    if missing:
        missing_list = ", ".join(sorted(missing))
        _error(path, f"missing required key(s): {missing_list}")
    allowed = required.union(optional)
    extra = set(obj.keys()).difference(allowed)
    if extra:
        extra_list = ", ".join(sorted(extra))
        _error(path, f"unexpected key(s): {extra_list}")


def _validate_filter(obj: object, path: str) -> None:
    filter_obj = _expect_dict(obj, path)
    if "type" not in filter_obj:
        _error(path, "missing required key(s): type")

    filter_type = _expect_string(filter_obj.get("type"), _path(path, "type"))
    if filter_type not in _FILTER_TYPES:
        allowed = ", ".join(sorted(_FILTER_TYPES))
        _error(
            _path(path, "type"),
            f"unknown filter type '{filter_type}', allowed: {allowed}",
        )

    if filter_type == "timed":
        _check_keys(
            filter_obj, path, required={"type", "start_tick", "duration", "inner"}
        )
        _ = _expect_int(
            filter_obj["start_tick"], _path(path, "start_tick"), min_value=0
        )
        _ = _expect_int(filter_obj["duration"], _path(path, "duration"), min_value=0)
        _validate_filter(filter_obj["inner"], _path(path, "inner"))
        return

    if filter_type == "sender":
        _check_keys(filter_obj, path, required={"type", "sender_id", "inner"})
        _ = _expect_int(filter_obj["sender_id"], _path(path, "sender_id"), min_value=0)
        _validate_filter(filter_obj["inner"], _path(path, "inner"))
        return

    if filter_type == "receiver":
        _check_keys(filter_obj, path, required={"type", "receiver_id", "inner"})
        _ = _expect_int(
            filter_obj["receiver_id"], _path(path, "receiver_id"), min_value=0
        )
        _validate_filter(filter_obj["inner"], _path(path, "inner"))
        return

    if filter_type == "sender_receiver":
        _check_keys(filter_obj, path, required={"type", "node_id", "inner"})
        _ = _expect_int(filter_obj["node_id"], _path(path, "node_id"), min_value=0)
        _validate_filter(filter_obj["inner"], _path(path, "inner"))
        return

    if filter_type == "leader_sender":
        _check_keys(filter_obj, path, required={"type", "inner"})
        _validate_filter(filter_obj["inner"], _path(path, "inner"))
        return

    if filter_type == "leader_receiver":
        _check_keys(filter_obj, path, required={"type", "inner"})
        _validate_filter(filter_obj["inner"], _path(path, "inner"))
        return

    if filter_type == "leader_msg":
        _check_keys(filter_obj, path, required={"type", "inner"})
        _validate_filter(filter_obj["inner"], _path(path, "inner"))
        return

    if filter_type == "latency":
        _check_keys(filter_obj, path, required={"type", "delay_ms"})
        delay_ms = _expect_list(filter_obj["delay_ms"], _path(path, "delay_ms"))
        if len(delay_ms) != 2:
            _error(_path(path, "delay_ms"), "expected array of 2 integers [min,max]")
        lo = _expect_int(delay_ms[0], _path(path, "delay_ms[0]"), min_value=0)
        hi = _expect_int(delay_ms[1], _path(path, "delay_ms[1]"), min_value=0)
        if lo > hi:
            _error(
                _path(path, "delay_ms"),
                f"expected [min,max] with min<=max, got [{lo}, {hi}]",
            )
        return

    if filter_type == "crash":
        _check_keys(filter_obj, path, required={"type"})
        return


def validate_filter_config(obj: object) -> None:
    """
    Validate the configuration dict and raise ValueError with clear error paths.
    """
    config = _expect_dict(obj, "")
    _check_keys(
        config,
        "",
        required={"filters"},
        optional={
            "duration_s",
            "num_nodes",
            "tick_ms",
            "heartbeat_interval_ms",
            "seed",
            "log_level",
            "node_timeout_range_ms",
        },
    )

    if "duration_s" in config:
        _ = _expect_number(
            config["duration_s"], _path("", "duration_s"), min_exclusive=0
        )

    if "num_nodes" in config:
        _ = _expect_int(config["num_nodes"], _path("", "num_nodes"), min_value=1)

    if "tick_ms" in config:
        _ = _expect_int(config["tick_ms"], _path("", "tick_ms"), min_value=1)

    if "heartbeat_interval_ms" in config:
        _ = _expect_number(
            config["heartbeat_interval_ms"],
            _path("", "heartbeat_interval_ms"),
            min_exclusive=0,
        )

    if "seed" in config:
        _ = _expect_int(config["seed"], _path("", "seed"))

    if "log_level" in config:
        log_level = _expect_string(config["log_level"], _path("", "log_level"))
        if log_level not in LOG_LEVELS:
            allowed = ", ".join(LOG_LEVELS)
            _error(_path("", "log_level"), f"expected one of: {allowed}")

    if "node_timeout_range_ms" in config:
        timeout_range = _expect_list(
            config["node_timeout_range_ms"], _path("", "node_timeout_range_ms")
        )
        if len(timeout_range) != 2:
            _error(_path("", "node_timeout_range_ms"), "expected array of 2 integers")
        _ = _expect_int(
            timeout_range[0], _path("", "node_timeout_range_ms[0]"), min_value=1
        )
        _ = _expect_int(
            timeout_range[1], _path("", "node_timeout_range_ms[1]"), min_value=1
        )

    filters = _expect_list(config.get("filters"), _path("", "filters"))
    for index, item in enumerate(filters):
        _validate_filter(item, _path("", f"filters[{index}]"))
