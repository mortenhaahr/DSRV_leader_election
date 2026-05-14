from typing import cast

from src.cli_parser import cli_parse_args
from src.filters import (
    CrashFilter,
    Filter,
    LatencyFilter,
    ReceiverFilter,
    SenderFilter,
    SenderReceiverFilter,
    TimedFilter,
)
from src.json_parser import json_parse_config_file
from src.log_config import configure_logging
from src.simulation import Simulation


def _to_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str)):
        return int(value)
    raise ValueError(f"{field} must be an integer")


def _to_float(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a number")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    raise ValueError(f"{field} must be a number")


def _format_filter_details(filter_obj: object) -> str:
    if isinstance(filter_obj, TimedFilter):
        duration = filter_obj.end_tick - filter_obj.start_tick
        inner = _format_filter_details(filter_obj.inner)
        return f"Timed(start_tick={filter_obj.start_tick}, duration={duration}, inner={inner})"
    if isinstance(filter_obj, SenderFilter):
        inner = _format_filter_details(filter_obj.inner)
        return f"Sender(sender_id={filter_obj.sender_id}, inner={inner})"
    if isinstance(filter_obj, ReceiverFilter):
        inner = _format_filter_details(filter_obj.inner)
        return f"Receiver(receiver_id={filter_obj.receiver_id}, inner={inner})"
    if isinstance(filter_obj, SenderReceiverFilter):
        inner = _format_filter_details(filter_obj.inner)
        return f"SenderReceiver(node_id={filter_obj.sender_filter.sender_id}, inner={inner})"
    if isinstance(filter_obj, LatencyFilter):
        lo, hi = filter_obj.delay_distribution
        return f"Latency(delay_ms=[{lo}, {hi}])"
    if isinstance(filter_obj, CrashFilter):
        return "Crash()"
    return type(filter_obj).__name__


def print_config_summary(config: dict[str, object]) -> None:
    raw_filters = config.get("filters")
    if isinstance(raw_filters, list):
        typed_filters = cast(list[object], raw_filters)
        filter_summary = ", ".join(
            _format_filter_details(filter_obj) for filter_obj in typed_filters
        )
    else:
        filter_summary = "None"

    print("Welcome to the RAFT leader election simulation.")
    print("Configuration overview:")
    print(f"  duration_s={config['duration_s']}")
    print(f"  num_nodes={config['num_nodes']}")
    print(f"  tick_ms={config['tick_ms']}")
    print(f"  heartbeat_interval_ms={config['heartbeat_interval_ms']}")
    print(f"  seed={config['seed']}")
    print(f"  log_level={config['log_level']}")
    print(f"  node_timeout_range_ms={config['node_timeout_range_ms']}")
    print(f"  filters={filter_summary}")


if __name__ == "__main__":
    # TODO: Bug that we are logging the sending time as the time of receiving the message. (Not sure if bug still is prevalent)
    args = cli_parse_args()
    raw_args = cast(dict[str, object], vars(args))

    config: dict[str, object] = {
        "duration_s": _to_float(raw_args["duration_s"], "duration_s"),
        "num_nodes": _to_int(raw_args["num_nodes"], "num_nodes"),
        "tick_ms": _to_int(raw_args["tick_ms"], "tick_ms"),
        "heartbeat_interval_ms": _to_float(
            raw_args["heartbeat_interval_ms"], "heartbeat_interval_ms"
        ),
        "seed": _to_int(raw_args["seed"], "seed"),
        "log_level": str(raw_args["log_level"]),
        "node_timeout_range_ms": cast(
            tuple[int, int], raw_args["node_timeout_range_ms"]
        ),
        "filters": None,
    }

    json_path = raw_args.get("json")
    if isinstance(json_path, str) and json_path:
        parsed_config = json_parse_config_file(
            json_path, seed=_to_int(config["seed"], "seed")
        )

        for key, value in parsed_config.items():
            if key in config:
                config[key] = value
        config["filters"] = parsed_config.get("filters", [])

    _ = configure_logging(str(config["log_level"]))

    raw_timeout_range = config["node_timeout_range_ms"]
    if isinstance(raw_timeout_range, list):
        timeout_values = cast(list[object], raw_timeout_range)
    elif isinstance(raw_timeout_range, tuple):
        timeout_values = cast(tuple[object, ...], raw_timeout_range)
    else:
        raise ValueError("node_timeout_range_ms must contain exactly two integers")

    if len(timeout_values) != 2:
        raise ValueError("node_timeout_range_ms must contain exactly two integers")

    timeout_range = (
        _to_int(timeout_values[0], "node_timeout_range_ms[0]"),
        _to_int(timeout_values[1], "node_timeout_range_ms[1]"),
    )

    raw_filters = config.get("filters")
    filters: list[Filter] | None = (
        cast(list[Filter], raw_filters) if isinstance(raw_filters, list) else None
    )

    print_config_summary(config)
    simulation = Simulation(
        seed=_to_int(config["seed"], "seed"),
        num_nodes=_to_int(config["num_nodes"], "num_nodes"),
        duration_s=_to_float(config["duration_s"], "duration_s"),
        tick_ms=_to_int(config["tick_ms"], "tick_ms"),
        heartbeat_interval_ms=_to_int(
            config["heartbeat_interval_ms"], "heartbeat_interval_ms"
        ),
        node_timeout_limits=timeout_range,
        filters=filters,
    )
    simulation.run()
