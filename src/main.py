import json

from src.cli_parser import cli_parse_args
from src.filters import (
    CrashFilter,
    LatencyFilter,
    ReceiverFilter,
    SenderFilter,
    SenderReceiverFilter,
    TimedFilter,
)
from src.json_parser import json_parse_config_dict
from src.log_config import configure_logging
from src.simulation import Simulation


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


def print_config_summary(config: dict) -> None:
    filters = config.get("filters")
    if filters is None:
        filter_summary = "None"
    else:
        filter_summary = ", ".join(_format_filter_details(f) for f in filters)
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
    args = cli_parse_args()
    config = vars(args).copy()
    if args.json:
        with open(args.json, "r") as config_file:
            json_config = json.load(config_file)
        json_seed = json_config.get("seed", args.seed)
        parsed_config = json_parse_config_dict(json_config, seed=json_seed)
        for key, value in parsed_config.items():
            if key in config:
                config[key] = value
        config["filters"] = parsed_config.get("filters", [])

    _ = configure_logging(config["log_level"])
    print_config_summary(config)
    simulation = Simulation(
        seed=config["seed"],
        num_nodes=config["num_nodes"],
        duration_s=config["duration_s"],
        tick_ms=config["tick_ms"],
        heartbeat_interval_ms=int(config["heartbeat_interval_ms"]),
        node_timeout_limits=config["node_timeout_range_ms"],
        filters=config.get("filters"),
    )
    simulation.run()
