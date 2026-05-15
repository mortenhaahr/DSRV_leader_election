import argparse
import random
from typing import cast

from .config_loader import coerce_timeout_range
from .log_config import LOG_LEVELS


def _positive_float(x: str) -> float:
    v = float(x)
    if v <= 0:
        raise argparse.ArgumentTypeError("must be > 0.0")
    return v


def _positive_int(s: str) -> int:
    v = int(s)
    if v <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return v


def cli_parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RAFT leader-election simulation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    _ = parser.add_argument("--duration-s", type=_positive_float, default=0.5)
    _ = parser.add_argument("--num-nodes", type=_positive_int, default=3)
    _ = parser.add_argument("--tick-ms", type=_positive_int, default=1)
    _ = parser.add_argument(
        "--heartbeat-interval-ms", type=_positive_float, default=20.0
    )
    _ = parser.add_argument("--seed", type=int, default=None)
    _ = parser.add_argument(
        "--log-level",
        default="INFO",
        choices=LOG_LEVELS,
        type=str.upper,  # allows: debug, Debug, DEBUG, etc.
        help="Log level (debug, info, warning, error, critical)",
    )

    _ = parser.add_argument(
        "--node-timeout-range-ms",
        metavar=("START", "END"),
        type=_positive_int,
        nargs=2,
        default=(150, 300),
        help="Range for random election timeout intervals in milliseconds (default: 150 300)",
    )

    _ = parser.add_argument(
        "--json",
        type=str,
        default=None,
        help="Path to JSON configuration file",
    )

    _ = parser.add_argument(
        "--event-logger",
        choices=("none", "mqtt"),
        default="none",
        help="Event logger backend",
    )
    _ = parser.add_argument(
        "--mqtt-broker",
        type=str,
        default="localhost",
        help="MQTT broker hostname or IP",
    )
    _ = parser.add_argument(
        "--mqtt-port",
        type=_positive_int,
        default=1883,
        help="MQTT broker port",
    )
    _ = parser.add_argument(
        "--topic-mapping-json",
        type=str,
        default="example_configs/event_topics/raft_event_topics.json",
        help="Path to event topic mapping JSON file",
    )

    args = parser.parse_args()
    raw_args = cast(dict[str, object], vars(args))

    timeout_range = raw_args.get("node_timeout_range_ms")
    try:
        _ = coerce_timeout_range(timeout_range, "node_timeout_range_ms")
    except ValueError as exc:
        parser.error(str(exc))

    if raw_args.get("seed") is None:
        args.seed = random.randint(0, 100000)

    return args
