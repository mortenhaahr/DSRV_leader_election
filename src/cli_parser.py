import argparse
import random

from src.log_config import LOG_LEVELS


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

    parser.add_argument("--duration-s", type=_positive_float, default=0.5)
    parser.add_argument("--num-nodes", type=_positive_int, default=3)
    parser.add_argument("--tick-ms", type=_positive_int, default=1)
    parser.add_argument("--heartbeat-interval-ms", type=_positive_float, default=20.0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=LOG_LEVELS,
        type=str.upper,  # allows: debug, Debug, DEBUG, etc.
        help="Log level (debug, info, warning, error, critical)",
    )

    parser.add_argument(
        "--node-timeout-range-ms",
        metavar=("START", "END"),
        type=_positive_int,
        nargs=2,
        default=(150, 300),
        help="Range for random election timeout intervals in milliseconds (default: 150 300)",
    )

    parser.add_argument(
        "--json",
        type=str,
        default=None,
        help="Path to JSON configuration file",
    )

    args = parser.parse_args()

    start, end = args.node_timeout_range_ms
    if start >= end:
        parser.error("--node-timeout-range-ms START must be < END")

    if args.seed is None:
        args.seed = random.randint(0, 100000)

    return args
