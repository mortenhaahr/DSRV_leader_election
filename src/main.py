import random

from src.simulation import Simulation
from src.parser import parse_args
from src.log_config import configure_logging

if __name__ == "__main__":
    args = parse_args()
    _ = configure_logging(args.log_level)
    simulation = Simulation(
        seed=args.seed,
        num_nodes=args.num_nodes,
        duration_s=args.duration_s,
        tick_ms=args.tick_ms,
        node_timeout_limits=args.node_timeout_range_ms,
    )
    simulation.run()
