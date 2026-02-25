import json

from src.cli_parser import cli_parse_args
from src.json_parser import json_parse_config_dict
from src.log_config import configure_logging
from src.simulation import Simulation

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

    _ = configure_logging(config["log_level"])
    simulation = Simulation(
        seed=config["seed"],
        num_nodes=config["num_nodes"],
        duration_s=config["duration_s"],
        tick_ms=config["tick_ms"],
        node_timeout_limits=config["node_timeout_range_ms"],
    )
    simulation.run()
