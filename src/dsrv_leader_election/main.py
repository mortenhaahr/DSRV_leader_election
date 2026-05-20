from typing import cast

from .cli_parser import cli_parse_args
from .config_loader import coerce_int, load_simulation_from_args
from .event_logger.mqtt_logger import MqttLogger
from .event_logger.raft_event_emitter import RaftEventEmitter
from .event_logger.rv_logger import EventLogger
from .event_logger.topic_mapping import TopicMapping
from .filters import (
    CrashFilter,
    LatencyFilter,
    ReceiverFilter,
    SenderFilter,
    SenderReceiverFilter,
    TimedFilter,
)
from .log_config import configure_logging
from .simulation import Simulation


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


def _load_topic_mapping(path: str) -> TopicMapping:
    with open(path, "r", encoding="utf-8") as file_obj:
        return TopicMapping.from_json(file_obj.read())


def _build_event_logger(raw_args: dict[str, object]) -> EventLogger | None:
    event_logger_backend = str(raw_args.get("event_logger", "none"))
    if event_logger_backend != "mqtt":
        return None

    mapping_path = raw_args.get("topic_mapping_json")
    if not isinstance(mapping_path, str) or not mapping_path:
        raise ValueError("--topic-mapping-json is required when --event-logger mqtt")

    topic_mapping = _load_topic_mapping(mapping_path)

    broker_raw = raw_args.get("mqtt_broker", "localhost")
    if not isinstance(broker_raw, str) or not broker_raw:
        raise ValueError("--mqtt-broker must be a non-empty string")

    port_raw = raw_args.get("mqtt_port", 1883)
    mqtt_port = coerce_int(port_raw, "mqtt_port")

    return MqttLogger(broker=broker_raw, port=mqtt_port, topic_mapping=topic_mapping)


def print_config_summary(simulation: Simulation) -> None:
    filter_summary = ", ".join(
        _format_filter_details(filter_obj) for filter_obj in simulation.filters
    )

    print("Welcome to the RAFT leader election simulation.")
    print("Configuration overview:")
    print(f"  duration_s={simulation.duration_s}")
    print(f"  num_nodes={simulation.num_nodes}")
    print(f"  tick_ms={simulation.tick_ms}")
    print(f"  heartbeat_interval_ms={simulation.heartbeat_interval_ms}")
    print(f"  seed={simulation.seed}")
    print(f"  log_level={simulation.log_level}")
    print(f"  node_timeout_range_ms={simulation.node_timeout_range}")
    rtf = simulation.real_time_factor
    print(f"  real_time_factor={'unlimited' if rtf is None else rtf}")
    print(f"  filters={filter_summary if filter_summary else 'None'}")


if __name__ == "__main__":
    # TODO: Bug that we are logging the sending time as the time of receiving the message. (Not sure if bug still is prevalent)
    args = cli_parse_args()
    raw_args = cast(dict[str, object], vars(args))

    event_logger = _build_event_logger(raw_args)
    event_emitter = RaftEventEmitter(event_logger)

    simulation = load_simulation_from_args(raw_args, event_emitter=event_emitter)

    _ = configure_logging(simulation.log_level)

    print_config_summary(simulation)
    simulation.run()
