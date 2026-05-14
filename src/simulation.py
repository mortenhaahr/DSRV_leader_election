from __future__ import annotations

from .event_logger.raft_event_emitter import RaftEventEmitter
from .filters import Filter
from .log_config import log_message_event, set_tick_time
from .message_scheduler import MessageScheduler
from .messages import ElectionMessage
from .raft_node import RaftNode, Role
from .simulation_state import SimulationState


class Simulation:
    seed: int
    num_nodes: int
    duration_s: float
    tick_ms: int
    heartbeat_interval_ms: int
    node_timeout_range: tuple[int, int]
    filters: list[Filter]
    event_emitter: RaftEventEmitter

    def __init__(
        self,
        seed: int,
        num_nodes: int,
        duration_s: float,
        tick_ms: int,
        heartbeat_interval_ms: int,
        node_timeout_limits: tuple[int, int],
        filters: list[Filter] | None = None,
        event_emitter: RaftEventEmitter | None = None,
    ):

        self.seed = seed
        self.num_nodes = num_nodes
        self.duration_s = duration_s
        self.tick_ms = tick_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.node_timeout_range = node_timeout_limits
        self.filters = filters or []
        self.event_emitter = event_emitter or RaftEventEmitter()

    def run(self) -> None:
        self.event_emitter.emit_map(
            "simulation_started",
            {
                "event": "simulation_started",
                "tick": 0,
                "seed": self.seed,
                "num_nodes": self.num_nodes,
                "duration_s": self.duration_s,
                "tick_ms": self.tick_ms,
                "heartbeat_interval_ms": self.heartbeat_interval_ms,
                "timeout_min_ms": self.node_timeout_range[0],
                "timeout_max_ms": self.node_timeout_range[1],
            },
        )

        nodes = [
            RaftNode(
                node_id=i,
                seed=self.seed + i,
                deadline_range=self.node_timeout_range,
                heartbeat_interval_ms=self.heartbeat_interval_ms,
                cluster_size=self.num_nodes,
                event_emitter=self.event_emitter,
            )
            for i in range(self.num_nodes)
        ]
        tick_ms = self.tick_ms
        scheduler = MessageScheduler(event_emitter=self.event_emitter)

        for filter_obj in self.filters:
            scheduler.add_filter(filter_obj)

        next_tick_messages: list[ElectionMessage] = []
        for tick in range(0, int(self.duration_s * 1000), tick_ms):
            set_tick_time(tick)
            self.event_emitter.emit_map(
                "simulation_tick",
                {
                    "event": "simulation_tick",
                    "tick": tick,
                },
            )

            # Schedule messages generated in the previous tick
            if next_tick_messages:
                scheduler.schedule_messages(next_tick_messages)
                next_tick_messages = []

            # Process each node's tick and collect outgoing messages
            for node in nodes:
                outgoing = node.handle_tick(tick_time_ms=tick)
                scheduler.schedule_messages(outgoing)

            leader_id = None
            for node in nodes:
                if node.state == Role.LEADER:
                    leader_id = node.node_id
                    break
            sim_state = SimulationState(leader_id=leader_id, current_tick=tick)
            scheduler.update_state(sim_state)

            # Deliver all messages scheduled for this tick
            to_deliver = scheduler.deliver_messages(tick)
            for message in to_deliver:
                self.event_emitter.emit_message_event(
                    "message_delivered",
                    message,
                    tick=tick,
                )
                for node in nodes:
                    if node.node_id == message.receiver:
                        log_message_event("receive", message, node_id=node.node_id)
                        next_tick_messages.extend(node.handle_message(message))
                        break

        final_leader_id = None
        for node in nodes:
            if node.state == Role.LEADER:
                final_leader_id = node.node_id
                break

        self.event_emitter.emit_map(
            "simulation_finished",
            {
                "event": "simulation_finished",
                "tick": int(self.duration_s * 1000),
                "final_leader_id": -1 if final_leader_id is None else final_leader_id,
            },
        )
