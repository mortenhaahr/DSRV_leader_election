from __future__ import annotations

from src.filters import Filter
from src.log_config import log_message_event
from src.log_config import set_tick_time
from src.message_scheduler import MessageScheduler
from src.raft_node import RaftNode, Role
from src.simulation_state import SimulationState


class Simulation:
    def __init__(
        self,
        seed: int,
        num_nodes: int,
        duration_s: float,
        tick_ms: int,
        heartbeat_interval_ms: int,
        node_timeout_limits: tuple[int, int],
        filters: list[Filter] | None = None,
    ):
        self.seed = seed
        self.num_nodes = num_nodes
        self.duration_s = duration_s
        self.tick_ms = tick_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.node_timeout_range = node_timeout_limits
        self.filters = filters or []

    def run(self) -> None:
        nodes = [
            RaftNode(
                node_id=i,
                seed=self.seed + i,
                deadline_range=self.node_timeout_range,
                heartbeat_interval_ms=self.heartbeat_interval_ms,
                cluster_size=self.num_nodes,
            )
            for i in range(self.num_nodes)
        ]
        tick_ms = self.tick_ms
        scheduler = MessageScheduler()
        filters = self.filters
        for f in filters:
            scheduler.add_filter(f)

        next_tick_messages = []
        for tick in range(0, int(self.duration_s * 1000), tick_ms):
            set_tick_time(tick)

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
            sim_state = SimulationState(leader_id=leader_id)
            scheduler.update_state(sim_state)

            # Deliver all messages scheduled for this tick
            to_deliver = scheduler.deliver_messages(tick)
            for message in to_deliver:
                for node in nodes:
                    if node.node_id == message.receiver:
                        log_message_event("receive", message, node_id=node.node_id)
                        next_tick_messages.extend(node.handle_message(message))
                        break
