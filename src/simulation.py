from __future__ import annotations

import random

from src.log_config import configure_logging
from src.raft_node import RaftNode, Role
from src.message_scheduler import MessageScheduler
from src.filters import CrashFilter, SenderReceiverFilter, LatencyFilter, TimedFilter


class Simulation:
    def __init__(
        self,
        seed: int,
        num_nodes: int,
        duration_s: float,
        tick_ms: int,
        node_timeout_limits: tuple[int, int],
    ):
        self.seed = seed
        self.num_nodes = num_nodes
        self.duration_s = duration_s
        self.tick_ms = tick_ms
        self.node_timeout_range = node_timeout_limits

    def run(self) -> None:
        logging, tick_filter = configure_logging()
        nodes = [
            RaftNode(
                node_id=i,
                seed=self.seed + i,
                deadline_range=self.node_timeout_range,
                cluster_size=self.num_nodes,
            )
            for i in range(self.num_nodes)
        ]
        tick_ms = self.tick_ms
        scheduler = MessageScheduler()
        filters = [
            LatencyFilter(
                delay_distribution=(2, 10), seed=self.seed + 1
            ),  # Overall latency
            SenderReceiverFilter(
                LatencyFilter(delay_distribution=(30, 100), seed=self.seed + 2),
                node_id=0,
            ),  # Higher latency for node 0
        ]
        for f in filters:
            scheduler.add_filter(f)

        next_tick_messages = []
        for tick in range(0, int(self.duration_s * 1000), tick_ms):
            tick_filter.set_tick(tick)

            if tick == 200:
                leader_id = next(
                    (node.node_id for node in nodes if node.state == Role.LEADER), None
                )
                if leader_id is not None:
                    logging.info(
                        f"Simulating crash of leader node {leader_id} at tick {tick}"
                    )
                    crash_filter = TimedFilter(
                        SenderReceiverFilter(
                            CrashFilter(),
                            node_id=leader_id,
                        ),
                        start_tick=tick,
                        duration=150,
                    )
                    scheduler.add_filter(crash_filter)
                else:
                    logging.warning(f"No leader found at tick {tick} to crash.")

            # Schedule messages generated in the previous tick
            if next_tick_messages:
                scheduler.schedule_messages(next_tick_messages)
                next_tick_messages = []

            # Process each node's tick and collect outgoing messages
            for node in nodes:
                outgoing = node.handle_tick(tick_time_ms=tick)
                scheduler.schedule_messages(outgoing)

            # Deliver all messages scheduled for this tick
            to_deliver = scheduler.deliver_messages(tick)
            for message in to_deliver:
                for node in nodes:
                    if node.node_id == message.receiver:
                        next_tick_messages.extend(node.handle_message(message))
                        break
