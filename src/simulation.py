from __future__ import annotations

import random

from src.log_config import configure_logging
from src.raft_node import RaftNode
from src.message_scheduler import (
    MessageScheduler,
    GeneralLatencyFilter,
    NodeLatencyFilter,
)


class Simulation:
    def run(self, seed: int, num_nodes: int) -> None:
        _, tick_filter = configure_logging()
        nodes = [RaftNode(node_id=i, seed=seed + i) for i in range(num_nodes)]
        tick_inc = 1
        scheduler = MessageScheduler()
        filters = [
            GeneralLatencyFilter(delay_distribution=(2, 10), seed=seed + 1),
            # NodeLatencyFilter(node_id=0, delay_distribution=(30, 100), seed=seed + 2),
        ]
        for f in filters:
            scheduler.add_filter(f)

        next_tick_messages = []
        for tick in range(0, 500, tick_inc):
            tick_filter.set_tick(tick)

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


if __name__ == "__main__":
    simulation = Simulation()
    seed = random.randint(0, 100000)
    simulation.run(seed=seed, num_nodes=3)
