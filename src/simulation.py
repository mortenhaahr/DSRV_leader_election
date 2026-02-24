from __future__ import annotations
from collections import deque
import random

from src.log_config import configure_logging
from src.raft_node import RaftNode


class Simulation:
    def run(self, seed : int, num_nodes : int) -> None:
        logger, tick_filter = configure_logging()
        nodes = [RaftNode(node_id=i, seed=seed+i) for i in range(num_nodes)]
        tick_inc = 1
        messages = deque()
        next_tick_messages = deque()
        for tick in range(0, 500, tick_inc):
            tick_filter.set_tick(tick)
            # Deliver messages scheduled for this tick
            messages.extend(next_tick_messages)
            next_tick_messages.clear()

            # Process each node's tick and collect outgoing messages
            for node in nodes:
                outgoing = node.handle_tick(tick_time_ms=tick)
                if outgoing:
                    logger.info(f"Node {node.node_id} tick generated messages: {list(outgoing)}")
                messages.extend(outgoing)

            # Process all messages and collect responses for the next tick 
            while messages:
                message = messages.popleft()
                for node in nodes:
                    if node.node_id == message.receiver:
                        next_tick_messages.extend(node.handle_message(message))
                        break



if __name__ == "__main__":
    simulation = Simulation()
    seed = random.randint(0, 100000)
    simulation.run(seed=seed, num_nodes=3)
