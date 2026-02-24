from __future__ import annotations
from collections import deque

from src.log_config import configure_logging
from src.raft_node import RaftNode


class Simulation:
    def run(self) -> None:
        logger, tick_filter = configure_logging()
        N = 3
        nodes = [RaftNode(node_id=i, seed=i) for i in range(N)]
        tick_inc = 20
        messages = deque()
        next_tick_messages = deque()
        for tick in range(0, 500, tick_inc):
            tick_filter.set_tick(tick)
            # Deliver messages scheduled for this tick
            messages.extend(next_tick_messages)
            next_tick_messages.clear()

            for node in nodes:
                outgoing = node.handle_tick(tick_time_ms=tick)
                if outgoing:
                    logger.info(f"Node {node.node_id} tick generated messages: {list(outgoing)}")
                messages.extend(outgoing)
            while messages:
                message = messages.popleft()
                for node in nodes:
                    if node.node_id == message.receiver:
                        new_messages = node.handle_message(message)
                        logger.info(f"Node {node.node_id} received from {message.sender}: {message}")
                        if new_messages:
                            logger.info(f"Node {node.node_id} generated new messages: {list(new_messages)}")
                            next_tick_messages.extend(new_messages)
                        break



if __name__ == "__main__":
    simulation = Simulation()
    simulation.run()
