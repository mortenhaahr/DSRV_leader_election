from __future__ import annotations
from collections import deque

from src.raft_node import RaftNode


class Simulation:
    def run(self) -> None:
        N = 3
        nodes = [RaftNode(node_id=i, seed=i) for i in range(N)]
        tick_inc = 20
        messages = deque()
        next_tick_messages = deque()
        for tick in range(0, 500, tick_inc):
            # Deliver messages scheduled for this tick
            messages.extend(next_tick_messages)
            next_tick_messages.clear()

            for node in nodes:
                messages.extend(node.handle_tick(tick_time_ms=tick))
            print(f"Tick: {tick}, Messages: {messages}")
            # Drain the deque completely; handle_message may enqueue more, but only for next tick
            while messages:
                message = messages.popleft()
                for node in nodes:
                    if node.node_id == message.receiver:
                        new_messages = node.handle_message(message)
                        if new_messages:
                            # print(f"Node {node.node_id} received from {message.sender} and generated new messages: {new_messages}")
                            next_tick_messages.extend(new_messages)
                        break


if __name__ == "__main__":
    simulation = Simulation()
    simulation.run()
