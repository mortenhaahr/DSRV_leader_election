from __future__ import annotations

from src.raft_node import RaftNode


class Simulation:
    def run(self) -> None:
        N = 3
        nodes = [RaftNode(node_id=i, seed=i) for i in range(N)]
        tick_inc = 20
        messages = []
        for tick in range(0, 300, tick_inc):
            for node in nodes:
                messages.extend(node.handle_tick(tick_time_ms=tick))
            print(f"Tick: {tick}, Messages: {messages}")


if __name__ == "__main__":
    simulation = Simulation()
    simulation.run()
