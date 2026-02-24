from __future__ import annotations

from src.raft_node import Follower


class Simulation:
    def run(self) -> None:
        N = 3
        nodes = [Follower(node_id=i, seed=i) for i in range(N)]
        tick_inc = 100
        messages = []
        for tick in range(0, 1000, tick_inc):
            for node in nodes:
                messages.extend(node.handle_tick(tick_time_ms=tick))
            print(f"Tick: {tick}, Messages: {messages}")


if __name__ == "__main__":
    simulation = Simulation()
    simulation.run()
