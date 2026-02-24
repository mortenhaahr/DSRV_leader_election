import random

from src.simulation import Simulation

if __name__ == "__main__":
    simulation = Simulation()
    seed = random.randint(0, 100000)
    simulation.run(seed=seed, num_nodes=3)
