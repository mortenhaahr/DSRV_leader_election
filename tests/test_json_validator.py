from __future__ import annotations

import unittest

from src.json_parser import validate_filter_config


class JsonValidatorTest(unittest.TestCase):
    def test_validate_filter_config(self) -> None:
        """Ensure filter configs are validated correctly."""
        valid_configs = [
            # 1) timed -> sender_receiver -> latency
            {
                "type": "timed",
                "start_tick": 100,
                "duration": 30,
                "inner": {
                    "type": "sender_receiver",
                    "node_id": 2,
                    "inner": {"type": "latency", "delay_ms": [5, 15]},
                },
            },
            # 2) leaf only: crash (unconditional drop)
            {"type": "crash"},
            # 3) leaf only: latency (global latency)
            {"type": "latency", "delay_ms": [0, 0]},
            # 4) timed -> crash (global crash window)
            {
                "type": "timed",
                "start_tick": 10,
                "duration": 5,
                "inner": {"type": "crash"},
            },
            # 5) sender -> latency (only messages from sender 0 get latency)
            {
                "type": "sender",
                "sender_id": 0,
                "inner": {"type": "latency", "delay_ms": [1, 3]},
            },
            # 6) receiver -> crash (drop messages to receiver 4)
            {
                "type": "receiver",
                "receiver_id": 4,
                "inner": {"type": "crash"},
            },
            # 7) timed -> sender -> crash (sender 1 crashes for a window)
            {
                "type": "timed",
                "start_tick": 200,
                "duration": 100,
                "inner": {
                    "type": "sender",
                    "sender_id": 1,
                    "inner": {"type": "crash"},
                },
            },
            # 8) nested timed wrappers are fine (time window within a time window)
            {
                "type": "timed",
                "start_tick": 0,
                "duration": 1000,
                "inner": {
                    "type": "timed",
                    "start_tick": 250,
                    "duration": 50,
                    "inner": {"type": "latency", "delay_ms": [10, 20]},
                },
            },
            # 9) sender_receiver -> latency (node 3 involved either direction gets latency)
            {
                "type": "sender_receiver",
                "node_id": 3,
                "inner": {"type": "latency", "delay_ms": [2, 2]},
            },
        ]

        for config in valid_configs:
            validate_filter_config(config)


if __name__ == "__main__":
    unittest.main()
