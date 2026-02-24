from __future__ import annotations

import unittest

from src.json_parser import validate_filter_config


class JsonValidatorTest(unittest.TestCase):
    def test_validate_filter_config(self) -> None:
        """Ensure filter configs are validated correctly."""
        valid_configs = [
            # 1) timed -> sender_receiver -> latency (+ duration_s)
            {
                "duration_s": 2.5,
                "filters": [
                    {
                        "type": "timed",
                        "start_tick": 100,
                        "duration": 30,
                        "inner": {
                            "type": "sender_receiver",
                            "node_id": 2,
                            "inner": {"type": "latency", "delay_ms": [5, 15]},
                        },
                    }
                ],
            },
            # 2) crash only
            {"filters": [{"type": "crash"}]},
            # 3) latency only
            {"filters": [{"type": "latency", "delay_ms": [0, 0]}]},
            # 4) timed -> crash (global crash window) (+ duration_s)
            {
                "duration_s": 10.0,
                "filters": [
                    {
                        "type": "timed",
                        "start_tick": 10,
                        "duration": 5,
                        "inner": {"type": "crash"},
                    }
                ],
            },
            # 5) sender -> latency
            {
                "filters": [
                    {
                        "type": "sender",
                        "sender_id": 0,
                        "inner": {"type": "latency", "delay_ms": [1, 3]},
                    }
                ]
            },
            # 6) receiver -> crash
            {
                "filters": [
                    {
                        "type": "receiver",
                        "receiver_id": 4,
                        "inner": {"type": "crash"},
                    }
                ]
            },
            # 7) timed -> sender -> crash
            {
                "filters": [
                    {
                        "type": "timed",
                        "start_tick": 200,
                        "duration": 100,
                        "inner": {
                            "type": "sender",
                            "sender_id": 1,
                            "inner": {"type": "crash"},
                        },
                    }
                ]
            },
            # 8) nested timed wrappers
            {
                "filters": [
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
                    }
                ]
            },
            # 9) sender_receiver -> latency
            {
                "filters": [
                    {
                        "type": "sender_receiver",
                        "node_id": 3,
                        "inner": {"type": "latency", "delay_ms": [2, 2]},
                    }
                ]
            },
            # 10) empty filters allowed (no filtering)
            {"filters": []},
            # 11) multiple filters at once
            {
                "filters": [
                    {"type": "crash"},
                    {"type": "latency", "delay_ms": [5, 15]},
                ]
            },
        ]

        for i, config in enumerate(valid_configs):
            with self.subTest(i=i):
                validate_filter_config(config)


if __name__ == "__main__":
    unittest.main()
