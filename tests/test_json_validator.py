from __future__ import annotations

import unittest

from src.json_parser import validate_filter_config
from src.json_validator import ValidationError


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
        # Mainly sim_config:
        valid_configs += [
            # 12) Validate num_nodes / tick_ms / heartbeat_interval_ms
            {
                "num_nodes": 5,
                "tick_ms": 2,
                "heartbeat_interval_ms": 25.0,
                "filters": [],
            },
            # 13) Validate seed (int) + empty filters
            {
                "seed": 123456,
                "filters": [],
            },
            # 14) Validate log_level enum
            {
                "log_level": "DEBUG",
                "filters": [],
            },
            # 15) Validate node_timeout_range_ms fixed-length tuple
            {
                "node_timeout_range_ms": [150, 300],
                "filters": [],
            },
            # 16) Validate everything together + multiple filters
            {
                "duration_s": 1.0,
                "num_nodes": 7,
                "tick_ms": 1,
                "heartbeat_interval_ms": 10.0,
                "seed": 42,
                "log_level": "WARNING",
                "node_timeout_range_ms": [10, 20],
                "filters": [
                    {"type": "crash"},
                    {"type": "latency", "delay_ms": [5, 15]},
                ],
            },
        ]

        for i, config in enumerate(valid_configs):
            with self.subTest(i=i):
                validate_filter_config(config)

    def test_validate_filter_config_invalid(self) -> None:
        """Ensure invalid filter configs are rejected by the schema."""
        invalid_configs = [
            # 1) Missing required top-level "filters"
            {},
            # 2) duration_s must be > 0
            {"duration_s": 0, "filters": []},
            # 3) num_nodes must be >= 1
            {"num_nodes": 0, "filters": []},
            # 4) tick_ms must be >= 1
            {"tick_ms": 0, "filters": []},
            # 5) heartbeat_interval_ms must be > 0
            {"heartbeat_interval_ms": 0.0, "filters": []},
            # 6) seed must be an integer (schema disallows null/float/string)
            {"seed": None, "filters": []},
            # 7) log_level must be one of the enum values
            {"log_level": "debug", "filters": []},
            # 8) node_timeout_range_ms must be exactly two ints
            {"node_timeout_range_ms": [150], "filters": []},
            # 9) additionalProperties=false at top level (unknown key)
            {"filters": [], "unknown": 123},
            # 10) filter item missing required field ("type")
            {"filters": [{}]},
            # 11) timed missing inner
            {"filters": [{"type": "timed", "start_tick": 0, "duration": 1}]},
            # 12) latency delay_ms wrong length
            {"filters": [{"type": "latency", "delay_ms": [1, 2, 3]}]},
            # 13) sender_receiver missing node_id
            {"filters": [{"type": "sender_receiver", "inner": {"type": "crash"}}]},
        ]

        for i, config in enumerate(invalid_configs):
            with self.subTest(i=i):
                with self.assertRaises(ValidationError):
                    validate_filter_config(config)


if __name__ == "__main__":
    unittest.main()
