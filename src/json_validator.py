from __future__ import annotations

import json
from typing import Any

from jsonschema import Draft202012Validator

FILTER_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.local/filter.schema.json",
    "title": "Simulation config",
    "$ref": "#/$defs/sim_config",
    "$defs": {
        "sim_config": {
            "type": "object",
            "additionalProperties": False,
            "required": ["filter"],
            "properties": {
                "duration_s": {"type": "number", "exclusiveMinimum": 0},
                "filter": {"$ref": "#/$defs/filter"},
            },
        },
        "filter": {
            "oneOf": [
                {"$ref": "#/$defs/timed"},
                {"$ref": "#/$defs/sender"},
                {"$ref": "#/$defs/receiver"},
                {"$ref": "#/$defs/sender_receiver"},
                {"$ref": "#/$defs/latency"},
                {"$ref": "#/$defs/crash"},
            ]
        },
        "timed": {
            "type": "object",
            "additionalProperties": False,
            "required": ["type", "start_tick", "duration", "inner"],
            "properties": {
                "type": {"const": "timed"},
                "start_tick": {"type": "integer", "minimum": 0},
                "duration": {"type": "integer", "minimum": 0},
                "inner": {"$ref": "#/$defs/filter"},
            },
        },
        "sender": {
            "type": "object",
            "additionalProperties": False,
            "required": ["type", "sender_id", "inner"],
            "properties": {
                "type": {"const": "sender"},
                "sender_id": {"type": "integer", "minimum": 0},
                "inner": {"$ref": "#/$defs/filter"},
            },
        },
        "receiver": {
            "type": "object",
            "additionalProperties": False,
            "required": ["type", "receiver_id", "inner"],
            "properties": {
                "type": {"const": "receiver"},
                "receiver_id": {"type": "integer", "minimum": 0},
                "inner": {"$ref": "#/$defs/filter"},
            },
        },
        "sender_receiver": {
            "type": "object",
            "additionalProperties": False,
            "required": ["type", "node_id", "inner"],
            "properties": {
                "type": {"const": "sender_receiver"},
                "node_id": {"type": "integer", "minimum": 0},
                "inner": {"$ref": "#/$defs/filter"},
            },
        },
        "latency": {
            "type": "object",
            "additionalProperties": False,
            "required": ["type", "delay_ms"],
            "properties": {
                "type": {"const": "latency"},
                "delay_ms": {
                    "type": "array",
                    "prefixItems": [
                        {"type": "integer", "minimum": 0},
                        {"type": "integer", "minimum": 0},
                    ],
                    "items": False,
                    "minItems": 2,
                    "maxItems": 2,
                },
            },
        },
        "crash": {
            "type": "object",
            "additionalProperties": False,
            "required": ["type"],
            "properties": {
                "type": {"const": "crash"},
            },
        },
    },
}

_validator = Draft202012Validator(FILTER_SCHEMA)


def validate_filter_config(obj: Any) -> None:
    """
    Raises jsonschema.ValidationError with a useful message/path on invalid input.
    """
    _validator.validate(obj)
