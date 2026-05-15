from __future__ import annotations

import logging
from collections.abc import Sequence

import pytest

from dsrv_leader_election.config_loader import load_simulation_from_args
from dsrv_leader_election.filters import (
    LatencyFilter,
    LeaderReceiverFilter,
    LeaderSenderFilter,
    LeaderSenderReceiverFilter,
    ReceiverFilter,
    SenderFilter,
    SenderReceiverFilter,
    TimedFilter,
)


def _first_latency_filter(filters: Sequence[object]) -> LatencyFilter:
    def walk(filter_obj: object) -> LatencyFilter | None:
        if isinstance(filter_obj, LatencyFilter):
            return filter_obj
        if isinstance(
            filter_obj,
            (
                TimedFilter,
                SenderFilter,
                ReceiverFilter,
                SenderReceiverFilter,
                LeaderSenderFilter,
                LeaderReceiverFilter,
            ),
        ):
            return walk(filter_obj.inner)
        if isinstance(filter_obj, LeaderSenderReceiverFilter):
            return walk(filter_obj.sender_filter.inner)
        return None

    for filter_obj in filters:
        found = walk(filter_obj)
        if found is not None:
            return found

    raise AssertionError("Expected at least one LatencyFilter")


def test_seed_from_cli_overrides_config_file_seed_and_logs_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        sim_a = load_simulation_from_args(
            {
                "json": "tests/fixtures/configs/leader_crash_timed.json",
                "seed": 123,
            }
        )

    with caplog.at_level(logging.WARNING):
        sim_b = load_simulation_from_args(
            {
                "json": "tests/fixtures/configs/leader_crash_timed.json",
                "seed": 999,
            }
        )

    warning_messages = [record.message for record in caplog.records]
    assert any("using CLI seed value" in message for message in warning_messages)

    # CLI seed should take precedence over config-file seed.
    assert sim_a.seed == 123
    assert sim_b.seed == 999

    # Latency filter RNG must also follow the effective seed, so different CLI seeds diverge.
    latency_a = _first_latency_filter(sim_a.filters)
    latency_b = _first_latency_filter(sim_b.filters)
    assert latency_a.random.randint(0, 1_000_000) != latency_b.random.randint(
        0, 1_000_000
    )


def test_seed_from_config_is_used_when_cli_seed_is_not_provided() -> None:
    sim = load_simulation_from_args(
        {
            "json": "tests/fixtures/configs/leader_crash_timed.json",
            "seed": None,
        }
    )

    assert sim.seed == 7


def test_seed_falls_back_to_default_when_missing_in_cli_and_config() -> None:
    sim = load_simulation_from_args(
        {
            "json": "tests/fixtures/configs/system_crash.json",
            "seed": None,
        }
    )

    assert sim.seed == 42
