from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest_plugins = (
    "dsrv_leader_election.testing.mqtt_test_support",
    "dsrv_leader_election.testing.trustworthiness_checker_test_support",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
TESTS_ROOT = PROJECT_ROOT / "tests"

for path in (PROJECT_ROOT, SRC_ROOT, TESTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--test-mqtt",
        action="store_true",
        default=False,
        help="run tests marked as mqtt",
    )
    parser.addoption(
        "--test-simulations",
        action="store_true",
        default=False,
        help="run tests marked as simulations",
    )
    parser.addoption(
        "--test-end-to-end",
        action="store_true",
        default=False,
        help="run tests marked as end_to_end",
    )
    parser.addoption(
        "--all-tests",
        action="store_true",
        default=False,
        help="run all tests including mqtt, simulation_sequence, and end_to_end tests",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "mqtt: marks tests requiring an MQTT broker")
    config.addinivalue_line(
        "markers",
        "simulations: marks long simulation sequence tests",
    )
    config.addinivalue_line(
        "markers",
        "end_to_end: marks end-to-end integration tests (e.g. trustworthiness-checker)",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    run_all_tests = config.getoption("--all-tests")
    run_end_to_end = run_all_tests or config.getoption("--test-end-to-end")
    run_mqtt = run_all_tests or config.getoption("--test-mqtt")
    run_simulation_sequences = run_all_tests or config.getoption("--test-simulations")

    skip_mqtt = pytest.mark.skip(
        reason="need --test-mqtt (or --all-tests) option to run"
    )
    skip_simseq = pytest.mark.skip(
        reason="need --test-simulations (or --all-tests) option to run"
    )
    skip_end_to_end = pytest.mark.skip(
        reason="need --test-end-to-end (or --all-tests) option to run"
    )

    for item in items:
        if (
            "mqtt" in item.keywords
            and "end_to_end" not in item.keywords
            and not run_mqtt
        ):
            item.add_marker(skip_mqtt)
        if "simulations" in item.keywords and not run_simulation_sequences:
            item.add_marker(skip_simseq)
        if "end_to_end" in item.keywords and not run_end_to_end:
            item.add_marker(skip_end_to_end)
