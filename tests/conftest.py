from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest_plugins = ("dsrv_leader_election.testing.mqtt_test_support",)

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


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "mqtt: marks tests requiring an MQTT broker")


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    if config.getoption("--test-mqtt"):
        return

    skip_mqtt = pytest.mark.skip(reason="need --test-mqtt option to run")
    for item in items:
        if "mqtt" in item.keywords:
            item.add_marker(skip_mqtt)
