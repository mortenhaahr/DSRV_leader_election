# pyright: reportMissingTypeStubs=false

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from dsrv_leader_election.event_logger.topic_mapping import TopicMapping

TRUSTWORTHINESS_CHECKER_IMAGE = os.getenv(
    "TRUSTWORTHINESS_CHECKER_IMAGE",
    "thomasdwright/trustworthiness-checker:latest",
)
TRUSTWORTHINESS_CHECKER_BINARY = os.getenv("TRUSTWORTHINESS_CHECKER_BINARY")


def load_example_topic_mapping() -> TopicMapping:
    mapping_path = Path("tests/fixtures/event_topics/raft_event_topics.json")
    with open(mapping_path, "r", encoding="utf-8") as file_obj:
        return TopicMapping.from_json(file_obj.read())


def decode_checker_payload(payload: str) -> object:
    raw = cast(object, json.loads(payload))
    assert isinstance(raw, dict), "Checker payload must be a JSON object"
    message = cast(dict[str, object], raw)
    assert "value" in message, "Checker payload must contain a top-level 'value' key"
    return message["value"]


def in_container_to_host_path(path_in_container: str, fixtures_dir: Path) -> str:
    if path_in_container.startswith("/tc-fixtures/"):
        return str(fixtures_dir / path_in_container.removeprefix("/tc-fixtures/"))
    return path_in_container


def checker_command_for_spec(
    spec_path: str,
    *,
    input_topics_path: str,
    mqtt_port: int,
    output_topics_path: str | None,
) -> list[str]:
    command = [
        spec_path,
        "--input-mqtt-file",
        input_topics_path,
    ]
    if output_topics_path is None:
        command.append("--mqtt-output")
    else:
        command.extend(["--output-mqtt-file", output_topics_path])
    command.extend(["--mqtt-port", str(mqtt_port)])
    return command


def await_checker_ready(trustworthiness_checker_process: object) -> None:
    deadline = time.time() + 8.0

    while time.time() < deadline:
        if isinstance(trustworthiness_checker_process, DockerContainer):
            wrapped = trustworthiness_checker_process.get_wrapped_container()
            wrapped.reload()
            if wrapped.status != "running":
                logs = trustworthiness_checker_process.get_logs()
                assert False, (
                    "Trustworthiness Checker container exited before tests ran. "
                    f"Logs: {logs}"
                )
        elif isinstance(trustworthiness_checker_process, subprocess.Popen):
            if trustworthiness_checker_process.poll() is not None:
                assert False, (
                    "Trustworthiness Checker local process exited before tests ran"
                )

        time.sleep(0.1)

    time.sleep(0.5)


def start_checker_for_spec(
    *,
    spec_path_in_container: str,
    input_topics_path_in_container: str,
    output_topics_path_in_container: str | None,
    fixtures_dir: Path,
    broker_id: str,
    broker_port: int,
    containers: list[DockerContainer],
    processes: list[subprocess.Popen[str]],
) -> object:
    if TRUSTWORTHINESS_CHECKER_BINARY:
        command = [
            TRUSTWORTHINESS_CHECKER_BINARY,
            *checker_command_for_spec(
                in_container_to_host_path(spec_path_in_container, fixtures_dir),
                input_topics_path=in_container_to_host_path(
                    input_topics_path_in_container,
                    fixtures_dir,
                ),
                mqtt_port=broker_port,
                output_topics_path=(
                    None
                    if output_topics_path_in_container is None
                    else in_container_to_host_path(
                        output_topics_path_in_container,
                        fixtures_dir,
                    )
                ),
            ),
        ]
        process = subprocess.Popen(command, text=True)
        processes.append(process)
        return process

    command = " ".join(
        checker_command_for_spec(
            spec_path_in_container,
            input_topics_path=input_topics_path_in_container,
            mqtt_port=1883,
            output_topics_path=output_topics_path_in_container,
        )
    )

    container = DockerContainer(TRUSTWORTHINESS_CHECKER_IMAGE)
    container = container.with_kwargs(network_mode=f"container:{broker_id}")
    container = container.with_env("RUST_LOG", "warn")
    container = container.with_volume_mapping(
        str(fixtures_dir), "/tc-fixtures", mode="ro"
    )
    container = container.with_command(command)
    container = container.start()

    containers.append(container)
    return container


def stop_checker_processes_and_containers(
    *,
    processes: list[subprocess.Popen[str]],
    containers: list[DockerContainer],
) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
            try:
                _ = process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                _ = process.wait(timeout=5)
    for container in containers:
        container.stop()


@pytest.fixture(scope="module")
def tc_mqtt_broker_container(request: pytest.FixtureRequest) -> DockerContainer:
    container = DockerContainer("eclipse-mosquitto:latest")
    container = container.with_exposed_ports(1883)
    container = container.waiting_for(
        LogMessageWaitStrategy(re.compile(r"mosquitto version \d+\.\d+\.\d+ running"))
    )
    container = container.start()

    def stop_container() -> None:
        container.stop()

    request.addfinalizer(stop_container)
    return container


@pytest.fixture(scope="module")
def mqtt_broker(tc_mqtt_broker_container: DockerContainer) -> tuple[str, int]:
    return tc_mqtt_broker_container.get_container_host_ip(), int(
        tc_mqtt_broker_container.get_exposed_port(1883)
    )


@pytest.fixture
def trustworthiness_checker_container_factory(
    request: pytest.FixtureRequest,
    tc_mqtt_broker_container: DockerContainer,
    mqtt_broker: tuple[str, int],
) -> Callable[[str, str, str | None], object]:
    fixtures_dir = Path("tests/fixtures/trustworthiness_checker").resolve()
    broker_id = tc_mqtt_broker_container.get_wrapped_container().id
    assert broker_id is not None
    _, broker_port = mqtt_broker

    containers: list[DockerContainer] = []
    processes: list[subprocess.Popen[str]] = []

    def create_for_spec(
        spec_path_in_container: str,
        input_topics_path_in_container: str,
        output_topics_path_in_container: str | None,
    ) -> object:
        return start_checker_for_spec(
            spec_path_in_container=spec_path_in_container,
            input_topics_path_in_container=input_topics_path_in_container,
            output_topics_path_in_container=output_topics_path_in_container,
            fixtures_dir=fixtures_dir,
            broker_id=broker_id,
            broker_port=broker_port,
            containers=containers,
            processes=processes,
        )

    def cleanup() -> None:
        stop_checker_processes_and_containers(
            processes=processes,
            containers=containers,
        )

    request.addfinalizer(cleanup)
    return create_for_spec
