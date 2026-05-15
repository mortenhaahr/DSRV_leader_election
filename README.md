## Setup

This project uses `uv` for environment and dependency management.

### Prerequisites

- Python `3.12+`
- `uv` installed

### Install `uv`

If `uv` is not installed yet:

- Linux/macOS: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Or follow the official instructions: https://docs.astral.sh/uv/getting-started/installation/

### Create environment and install dependencies

From the repository root:

```bash
uv sync --frozen
```


### Run the simulation

Use `uv run` so commands run in the managed environment:

```bash
uv run python -m dsrv_leader_election.main
```


### Run tests

By default, tests that require a live MQTT broker are skipped.

```bash
uv run pytest
```

To include MQTT integration tests, pass `--test-mqtt`:

```bash
uv run pytest --test-mqtt
```

Note that MQTT integration tests use `testcontainers` to launch isolated MQTT brokers and require a working Docker runtime.

## Running without MQTT logging


MQTT logging is optional. To run the simulation with standard console logging only, either omit `--event-logger` (default is `none`) or set it explicitly.

### Example run (no MQTT)

```bash
uv run python -m dsrv_leader_election.main
# or explicitly:
uv run python -m dsrv_leader_election.main --event-logger none
```



## MQTT event logging

You can emit RAFT simulation events to MQTT by enabling the MQTT event logger backend.


### CLI options

- `--event-logger`: Event logger backend (`none` or `mqtt`)
- `--mqtt-broker`: MQTT broker hostname/IP (default: `localhost`)
- `--mqtt-port`: MQTT broker port (default: `1883`)
- `--topic-mapping-json`: Path to the topic mapping JSON file

### Example run

```bash
uv run python -m dsrv_leader_election.main \
  --event-logger mqtt \
  --mqtt-broker localhost \
  --mqtt-port 1883 \
  --topic-mapping-json example_configs/event_topics/raft_event_topics.json
```



### Topic mapping JSON format

The topic mapping format is the same as that used by the trustworthiness
checker.

The mapping file is a JSON object where:
- each key is the emitted event var name
- each value is an object containing a `topic` field

```json
{
  "simulation_started": { "topic": "raft/simulation/started" },
  "node_role_transition": { "topic": "raft/node/role_transition" },
  "node_term_changed": { "topic": "raft/node/term_changed" },
  "leader_elected": { "topic": "raft/node/leader_elected" },
  "message_generated": { "topic": "raft/message/generated" },
  "message_delivered": { "topic": "raft/message/delivered" }
}
```

If an emitted var name is not found in the mapping file, the var name itself is used as the MQTT topic.

This is configured by the file `example_configs/event_topics/raft_event_topics.json`.


### Current emitted event var names

- `simulation_started`
- `simulation_tick`
- `simulation_finished`
- `node_initialized`
- `node_role_transition`
- `node_term_changed`
- `leader_elected`
- `message_generated`
- `message_scheduled`
- `message_delayed`
- `message_dropped`
- `message_delivered`
- `message_received`
