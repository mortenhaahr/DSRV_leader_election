# AGENTS.md — RAFT Leader Election Case Study Guide

This repository develops a **simple, academically credible distributed leader-election prototype using RAFT**.
Primary goal: implement and evaluate RAFT leader election behavior (not full log replication) in a way suitable for a conference case study.

## Documentation
- a simple introduction to the project is is README.md

## Packaging
- The project uses uv for package management.

## Change validation
- The program is executed by running `uv run python -m dsrv_leader_election.main`.

- The test suite uses `pytest`.
- By default, tests marked `mqtt` and `simulations` are skipped.
- Run tests using the following commands:
  1. Fast/default checks: `uv run pytest tests -q`
  2. Include simulation-sequence tests: `uv run -m pytest tests -q --test-simulations`
  3. Include MQTT tests: `uv run -m pytest tests -q --test-mqtt`
  4. Run everything: `uv run -m pytest tests -q --all-tests`

- MQTT tests require Docker/testcontainers.
- Simulation tests use dedicated fixtures in `tests/fixtures/` rather than `example_configs/`.
- Validate every suggested patch with the most appropriate pytest command(s) above.
- Also ensure that types are used consistently throughout the codebase, and validate patches using type checking / diagnostics.
