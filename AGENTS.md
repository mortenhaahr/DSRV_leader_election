# AGENTS.md — RAFT Leader Election Case Study Guide

This repository develops a **simple, academically credible distributed leader-election prototype using RAFT**.
Primary goal: implement and evaluate RAFT leader election behavior (not full log replication) in a way suitable for a conference case study.

## Change validation
- The program is executed by running `python -m src.main`.
- There are limited tests so validation is carried out by:
    1. Ensuring no runtime errors.
    2. Validating that the outputs seems sensible.
- Tests can be ran by executing `python -m unittest discover -s tests -p 'test*.py' -v`.
- Validate every suggested patch.
