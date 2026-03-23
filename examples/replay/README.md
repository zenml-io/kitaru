# Replay example

This group focuses on Kitaru's replay model: keep earlier durable work and rerun
only the suffix you care about.

## Getting started

```bash
cd examples/replay
uv sync --extra local       # Install dependencies (from repo root, or use pip)
kitaru init                  # Initialize a Kitaru project in this directory
python replay_with_overrides.py
```

This example uses your current Kitaru connection context. If you want replay to
run against a deployed Kitaru server, connect first with `kitaru login
<server>` and verify with `kitaru status`.

For the full catalog, see [../README.md](../README.md).

## `replay_with_overrides.py` — Replay from a checkpoint with modified inputs

Runs a three-step content pipeline (research → write draft → publish), then
replays from `write_draft` while swapping the research checkpoint's cached
output for edited notes. Checkpoints before the replay point return their
cached results — no tokens wasted re-running `research`. Only `write_draft`
and `publish` re-execute with the new input.

This is the core value of durable execution: fix a mistake at step 3 without
paying for steps 1 and 2 again.
