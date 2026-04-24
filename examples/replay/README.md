# Replay example

This group focuses on Kitaru's replay model: keep earlier durable work and rerun
only the downstream branch affected by your replay roots.

## Getting started

```bash
cd examples/replay
uv pip install 'kitaru[local]'   # Install Kitaru with local runtime
kitaru init                      # Initialize a Kitaru project in this directory
```

Then run:

```bash
python replay_with_overrides.py
```

This example uses your current Kitaru connection context. If you want replay to
run against a deployed Kitaru server, connect first with `kitaru login
<server>` and verify with `kitaru status`.

For the full catalog, see [../README.md](../README.md).

## `replay_with_overrides.py` — Replay from a checkpoint with modified inputs

Runs a three-step content pipeline (research → write draft → publish), then
replays from `write_draft` while swapping the research checkpoint's cached
output for edited notes. In this setup, the override is applied at
`write_draft` (the direct consumer), and replay continues downstream from
there. Checkpoints before that branch return cached results — no work wasted
re-running `research`. Only `write_draft` and `publish` re-execute with the
new input.

This is the core value of durable execution: fix a mistake at step 3 without
paying for steps 1 and 2 again.
