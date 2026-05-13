---
name: kitaru-replay-lab-compare
description: >-
  Run or guide the local Replay Lab comparison using a manifest, a candidate
  descriptor, and the kitaru_replay_lab_compare MCP tool. Use when the user is
  ready to compare observed production, baseline replay, and candidate replay.
---

# Kitaru Replay Lab: Compare

Use this skill when a cohort manifest and candidate descriptor exist and the user wants to run the Replay Lab comparison.

## Prototype location

Main demo directory:

- `examples/end_to_end/replay_lab/`

Common inputs:

- Manifest: `examples/end_to_end/replay_lab/manifests/*.json`
- Candidate descriptor: `examples/end_to_end/replay_lab/candidates/cheaper_support_agent.json`

Common outputs:

- Reports: `examples/end_to_end/replay_lab/reports/`

Backend entrypoint:

- MCP tool: `kitaru_replay_lab_compare`

## What the compare step does

For each case in the manifest, it gathers or creates three lanes:

1. **Observed production** — fetch the original execution.
2. **Baseline replay** — replay the source execution with no candidate changes.
3. **Candidate replay** — replay the same source execution with the candidate descriptor applied.

Then it writes structured report data and rendered report files.

## Preferred flow

1. Read `examples/end_to_end/replay_lab/README.md` so commands match the repo.
2. Confirm the manifest path under `examples/end_to_end/replay_lab/manifests/`.
3. Confirm the candidate path under `examples/end_to_end/replay_lab/candidates/`.
4. Prefer the `kitaru_replay_lab_compare` MCP tool when available.
5. If the user is running the local demo from the terminal, guide them through `examples/end_to_end/replay_lab/run_replay_lab.py` as documented in the README.
6. After comparison finishes, list the generated files in `examples/end_to_end/replay_lab/reports/`.

## Candidate descriptor shape

The v0 candidate descriptor may include:

- `label` — human-readable candidate name;
- `flow_inputs` — replay flow input changes;
- `checkpoint_overrides` — supported checkpoint replay overrides;
- `notes` — optional explanation for the report.

Do not assume deployment or version selectors are required in v0.

## How to explain results while running

Use this language:

- Replay drift = baseline replay compared with observed production.
- Candidate effect = candidate replay compared with baseline replay.

If observed and baseline are far apart, say so clearly. The comparison is less trustworthy because replay itself changed the measurement.

## Guardrails

- Do not manually orchestrate many individual `kitaru_executions_replay` calls if `kitaru_replay_lab_compare` can do the cohort comparison.
- Do not claim the candidate improved just because observed production was worse. Check candidate replay against baseline replay.
- Do not modify backend, demo, smoke-test, examples, `../kitaru-skills`, or plugin metadata unless explicitly asked.
