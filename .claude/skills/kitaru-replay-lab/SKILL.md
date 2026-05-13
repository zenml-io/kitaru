---
name: kitaru-replay-lab
description: >-
  Drive the local Kitaru Replay Lab prototype workflow: choose a cohort of
  observed executions, run baseline and candidate replays, compare the three
  lanes, and explain the resulting reports. Use when the user asks to run,
  demo, understand, or iterate on Replay Lab in this repo.
---

# Kitaru Replay Lab

Use this parent skill for the local Replay Lab prototype in the main Kitaru repo.

Replay Lab answers one concrete question:

> We noticed some production-like executions that look worse than expected. If we replay the same cases and try a candidate change, did the candidate actually help?

## Mental model

Replay Lab compares three lanes for each case:

1. **Observed production** — what happened in the original execution.
2. **Baseline replay** — the same execution replayed from the chosen checkpoint with no candidate changes.
3. **Candidate replay** — the same execution replayed from the same checkpoint with the candidate descriptor applied.

Tell the story plainly:

> Observed production tells us what happened. Baseline replay tells us what replay changes by itself. Candidate replay tells us what the candidate changed beyond replay effects.

The important safety check is the baseline lane. If production cost was high, but baseline replay is already cheap before any candidate change, then replay changed the measurement. Do not give the candidate credit for that.

## Local prototype files

The runnable demo lives here:

- `examples/end_to_end/replay_lab/README.md` — user-facing walkthrough.
- `examples/end_to_end/replay_lab/seed_observed.py` — seeds observed executions and writes a generated manifest.
- `examples/end_to_end/replay_lab/run_replay_lab.py` — runs the comparison.
- `examples/end_to_end/replay_lab/render_report.py` — renders static HTML from the JSON report.
- `examples/end_to_end/replay_lab/candidates/cheaper_support_agent.json` — sample candidate descriptor.
- `examples/end_to_end/replay_lab/manifests/` — generated cohort manifests.
- `examples/end_to_end/replay_lab/reports/` — generated JSON, Markdown, and HTML reports.

Backend entrypoint:

- MCP tool: `kitaru_replay_lab_compare`.

## Route to sub-skills

Use the narrow sub-skill that matches the user's current step:

- Use `kitaru-replay-lab-investigate` when the user has executions or a problem signal and needs a cohort manifest.
- Use `kitaru-replay-lab-compare` when the manifest and candidate descriptor exist and the user wants to run the comparison.
- Use `kitaru-replay-lab-report` when reports exist and the user wants help reading, explaining, or polishing them.

## Default workflow

1. Read `examples/end_to_end/replay_lab/README.md` first so your commands match the current demo.
2. If there is no manifest yet, use the investigate path or run the seed script described in the README.
3. Confirm the candidate descriptor path, usually `examples/end_to_end/replay_lab/candidates/cheaper_support_agent.json`.
4. Run or guide the `kitaru_replay_lab_compare` flow through MCP or the local demo script.
5. Read the JSON/Markdown report in `examples/end_to_end/replay_lab/reports/`.
6. Explain the result in terms of replay drift and candidate effect.

## Guardrails

- Do not describe baseline replay as a new replay mode. It is normal replay with no candidate changes.
- Do not treat observed-vs-candidate differences as the main proof. The safer comparison is candidate replay vs baseline replay.
- Do not modify backend, examples, smoke tests, or plugin metadata unless the user explicitly asks or a tiny typo blocks skill accuracy.
- Keep this local prototype separate from `../kitaru-skills` until the workflow language has stabilized.
