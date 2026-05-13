---
name: kitaru-replay-lab-investigate
description: >-
  Inspect Kitaru execution data for the local Replay Lab prototype and help
  choose a useful cohort manifest under examples/end_to_end/replay_lab/manifests/.
  Use when the user has suspicious executions, production-like signals, or wants
  to seed/propose a Replay Lab cohort.
---

# Kitaru Replay Lab: Investigate

Use this skill to turn a problem signal into a small Replay Lab cohort.

The goal is not to prove anything yet. The goal is to freeze a useful set of cases so Replay Lab can replay them consistently.

## Prototype location

Use the local demo files under:

- `examples/end_to_end/replay_lab/`

Relevant files and folders:

- `examples/end_to_end/replay_lab/README.md` — current demo instructions.
- `examples/end_to_end/replay_lab/seed_observed.py` — creates deterministic observed executions for the demo.
- `examples/end_to_end/replay_lab/scenarios.py` — synthetic cases and problem signals.
- `examples/end_to_end/replay_lab/manifests/` — generated cohort manifests.

## What to look for

A good v0 cohort has a small number of concrete cases with clear reasons, for example:

- cost spike;
- quality drop;
- customer complaint;
- changed output;
- latency spike;
- tool failure;
- deployment or prompt change that feels worse.

Prefer specific case reasons over abstract labels. For example: "expensive refund case with weak answer" is better than "bad quality".

## Investigation steps

1. Read `examples/end_to_end/replay_lab/README.md` to confirm the current seed and run commands.
2. If the user wants the built-in demo, use `seed_observed.py` as the source of observed executions and generated manifest data.
3. If the user provides execution IDs, inspect them with existing Kitaru execution tools before proposing a cohort.
4. For each selected case, record:
   - case ID;
   - observed execution ID;
   - reason for selection;
   - replay checkpoint selector;
   - expected artifacts, normally `scorecard` and `final_response`;
   - optional labels such as customer tier, topic, or deployment marker.
5. Write or point to the manifest under `examples/end_to_end/replay_lab/manifests/`.

## Manifest expectations

The manifest is the v0 test set. It should identify observed executions and explain why each case belongs in the comparison.

Keep it practical and readable. The compare step will use it to run:

- observed production inspection;
- baseline replay;
- candidate replay.

## Guardrails

- Do not call the cohort a permanent pack or registry object. In this prototype it is just a manifest.
- Do not overfit the cohort to make the candidate look good. Include at least one case where the candidate might be risky if the demo data supports it.
- Do not edit `../kitaru-skills` or plugin metadata from this local prototype skill.
