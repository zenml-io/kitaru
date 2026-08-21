# DABstep coding-agent V0

This is an experimental, local V0 for demonstrating a coding agent answering one DABstep development question from a stable filesystem. It creates a fresh temporary agent workdir for every run, retains the Codex JSONL and answer after the workdir is removed, scores the answer outside the agent-visible directory, and writes one portable Kitaru JSONL session for import.

It is not a faithful replay adapter or a benchmark claim. DABstep v1 development answers are public and may be contaminated. A context-free success disqualifies that task from the filesystem-grounded demo; a context-free failure does not establish that it is clean. One baseline/candidate pair is illustrative only.

## Prerequisites

- A working `codex` CLI login. The wrapper invokes `codex exec` non-interactively and makes real model calls.
- A Kitaru server and an existing agent or agent version for the final import. The wrapper only produces portable Kitaru JSONL, so it does not need server credentials itself.
- About 25 MB for the public DABstep context and additional local space for raw traces under `.cache/dabstep-coding-agent/`. Treat that directory as restricted: raw traces may contain local paths or other sensitive content.

## Run V0

From the repository root, prepare the preferred hard development task. The default is task `1273`; use `1305` as the pre-nominated backup if the canary disqualifies the preferred task.

```bash
uv run python -m examples.python.dabstep_coding_agent.prepare \
  --destination .cache/dabstep-coding-agent/task-1273 \
  --task-id 1273
```

The command writes the agent-visible fixture under `public/` and the known answer under `private/gold.json`. Never copy the private directory into the agent workspace or share it with the agent.

Run the no-model scorer preflight first. The oracle must pass and the deliberately wrong control must fail.

```bash
uv run python -m examples.python.dabstep_coding_agent.runner preflight \
  --gold .cache/dabstep-coding-agent/task-1273/private/gold.json
```

Then run the context-free canary. It gives Codex the question and skill, but not the data directory. It still forces an outbound-connectivity probe inside a Codex sandbox whose command network access is explicitly disabled.

```bash
uv run python -m examples.python.dabstep_coding_agent.runner run \
  --fixture .cache/dabstep-coding-agent/task-1273/public \
  --gold .cache/dabstep-coding-agent/task-1273/private/gold.json \
  --skill examples/python/dabstep_coding_agent/skills/analysis-a.md \
  --artifacts .cache/dabstep-coding-agent/runs \
  --context-free
```

If `score.passed` is `true`, do not use task 1273 for the filesystem-grounded demo. Repeat preparation and the canary with task 1305. If that canary also scores correctly, stop and nominate another task before proceeding.

For an eligible task, run the baseline with the public context. Every Codex invocation supplies `--ignore-user-config`, `--sandbox workspace-write`, and `-c sandbox_workspace_write.network_access=false`, so the user's normal Codex network setting cannot leak into the agent-command sandbox. The wrapper refuses to accept a run unless its separate sandboxed connectivity probe records a failure.

```bash
uv run python -m examples.python.dabstep_coding_agent.runner run \
  --fixture .cache/dabstep-coding-agent/task-1273/public \
  --gold .cache/dabstep-coding-agent/task-1273/private/gold.json \
  --skill examples/python/dabstep_coding_agent/skills/analysis-a.md \
  --artifacts .cache/dabstep-coding-agent/runs
```

The printed `run_dir` includes `agent.codex.jsonl`, `network-probe.codex.jsonl`, `answer.txt`, `score.json`, and `kitaru-session.jsonl`. The wrapper adds `_kitaru_observed_at` to each captured Codex record so Kitaru can order the visible steps and records the exact wrapper prompt and skill text as inputs to a root `Codex agent run` span. That span also carries the task execution's token totals reported by Codex and, for GPT-5.4, an explicitly labeled base-rate API-equivalent cost estimate based on a fixed `genai-prices` data snapshot. It excludes the separate network-sandbox preflight, may understate long-context requests, and is not an observed Codex charge. Review the raw trace locally for credentials and sensitive paths before sharing. The portable session performs a narrow path/token redaction and records its redaction and fidelity gaps in metadata, but that is not a general secret-scanning policy.

Import the portable session using the built-in Kitaru JSONL importer. Replace the agent value with an existing Kitaru agent or version.

```bash
uv run kitaru session import \
  .cache/dabstep-coding-agent/runs/<run-id>/kitaru-session.jsonl \
  --importer kitaru/kitaru-jsonl@latest \
  --agent <your-agent>@latest \
  --media-type application/x-ndjson \
  --wait
```

Inspect the imported session in the current UI. The V0 converter represents the coding-agent invocation as a root span, visible Codex messages as LLM nodes, and visible shell or custom calls as tool nodes. The Codex system prompt remains unavailable because `codex exec --json` does not expose it. The converter cannot reconstruct the full workspace, hidden reasoning, or a faithful mid-trajectory continuation.

## The skill-B rerun

Do not author skill B up front. First inspect the imported skill-A trace and name the one node that shows a real failure, inefficient detour, or decision point. Copy `skills/analysis-a.md` to a separately named skill-B file, add the targeted instruction, and record the baseline node ID, rationale, and predicted behavior change in the file header. Rerun the exact same fixture and model with that skill file. The task fixture, scorer, and source data must stay unchanged.

Present the two run directories and scores as an illustrative comparison, not evidence that the skill reliably improves performance. Record missing trace detail or UI friction for V1 rather than expanding this example during a colleague demo.

## Five-task cohort prototype

After the one-task loop works, prepare hard development tasks `1273`, `1305`, `1464`, `1681`, and `1753`. Treat the first three as development tasks and the final two as reserved checks. The tasks and answers are public, so this remains an illustrative workflow rather than an uncontaminated benchmark.

The `task-run` command can select one prepared fixture from the recorded Kitaru task input:

```bash
uv run python -m examples.python.dabstep_coding_agent.runner task-run \
  --fixtures-root .cache/dabstep-coding-agent \
  --skill examples/python/dabstep_coding_agent/skills/analysis-c.md \
  --artifacts .cache/dabstep-coding-agent/runs \
  --model gpt-5.4
```

For task ID `1305`, it reads `.cache/dabstep-coding-agent/task-1305/public` and scores against `.cache/dabstep-coding-agent/task-1305/private/gold.json`. It rejects non-numeric task IDs, checks that the recorded question matches the prepared fixture, and never copies the private directory into the agent workdir.

Create one Skill A baseline session per task, freeze those five session IDs as one cohort version, and register one agent version whose command uses `--fixtures-root` with `analysis-c.md`. Start one experiment run against that cohort with baseline evaluation enabled. The experiment should use the multi-task correctness evaluator, `kitaru/output-contract` with `required_paths` set to `["/answer"]`, `kitaru/latency`, and `kitaru/tool-health`.

The first completed five-task prototype improved correctness from 3/5 under Skill A to 4/5 under Skill C. It fixed task `1305`, preserved the three passing baselines, and did not fix task `1273`. Total latency fell by about 17% and the API-equivalent cost estimate fell by about 25.5%, while failed command calls rose from 3 to 5. These mixed results are useful investigation evidence, not a reliable improvement claim; repeated fresh runs are still required before making one.
