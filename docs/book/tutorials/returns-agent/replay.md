---
description: Register the stricter returns agent and replay the frozen target and control cohorts.
icon: rotate-left
---

# 4. Replay the changed agent

**Observe → Judge → Define → Replay → Compare**

This is the first phase that executes the agent. You will register the candidate code, make model credentials available to the worker, define how replayed tools behave, and run the target and control cohorts.

## Register the candidate agent version

The baseline agent assumes its action tools enforce refund limits. The candidate checks the policy before it chooses an action. The example selects this behavior with the `RETURNS_POLICY_MODE` environment variable.

Register the candidate as version 2:

```bash
uv run kitaru agent version register \
  returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
  --description "Check approval and risk rules before issuing a refund." \
  --display-version strict-policy-v2 \
  --working-dir ../.. \
  --env RETURNS_POLICY_MODE=strict \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Registration still does not run the agent. It creates `returns-resolver@2`, an immutable run specification for the candidate that the worker will execute. It does not snapshot the source tree at `--working-dir`, so a reproducible experiment also requires the worker to use the intended checkout, commit, or container image.

## Give the worker model credentials

Create `.env`, add a valid `OPENAI_API_KEY`, and export it in Terminal 1:

```bash
cp -n .env.example .env
# Edit .env before continuing.
set -a; source .env; set +a
```

In Terminal 2, stop the existing worker with `Ctrl-C`. Export the same file, then restart the worker:

```bash
set -a; source .env; set +a
uv run kitaru worker start --name returns-tutorial-worker
```

Restarting matters because the worker launches the registered agent as a subprocess. A process cannot inherit environment variables that were added to another terminal after it started.

The five replayed agent runs use `BASELINE_MODEL` from `.env`, which defaults to `openai:gpt-5-mini`. Each agent run may make more than one paid model request.

## Choose the replay tool policy

When agent code runs again, its tool calls need an explicit relationship to the outside world. Kitaru's tool policy determines whether each call uses recorded history, a supplied result, a mock, or the real tool.

This tutorial uses passthrough:

```json
{"default":{"type":"passthrough"},"tools":{}}
```

That is safe here because every action tool writes only to a new in-memory store created for the replay. Do **not** copy this choice for a tool that charges a card, sends a message, modifies production data, or triggers another irreversible action. For those tools, use recorded history or an explicit mock and fail when a safe result is unavailable. See [Replay and overrides](../../guides/replay-and-overrides.md) for the available policies.

{% hint style="warning" %} Replay safety comes from the configured tool policy and the tool implementations, not from the word “replay.” Review both before starting a run. {% endhint %}

## Create the experiment definition

An [**experiment**](../../concepts/experiments.md) is the reusable definition of the change and its measurements. An **experiment run** supplies one frozen cohort version and one agent version to execute.

Create the experiment:

```bash
uv run kitaru experiment create \
  improve-returns-policy \
  --agent returns-resolver \
  --description "Replay risky and valid refunds with strict approval rules." \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}' \
  --evaluator returns-policy@1 \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest
```

The experiment fixes three things:

- the agent whose behavior is being studied;
- the policy for replayed tool calls; and
- the evaluator versions applied to original and replayed sessions.

The cohort and candidate version are supplied when you start each run. This separation lets you reuse the same experiment against a new cohort version or a later candidate.

## Resolve the frozen cohort versions

The cohort names refer to evolving resources. Experiment runs require the immutable version IDs so their population cannot change underneath them:

```bash
TARGET_COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get unsafe-refund-baseline@1 \
  | jq -r '.item.id'
)"

CONTROL_COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get safe-refund-control@1 \
  | jq -r '.item.id'
)"
```

## Run the target and control

Replay the two known failures through the candidate:

```bash
uv run kitaru experiment run start \
  improve-returns-policy \
  --cohort-version "$TARGET_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Then replay the three valid refunds:

```bash
uv run kitaru experiment run start \
  improve-returns-policy \
  --cohort-version "$CONTROL_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Each command creates one **experiment run**. The run creates one replay per cohort session. The worker launches the registered candidate command for each replay, stores each new run as a session with `origin: replay`, and applies the experiment's evaluators.

`--evaluate-baselines` also applies the same evaluator versions to the imported sessions. Without it, you would have candidate evaluations but no like-for-like baseline. `--timeout-seconds 180` limits each agent subprocess; `--timeout 1800` limits how long the CLI waits for the entire cohort run.

If a run fails or times out, do not treat the completed subset as the experiment result. Read the run receipt, inspect its child jobs, and resolve the missing replay before drawing a comparison.

## Checkpoint

You now have:

- `returns-resolver@2`, the strict-policy candidate;
- `improve-returns-policy`, the reusable experiment definition;
- one experiment run over the target cohort; and
- one experiment run over the control cohort.

The worker has made paid model requests and created five replay sessions. Continue to [5. Compare the evidence](compare.md).
