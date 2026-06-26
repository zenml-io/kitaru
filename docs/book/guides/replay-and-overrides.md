---
description: Reproduce a real execution from a checkpoint, replay it with flow, checkpoint, and invocation overrides, and diff the two.
icon: rotate-left
---

# Replay and Overrides

Replay re-executes a real, recorded run from a checkpoint to produce a **new execution** — the same run, with the changes you specify. This is what makes Kitaru more than tracing: because every model call and tool call was recorded as a durable checkpoint, you can reproduce a run faithfully, fork it with one input changed, and attribute the difference to your change rather than to replay noise.

The practical promise is simple: you keep the work that already succeeded, then rerun only the part you want to test or recover. If a flow researched a customer, called three tools, then failed while writing the final answer, replay can reuse the recorded research and tool results instead of paying for them again.

The core loop is three steps:

1. **Reproduce** — replay a run with nothing changed. A faithful rerun is your control/baseline.
2. **Fork** — replay the same run again with exactly one input overridden (a different model, a different prompt).
3. **Diff** — compare the two new executions. Because the baseline reproduced, the delta is your change.

This is not re-scoring stored outputs like an eval. Replay re-executes the real run from a checkpoint forward, with one input swapped.

## The three-way trust idea

Replay gives you three comparable executions:

- **Observed** — the original run as it happened, exactly as recorded.
- **Reproduced** — a replay with no overrides. If it matches the observed run, replay is faithful and you can trust the next step.
- **Forked** — a replay with one input changed.

The reproduced run validates the harness; the forked run isolates the effect of your change. Always confirm the reproduced run matches before trusting a forked diff.

## What replay does, step by step

When you replay an execution, Kitaru follows this sequence:

1. Kitaru loads the source execution and its recorded checkpoint calls.
2. Kitaru resolves `at` to one recorded checkpoint invocation, tool call, or model call. If the selector points to several calls, replay fails and asks for a more specific selector.
3. Checkpoints before `at` reuse their recorded outputs. Their Python functions do not run again.
4. The checkpoint at `at`, and checkpoints that depend on it, run again unless an override changes that behavior.
5. An override with an `"output"` value injects that value and does not run the targeted checkpoint.
6. An override with a `"code"` value imports a replacement callable before the checkpoint reruns.
7. An override with a `"model"` value changes the model only for supported LLM checkpoint calls. If the target is not a supported LLM checkpoint, Kitaru fails validation before submission.

So the replay is not a mutation of the old execution. The source execution stays as evidence. The replay is a new execution with metadata that points back to the source.

## The three override levels

Replay overrides are grouped by what they target.

| Override level | What it targets | When to use it |
|---|---|---|
| `flow_overrides` / `--flow-overrides` | Flow parameters for the replay run | Change top-level inputs such as `topic`, `customer_id`, or `prompt_profile`. |
| `checkpoint_overrides` / `--checkpoint-overrides` | Every invocation of a checkpoint name | Change all recorded calls named `lookup_policy_tool` or `draft_answer`. |
| `invocation_overrides` / `--invocation-overrides` | One recorded checkpoint, tool, or model call | Change exactly one call when the same checkpoint ran multiple times. |

Checkpoint and invocation overrides can contain these fields:

| Field | What literally happens |
|---|---|
| `"input"` | Kitaru replaces the target checkpoint's inputs, then reruns that checkpoint and the checkpoints that depend on it. |
| `"output"` | Kitaru injects the provided value as the target checkpoint output. The targeted checkpoint does not run. Downstream checkpoints consume the injected value. |
| `"code"` | Kitaru imports the replacement callable and uses it when the target checkpoint reruns. The callable must accept the same inputs as the original. |
| `"model"` | Kitaru changes the model for the targeted LLM checkpoint call. Unsupported targets fail before replay starts. |

If a checkpoint override and an invocation override both target the same recorded call, the invocation override wins because it is more specific.

## Reproduce a run

Replay with no overrides to get a faithful rerun. Use this as your baseline:

```python
import kitaru

client = kitaru.KitaruClient()

baseline = client.executions.replay("kr-a8f3c2", at="write_draft")

row = baseline.results[0]
print(row.replay_exec_id)
print(row.original_exec_id)  # points to the source execution
```

## Replay with one overridden input

Override exactly one thing — a flow input, a checkpoint output, a single model call — while reproducing everything else. The forked run then differs from the baseline only by that change.

Replay one execution with flow and checkpoint overrides:

```python
submission = client.executions.replay(
    "kr-a8f3c2",
    at="write_draft",
    flow_overrides={"topic": "New topic"},
    checkpoint_overrides={
        "research": {"output": "Edited notes"},
    },
)

row = submission.results[0]
print(row.replay_exec_id)
print(row.original_exec_id)  # points to the source execution
print(row.compare_url)
```

Replay one exact recorded invocation:

```python
submission = client.executions.replay(
    "kr-a8f3c2",
    at="support_copilot_model_request_2",
    invocation_overrides={
        "support_copilot_model_request_2": {
            "model": "openai:gpt-5-nano",
        },
    },
)
```

Use invocation IDs or call IDs when a checkpoint name appears more than once in an execution. For example, if `lookup_policy_tool` ran three times, a checkpoint override changes all three calls. An invocation override changes only the one ID you name.

## Flow-object replay

If you have the flow object available, you can replay through it directly. This returns the same `ReplaySubmission` model as the client API.

```python
submission = content_pipeline.replay(
    "kr-a8f3c2",
    at="write_draft",
    flow_overrides={"topic": "New topic"},
    checkpoint_overrides={
        "research": {"output": "Edited notes"},
    },
)

print(submission.summary.to_json())
```

A single execution waits by default. A multi-execution replay submits by default and returns after submission unless you pass `wait=True`.

## Multi-execution replay

Pass several explicit execution IDs to replay the same plan across several source executions:

```python
submission = client.executions.replay(
    ["kr-a", "kr-b", "kr-c"],
    at="write_draft",
    invocation_overrides={
        "write_draft:model_call_1": {"model": "openai:gpt-5-nano"},
    },
    tag="best-replay-june",
    wait=True,
    on_error="collect",
)

for row in submission.results:
    print(row.original_exec_id, "->", row.replay_exec_id, row.status)

for row in submission.skipped:
    print("skipped", row.original_exec_ref, row.reason)

for row in submission.failures:
    print("failed", row.original_exec_ref, row.reason)
```

`on_error="collect"` is useful for batches. If one source execution does not contain the `at` checkpoint, Kitaru records that parent in `submission.skipped` and keeps submitting the others. `on_error="fail"` stops at the first error.

Cohort selection stays separate from replay. First resolve the executions you want, inspect the list, then pass the explicit IDs to `replay`:

```python
cohort = client.executions.cohort(
    flow="content_pipeline",
    at="write_draft",
    order_by="-display_cost_usd",
    limit=10,
).resolve()

submission = client.executions.replay(
    cohort.exec_ids,
    at="write_draft",
    flow_overrides={"model": "openai:gpt-5-nano"},
    wait=True,
    on_error="collect",
)
```

## CLI replay

The CLI mirrors the SDK. Pass overrides as JSON blobs.

Replay one execution:

```bash
# Reproduce (baseline)
kitaru executions replay kr-a8f3c2 --at write_draft

# Fork with flow-input overrides
kitaru executions replay kr-a8f3c2 \
  --at write_draft \
  --flow-overrides '{"model":"gpt-4o","prompt_profile":"concise"}'

# Override a recorded checkpoint output
kitaru executions replay kr-a8f3c2 \
  --at write_draft \
  --flow-overrides '{"topic":"New topic"}' \
  --checkpoint-overrides '{"research":{"output":"Edited notes"}}'
```

Replay several explicit executions:

```bash
kitaru executions replay kr-a kr-b kr-c \
  --at write_draft \
  --invocation-overrides '{"write_draft:model_call_1":{"model":"openai:gpt-5-nano"}}' \
  --tag best-replay-june \
  --wait \
  --on-error collect \
  --output json
```

You can also pass IDs through a JSON file:

```bash
kitaru executions replay \
  --ids-file replay-parents.json \
  --at write_draft \
  --checkpoint-overrides '{"lookup_policy_tool":{"code":"mocks.lookup_policy"}}'
```

The file may contain either a JSON list:

```json
["kr-a", "kr-b", "kr-c"]
```

or an object with an `exec_ids` list:

```json
{"exec_ids": ["kr-a", "kr-b", "kr-c"]}
```

## Choosing `at`

`at` tells Kitaru where the recorded execution stops being reused and where the replay starts doing work again.

`at` can be:

- a checkpoint name, if that name identifies exactly one recorded invocation;
- a checkpoint invocation ID;
- a checkpoint call ID.

If the selector is missing or ambiguous, replay fails clearly before submitting the new execution.

`at` always contributes one replay root. Each checkpoint override adds replay roots at the direct consumers of that checkpoint's output. Any checkpoint that is neither a replay root nor a downstream dependency is reused from the recorded run, which is what keeps the reproduced run faithful.

Concrete example:

```text
research → lookup_policy_tool → write_draft → publish_answer
```

If you replay with `at="write_draft"`, Kitaru reuses the recorded `research` and `lookup_policy_tool` outputs. Then it runs `write_draft` and `publish_answer` again.

If you add this checkpoint override:

```json
{"write_draft": {"output": "Use this final draft."}}
```

Kitaru injects `"Use this final draft."` as the `write_draft` result. It does not call `write_draft`. `publish_answer` then runs and receives the injected draft.

## Common override patterns

### Change flow parameters

Use flow overrides for top-level flow arguments:

```bash
kitaru executions replay kr-a8f3c2 \
  --at write_draft \
  --flow-overrides '{"topic":"refund policy","prompt_profile":"strict"}'
```

### Replace all calls to a checkpoint name

Use a checkpoint override when the checkpoint name is the thing you mean:

```python
submission = client.executions.replay(
    "kr-a8f3c2",
    at="lookup_policy_tool",
    checkpoint_overrides={
        "lookup_policy_tool": {"code": "mocks.lookup_policy"},
    },
)
```

If `lookup_policy_tool` appears multiple times in the source execution, Kitaru applies the replacement callable to each matching recorded invocation.

### Replace one recorded call

Use an invocation override when the same checkpoint ran several times and you only want one of them:

```python
submission = client.executions.replay(
    "kr-a8f3c2",
    at="lookup_policy_tool_2",
    invocation_overrides={
        "lookup_policy_tool_2": {
            "output": {"policy": "manual approval required"},
        },
    },
)
```

### Force a recorded result to be reused

Use `skip` when `at` would normally make a checkpoint rerun, but you want that checkpoint to reuse its recorded output:

```bash
kitaru executions replay kr-a8f3c2 \
  --at lookup_policy_tool \
  --skip write_draft_call_1
```

In this example, Kitaru reruns from `lookup_policy_tool`, but reuses the recorded output for the later `write_draft_call_1` call instead of running it again.

You cannot both skip and override the same replay target. Kitaru fails validation rather than guessing which instruction you meant.

## Diff the two runs

Once you have a baseline and a forked execution, compare them to attribute the difference to your change.

Compare an original execution against one or more replays:

```python
execution_diff = kitaru.diff("kr-original")
execution_diff = kitaru.diff("kr-original", "kr-replay-a", "kr-replay-b")
```

When replay executions are omitted, `kitaru.diff` discovers all runs whose `original_exec_id` matches the source.

Diff many originals against their auto-discovered replays:

```python
matrix = kitaru.diff_matrix(["kr-original-a", "kr-original-b"])
for row in matrix.rows:
    print(row.original_exec_id, row.urls)
```

CLI:

```bash
kitaru executions diff kr-original kr-replay-a -o json
kitaru executions diff-matrix kr-a kr-b kr-c -o json
```

`ExecutionDiff.urls` links to the Kitaru UI compare view — one URL listing the original and every compared replay, whether discovered automatically or passed explicitly:

```text
{server}/flows/{flow_id}/v/{deployment_version}/compare?executions={id1},{id2},{id3}
```

When deployment metadata is absent on the execution, the version segment defaults to `local`.

Use `kitaru.build_compare_url_for_executions(...)`, `kitaru.compare_url_for_executions(...)`, or `kitaru.build_compare_url(...)` when constructing links yourself.

When the Kitaru frontend is hosted separately from the API server, set `KITARU_UI_URL` to the dashboard origin. Compare and execution links from `kitaru executions replay`, `kitaru.diff()`, and flow submission logs use that override; API traffic still uses `KITARU_SERVER_URL`.

{% hint style="info" %}
Cohort experiments — applying the same override across many recent runs and
ranking the results by cost, latency, or a quality judge — are a pattern you
build on top of replay, not a separate API: resolve a cohort of recent
executions, replay each with the same override, and aggregate the metrics
yourself.
{% endhint %}

## Waits during replay

Replay does not support overriding or pre-populating wait results. If a replayed execution reaches a `wait()` during normal execution, resolve it through the normal wait input flow:

- SDK: `client.executions.input(exec_id, wait="name", value=...)`
- CLI: `kitaru executions input <exec_id> --value ...`

## Divergence

Replay can raise `KitaruDivergenceError` when the backend detects that the replayed call sequence is no longer compatible with the source execution. A divergence means the run can no longer be reproduced faithfully, so any diff from it is untrustworthy.

A concrete example: the source execution recorded `research → write_draft → publish_answer`, but your current code now runs `research → classify_customer → write_draft → publish_answer`. Kitaru cannot safely pretend the old recorded values line up with the new call sequence, so it stops instead of producing a replay that looks valid but used the wrong checkpoint history.

Also check the replayed execution's failure metadata:

```python
latest = client.executions.get(submission.results[0].replay_exec_id)
if latest.failure:
    print(latest.failure.origin, latest.failure.message)
```

## Example in this repository

```bash
uv sync --extra local
uv run python examples/features/replay/replay_with_overrides.py
uv run pytest tests/test_phase16_replay_example.py
```

For the broader catalog, see [Examples](../getting-started/examples.md).

## Related blog posts

- [Agents need more than traces](https://kitaru.ai/blog/agents-need-more-than-traces)
- [Why agents need durable execution](https://kitaru.ai/blog/why-agents-need-durable-execution)
