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
5. An override with an `"input"` value changes the targeted checkpoint's recorded inputs, then reruns that checkpoint with those changed inputs.
6. An override with an `"output"` value injects that value and does not run the targeted checkpoint.
7. An override with a `"code"` value imports a replacement callable before the checkpoint reruns.
8. An override with a `"model"` value changes the model only for supported LLM checkpoint calls. If the target is not a supported LLM checkpoint, Kitaru fails validation before submission.

So the replay is not a mutation of the old execution. The source execution stays as evidence. The replay is a new execution with metadata that points back to the source.

## Replay runs your code again, side effects included

Step 4 above says the checkpoint at `at` and everything downstream "run again." Take that literally. "Run again" means your Python executes again, so any real-world action inside those checkpoints fires a second time. Kitaru cannot tell which lines touched the outside world; to Kitaru they are ordinary checkpoint code that the replay plan marked to run.

A concrete failure story:

```text
recorded run:           classify -> lookup_order -> issue_refund -> write_summary
replay at issue_refund: issue_refund runs again, then write_summary runs again
```

Suppose `issue_refund` refunds a customer's card and posts a message to Slack. Replaying at `issue_refund` refunds the card a second time and posts to Slack a second time. The customer sees two refunds and two notifications, and nothing in Kitaru stopped it. The same risk applies to any irreversible action inside a replayed checkpoint: paid API calls, emails, file writes, database mutations, and outbound webhooks.

This is the same double-effect risk the adapter guides call out for [LangGraph](../adapters/langgraph.md), the [Claude Agent SDK](../adapters/claude-agent-sdk.md), and [Gemini interactions](../adapters/gemini-interactions.md): when code reruns, its side effects rerun too.

### Guard side effects with `kitaru.is_replay()`

`kitaru.is_replay()` returns `True` when the current process is running inside a replay execution and `False` during a normal run. Use it to skip or stub the irreversible part of a checkpoint while letting the rest of the work run:

```python
import kitaru

@kitaru.checkpoint
def issue_refund(order: Order) -> RefundResult:
    if kitaru.is_replay():
        # Don't charge the card or post to Slack again during replay.
        return RefundResult(refunded=False, note="skipped during replay")
    return payment_api.refund(order.id)
```

This snippet is illustrative, not a complete production policy. Real flows often need more than an on/off switch. You might return a recorded refund ID so downstream checkpoints still receive a realistic value, or branch on which call is being replayed. For those finer decisions, `kitaru.get_replay_runtime_context()` returns the full `ReplayRuntimeContext` during replay (and `None` outside it).

## The three override levels

Replay overrides are grouped by what they target.

| Override level | What it targets | When to use it |
|---|---|---|
| `flow_overrides` / `--flow-overrides` | Flow parameters for the replay run | Change top-level inputs such as `topic`, `customer_id`, or `prompt_profile`. |
| `checkpoint_overrides` / `--checkpoint-overrides` | Every recorded call in a checkpoint name's family | Change all recorded calls named `lookup_policy_tool` or `draft_answer`. |
| `invocation_overrides` / `--invocation-overrides` | One recorded checkpoint, tool, or model call | Change exactly one call when the same checkpoint ran multiple times. |

Checkpoint and invocation overrides can contain these fields:

| Field | What literally happens |
|---|---|
| `"input"` | Kitaru replaces the target checkpoint's inputs, then reruns that checkpoint and the checkpoints that depend on it. |
| `"output"` | Kitaru injects the provided value as the target checkpoint output. The targeted checkpoint does not run. Downstream checkpoints consume the injected value. |
| `"code"` | Kitaru imports the replacement callable and uses it when the target checkpoint reruns. The callable must accept the same inputs as the original and be importable in the replay runtime. |
| `"model"` | Kitaru changes the model for the targeted LLM checkpoint call. Unsupported targets fail before replay starts. |

If a checkpoint override and an invocation override both target the same recorded call, the invocation override wins because it is more specific.

### Input override vs output override

`"input"` and `"output"` both change a replay, but they change it in opposite ways.

Use `"input"` when you want the checkpoint body to run again with changed inputs. A concrete story: the original run called `lookup_policy_tool` with `{"account_id": "acct-1"}`. You replay with an input override for `{"account_id": "acct-2"}`. Kitaru reruns `lookup_policy_tool`, the tool receives `acct-2`, and later checkpoints receive whatever that rerun returned. If the tool calls a paid API, writes a file, or sends a message, that action happens again unless your tool code guards it.

Use `"output"` when you do **not** want the checkpoint body to run. Kitaru pretends the checkpoint returned the value you supplied, then passes that value to later checkpoints. A concrete story: the original `lookup_policy_tool` returned `{"policy": "standard"}`. You replay with `{"output": {"policy": "manual approval required"}}`. Kitaru does not call the tool; it injects the edited result into the next checkpoint.

For PydanticAI tool checkpoints created by `checkpoint_strategy="calls"`, the replayable input is named `tool_args`, and the replayable result is named `output`. You still use the public `input` override field; `tool_args` is the recorded checkpoint input inside that field, not a new top-level override field.

PydanticAI model-request and whole-turn checkpoints may record their original inputs for inspection, but they do not currently accept edited `input` replay values. Use an `output` override for those checkpoints unless the checkpoint inspection output lists a replayable input slot.

These two forms are equivalent for a PydanticAI tool checkpoint whose only replayable input is `tool_args`:

```python
# Shorthand: Kitaru knows this tool checkpoint's input slot is `tool_args`.
checkpoint_overrides={
    "lookup_policy_tool": {
        "input": {"account_id": "acct-2"},
    },
}

# Explicit form: useful when you want to show the recorded input name.
checkpoint_overrides={
    "lookup_policy_tool": {
        "input": {"tool_args": {"account_id": "acct-2"}},
    },
}
```

If the checkpoint does not expose replayable inputs, Kitaru fails before starting replay instead of guessing. This matters for hand-written checkpoints with `type="tool_call"`: they are not treated as adapter-generated PydanticAI tool checkpoints unless they actually record a compatible replay input such as `tool_args`.

### How each override level matches calls

`flow_overrides` set top-level flow parameters for the whole replay run. `flow_overrides={"model": ...}` only changes the model when your flow actually exposes `model` as a top-level parameter that its checkpoints read. If the model is chosen inside a checkpoint or by an adapter, a flow override named `model` does nothing useful; target the LLM call with a checkpoint or invocation override instead.

`checkpoint_overrides` match a whole family of recorded calls that share a base name. Adapters generate repeated calls with numbered suffixes, so a single tool used three times records as `lookup_policy_tool`, `lookup_policy_tool_2`, `lookup_policy_tool_3`, and a model request sent three times records as `support_copilot_model_request`, `support_copilot_model_request_2`, `support_copilot_model_request_3`. A checkpoint override keyed by the base name (`lookup_policy_tool` or `support_copilot_model_request`) applies to every member of that family. A checkpoint override keyed by an exact suffixed name (`support_copilot_model_request_2`) stays exact and matches only that one call. Kitaru does not collapse arbitrary numbered names: a checkpoint you named `phase_2` is never folded into a `phase` family.

`invocation_overrides` are the precise path for one call. When the same checkpoint ran several times and you want to change exactly one of them, name that invocation ID, call ID, or suffixed checkpoint name. An invocation override always wins over a checkpoint override that also matches the call.

### Output overrides need a downstream consumer

An `"output"` override works by injecting the value as the target checkpoint's result and feeding it into the checkpoints that consume that output. It needs somewhere for the value to go. If you point an output override at a terminal (leaf) checkpoint whose result nothing else reads, there is no later input to replace, and Kitaru fails before submission with a message telling you to guard the side effect with `kitaru.is_replay()` or move the side effect into a checkpoint whose output is consumed later. So an output override cannot neutralize a side-effectful final checkpoint; use the replay guard for that.

`"code"` overrides are runtime-only. They are carried in replay context and require the flow-wrapper replay path so the runtime can see that context while the checkpoint reruns. In a Pydantic AI tool call, Kitaru's wrapper reaches the tool checkpoint, reads the replay context, imports the replacement function, and calls that replacement instead of the original tool function.

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

Override exactly one thing — a flow input, a checkpoint input, a checkpoint output, or a single model call — while reproducing everything else. The forked run then differs from the baseline only by that change.

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

Replay a PydanticAI tool call with edited arguments:

```python
submission = client.executions.replay(
    "kr-a8f3c2",
    at="lookup_policy_tool",
    checkpoint_overrides={
        "lookup_policy_tool": {
            "input": {"account_id": "acct-2"},
        },
    },
)
```

In that last example, Kitaru reruns the `lookup_policy_tool` checkpoint. The PydanticAI tool body receives `account_id="acct-2"`, and the `tool_args` input artifact recorded on the replay reflects the edited arguments.

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

# Rerun a PydanticAI tool checkpoint with edited tool arguments
kitaru executions replay kr-a8f3c2 \
  --at lookup_policy_tool \
  --checkpoint-overrides '{"lookup_policy_tool":{"input":{"account_id":"acct-2"}}}'
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

If `lookup_policy_tool` appears multiple times in the source execution, Kitaru applies the replacement callable to each matching recorded invocation. The replacement function must be importable where replay runs; for example, the replay overrides demo uses `mocks.lookup_policy` from `examples/end_to_end/replay_overrides_demo/mocks.py`.

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

When replay executions are omitted, `kitaru.diff` discovers all runs whose `original_exec_id` matches the source. An explicit single diff accepts only recorded direct replays of the requested original. Compare a replay chain one hop at a time: compare the original with its direct replay, then compare that replay with its own direct replay. Kitaru rejects blank execution IDs and executions with missing or conflicting replay lineage instead of comparing unrelated runs. Repeated selectors, including different aliases for the same replay, are compared once in first-occurrence order.

Diff many originals against their auto-discovered replays:

```python
matrix = kitaru.diff_matrix(["kr-original-a", "kr-original-b"])
for row in matrix.rows:
    print(row.original_exec_id, row.urls)
```

CLI:

```bash
# Compact checkpoint comparison in the default text output
kitaru executions diff kr-original kr-replay-a

# Complete structured results
kitaru executions diff kr-original kr-replay-a -o json
kitaru executions diff-matrix kr-a kr-b kr-c -o json
```

The default `executions diff` output shows one row per checkpoint for each compared replay:

```text
Diff for kr-original
  Compared against 1 replay execution(s).
Checkpoint differences
  Replay        Checkpoint    Result    Duration Δ (ms)   Input Δ   Output Δ   Total Δ   Cost Δ (USD)   Artifacts
  -----------   -----------   -------   ---------------   -------   --------   -------   ------------   ----------------
  kr-replay-a   research      match     -8.0              +0        +0         +0        +0.000000      output=unchanged
  kr-replay-a   write_draft   changed   +412.5            -120      +40        -80       -0.001700      output=changed
Applied output overrides [kr-replay-a]:
  checkpoint family 'research' -> research, research_2
```

The table reports `changed` when checkpoint status, token usage, cost, or
artifact hashes differ. It uses `original only` when a source checkpoint has no
replay match and `replay only` when a new checkpoint appears only in the replay.
Duration, token, and cost deltas are always **replay minus original**: a
negative value means the replay was faster, used fewer tokens, or cost less.
The `Artifacts` column lists each saved artifact role sorted by name —
`unchanged` when both content hashes match, `changed` when both exist but
differ, and `unavailable` when either hash is missing. Artifact hashes and
values themselves are never printed. `n/a` means the corresponding metric or
artifact comparison is unavailable, which is distinct from an explicit zero
delta. `executions diff-matrix` keeps its existing summary-only text output.

Each replay entry in JSON has an additive `applied_output_overrides` field. It
contains only the selector, selector kind, resolved checkpoint invocation IDs,
and the fact that the `output` field was overridden:

```json
{
  "replay_exec_id": "kr-replay-a",
  "applied_output_overrides": [
    {
      "selector_kind": "checkpoint",
      "selector": "research",
      "matched_invocation_ids": ["research", "research_2"],
      "field": "output"
    }
  ]
}
```

The replacement value is never included. `applied_output_overrides: null` means
the evidence is unavailable, as with an older replay or malformed metadata.
`applied_output_overrides: []` proves that the replay was recorded with no
output override. Matrix JSON includes the same field inside each compared replay.
This is submission-time evidence from the resolved replay plan, not confirmation
that a downstream checkpoint completed; use the checkpoint status rows to see
what actually ran.

An output override remains a downstream injection, not a replacement artifact.
Kitaru skips the targeted checkpoint, passes the supplied value to downstream
checkpoint inputs, and continues to report the source checkpoint's recorded
artifact. Its original and replay artifact hashes therefore remain the same.

Checkpoint token and cost deltas in JSON output are always calculated as
**replay minus original**. For example:

```json
{
  "token_delta": {
    "prompt_tokens": -120,
    "completion_tokens": 40,
    "total_tokens": -80
  },
  "cost_delta_usd": -0.0017
}
```

The token object always uses the keys `prompt_tokens`, `completion_tokens`, and
`total_tokens`. It measures workload, including model work reused by the replay.
A negative value means the replay workload used fewer tokens than the original;
a positive value means it used more. A recorded model call with zero tokens
produces a three-key object containing zeroes, while `token_delta: null` means
neither checkpoint has a model usage record.

The cost follows the same subtraction rule but measures incurred display cost.
`cost_delta_usd: null` means at least one model call that was not explicitly
reused has neither usable actual cost nor a usable estimate, so Kitaru cannot
calculate a trustworthy difference. Reused checkpoint work contributes zero
cost because the provider call was not made again.

For example, a replay that fully reuses the original model work can keep the
same workload while avoiding all provider spend:

```json
{
  "token_delta": {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0
  },
  "cost_delta_usd": -0.0025
}
```

The zero token delta says the recorded workload is unchanged. The negative cost
delta says the replay avoided the original $0.0025 of provider spend.

`ExecutionDiff.urls` links to the Kitaru UI compare view — one URL listing the original and every compared replay, whether discovered automatically or passed explicitly:

```text
{server}/flows/{flow_id}/v/{deployment_version}/compare?executions={id1},{id2},{id3}
```

When deployment metadata is absent on the execution, the version segment defaults to `local`.

Use `kitaru.build_compare_url_for_executions(...)`, `kitaru.compare_url_for_executions(...)`, or `kitaru.build_compare_url(...)` when constructing links yourself.

When the Kitaru frontend is hosted separately from the API server, set `KITARU_UI_URL` to the dashboard origin. Compare and execution links from `kitaru executions replay`, `kitaru.diff()`, and flow submission logs use that override; API traffic still uses `KITARU_API_URL`.

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

### Don't reshape the run before the anchor

The same caution applies to `flow_overrides`. The checkpoints before `at` are reused by matching the recorded call sequence, so the shape of the run up to the anchor has to stay the same. If a flow override changes pre-anchor control flow, for example a loop count, a branch condition, or the number of tool calls before the anchor, the recorded skip set no longer lines up with the new sequence and replay diverges instead of quietly producing a wrong result. If you need to change something that early, pick an earlier anchor that still matches, or expect replay to fail loudly rather than reproduce faithfully.

Also check the replayed execution's failure metadata:

```python
latest = client.executions.get(submission.results[0].replay_exec_id)
if latest.failure:
    print(latest.failure.origin, latest.failure.message)
```

## Example in this repository

For a small deterministic replay example:

```bash
uv sync --extra local
uv run python examples/features/replay/replay_with_overrides.py
uv run pytest tests/test_phase16_replay_example.py
```

For the end-to-end override walkthrough, including the Pydantic AI policy-tool code swap:

```bash
uv sync --extra local --extra pydantic-ai
cd examples/end_to_end/replay_overrides_demo
uv run kitaru init  # required in fresh worktrees; creates .kitaru/ here
uv run python demo.py seed --count 1
uv run python demo.py code-swap
```

For the broader catalog, see [Examples](../getting-started/examples.md).

## Related blog posts

- [Agents need more than traces](https://kitaru.ai/blog/agents-need-more-than-traces)
- [Why agents need durable execution](https://kitaru.ai/blog/why-agents-need-durable-execution)
