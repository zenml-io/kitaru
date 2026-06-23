---
description: Reproduce a real execution from a checkpoint, replay it with one input changed, and diff the two.
icon: rotate-left
---

# Replay and Overrides

Replay re-executes a real, recorded run from a checkpoint to produce a **new execution** — the same run, with the changes you specify. This is what makes Kitaru more than tracing: because every model call and tool call was recorded as a durable checkpoint, you can reproduce a run faithfully, fork it with one input changed, and attribute the difference to your change rather than to replay noise.

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

## Reproduce a run

Replay with no overrides to get a faithful rerun. Use this as your baseline.

```python
import kitaru

client = kitaru.KitaruClient()

baseline = client.executions.replay("kr-a8f3c2", from_="write_draft")

print(baseline.exec_id)           # new execution
print(baseline.original_exec_id)  # points to the source execution
```

## Replay with one overridden input

Override flow inputs as keyword arguments. Overriding a single input (for example the model or the prompt profile) while reproducing everything else is the differentiator: the forked run differs from the baseline only by that input.

```python
forked = client.executions.replay(
    "kr-a8f3c2",
    from_="write_draft",
    model="gpt-4o",
    prompt_profile="concise",
)

print(forked.exec_id)
```

You can also override a recorded checkpoint output directly with the `overrides`
map (keys must start with `checkpoint.`):

```python
forked = client.executions.replay(
    "kr-a8f3c2",
    from_="write_draft",
    topic="New topic",
    overrides={"checkpoint.research": "Edited notes"},
)
```

### Flow-object replay

If you hold the flow object, call `.replay(...)` on it and `.wait()` for the result:

```python
handle = content_pipeline.replay(
    exec_id="kr-a8f3c2",
    from_="write_draft",
    model="gpt-4o",
)

result = handle.wait()
print(result.exec_id)
```

## CLI replay

The CLI mirrors the SDK. Pass flow-input overrides as a JSON `--args` blob and
checkpoint-output overrides as `--overrides`:

```bash
# Reproduce (baseline)
kitaru executions replay kr-a8f3c2 --from write_draft

# Fork with one input changed
kitaru executions replay kr-a8f3c2 \
  --from write_draft \
  --args '{"model":"gpt-4o","prompt_profile":"concise"}'

# Override a recorded checkpoint output
kitaru executions replay kr-a8f3c2 \
  --from write_draft \
  --args '{"topic":"New topic"}' \
  --overrides '{"checkpoint.research":"Edited notes"}'
```

## Diff the two runs

Once you have a baseline and a forked execution, compare them to attribute the
difference. Fetch each execution and inspect its outputs and metadata:

```python
base = client.executions.get(baseline.exec_id)
fork = client.executions.get(forked.exec_id)

# Compare final outputs, per-checkpoint results, cost, and latency
```

{% hint style="info" %}
Cohort experiments — applying the same override across many recent runs and
ranking the results by cost, latency, or a quality judge — are a pattern you
build on top of replay, not a separate API: list recent executions, replay each
with the same override, and aggregate the metrics yourself.
{% endhint %}

## Selector rules

`from_` can target:

- a checkpoint name (for example `write_draft`)
- a checkpoint invocation ID
- a checkpoint call ID

If a selector is ambiguous, replay raises `KitaruStateError`.

## What gets replayed

Replay computes a set of replay roots, then re-executes those roots and their
downstream descendants.

- `from_` always contributes one replay root.
- Each `checkpoint.<selector>` override adds replay roots at the direct
  consumers of that checkpoint output.
- Any checkpoint that is neither a replay root nor a downstream dependency is
  reused from the recorded run, which is what keeps the reproduced run faithful.

## Override keys

Replay override keys in the `overrides` map must start with `checkpoint.`.

Examples:

- `checkpoint.research`
- `checkpoint.fetch_data`

Any other prefix raises `KitaruUsageError`.

Override behavior:

- Overrides target checkpoint outputs (`checkpoint.<selector>`).
- The overridden checkpoint must expose a single output.
- `checkpoint.<selector>` replaces that checkpoint output at each direct
  consumer input; the source checkpoint itself is not forced to re-execute.
- Replay roots include direct consumers of the overridden checkpoint output, so
  replay re-executes from those consumers forward.
- If an overridden checkpoint fans out to multiple direct consumers, replay
  re-executes all those consumer branches.

{% hint style="warning" %}
**Roadmap.** Overriding a *specific tool call* to return a fake value or raise
an error (per-tool-call `output=` / `raise_=` mocks) is planned but not yet
shipped. Today, replay overrides target flow inputs and checkpoint outputs, not
individual tool-call results.
{% endhint %}

## Waits during replay

Replay does not support overriding or pre-populating wait results. If a replayed
execution reaches a `wait()` during normal execution, that wait behaves like any
new wait and must be resolved through the normal wait input flow:

- SDK: `client.executions.input(exec_id, wait="name", value=...)`
- CLI: `kitaru executions input <exec_id> --value ...` (auto-detects single pending wait)
- CLI interactive: `kitaru executions input <exec_id> --interactive`

## Divergence

Replay can raise `KitaruDivergenceError` when the backend detects that durable
call sequence compatibility is broken — for example if the flow code changed in a
way that no longer matches the recorded call sequence. A divergence means the run
can no longer be reproduced faithfully, so any diff from it is untrustworthy.

Also check replayed execution failure metadata:

```python
latest = client.executions.get(forked.exec_id)
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
