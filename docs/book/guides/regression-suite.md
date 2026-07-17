---
description: Replay a cohort of recent executions against your working tree, tag the batch, and rank the results.
icon: vials
---

# Build a regression suite from production

One replay tells you about one run. A hundred replays tell you whether a change is
safe to ship. Because every [execution](../concepts/executions.md) is a
reproducible test case, a slice of production traffic is the eval suite you never
had to write: replay last week's real runs against the code in your working tree,
tag the batch, and see what moved. **The cohort is a regression test.**

This guide assumes you already know the single-run loop from
[Debug and test on real runs](replay-and-overrides.md). Here you widen it.

## 1. Pick a cohort

The simplest cohort is a recent slice of one flow:

```python
from kitaru import KitaruClient

client = KitaruClient()

recent = client.executions.list(flow="support_agent", status="completed", limit=50)
exec_ids = [e.exec_id for e in recent]
```

When you want only the runs that actually contain the checkpoint you plan to
replay from, let Kitaru resolve the cohort for you. `executions.cohort(...)`
filters to original executions that reached a given cut point, newest first:

```python
cohort = client.executions.cohort(
    flow="support_agent",
    at="support-agent_model_request",
    order_by="-started_at",
    limit=50,
).resolve()

print(len(cohort), "executions")
```

Cohort selection stays separate from replay: resolve the set, inspect it, then
replay it.

## 2. Replay the whole cohort with one change

Pass the cohort straight to `replay` with the override you want to test and a
`tag` that names the batch. A multi-execution replay submits without blocking and
collects errors instead of stopping at the first one:

```python
submission = client.executions.replay(
    cohort,
    at="support-agent_model_request",
    flow_overrides={"model": "openai/gpt-5-nano"},
    tag="pr-1284-cheaper-model",
    on_error="collect",
)

for row in submission.results:
    print(row.original_exec_id, "->", row.replay_exec_id, row.status)

for row in submission.skipped:
    print("skipped", row.original_exec_ref, row.reason)
```

`on_error="collect"` records any source run that doesn't contain the `at`
checkpoint in `submission.skipped` and keeps submitting the rest. The `tag` lets
you find every replay in this experiment later, from the CLI, the dashboard, or
execution statistics.

{% hint style="info" %}
`flow_overrides={"model": ...}` only swaps the model when your flow exposes
`model` as a top-level input its checkpoints read. If the model is chosen inside
a checkpoint or by an adapter, target the model request with a
`checkpoint_overrides` or `invocation_overrides` entry instead. See
[the override levels](replay-and-overrides.md#the-three-override-levels).
{% endhint %}

## 3. Diff and rank the results

Diff every original against its discovered replays in one call:

```python
import kitaru

matrix = kitaru.diff_cohort([row.original_exec_id for row in submission.results])
for row in matrix.rows:
    print(row.original_exec_id, row.urls)
```

`diff_cohort` (and its alias `diff_matrix`) returns a `CohortDiff` whose rows each
compare one original against its replays, with per-checkpoint token and cost
deltas measured as **replay minus original**. A negative cost delta across the
cohort is your cheaper model paying off; a changed checkpoint status is where
behavior moved.

For an aggregate view of the tagged batch — total incurred cost, token volume,
failure mix — use execution statistics rather than fetching every run:

```python
client.executions.statistics(
    group_by=["status"],
    metrics=["llm_display_cost", "llm_total_tokens"],
)
```

{% hint style="info" %}
Ranking by a quality judge is a pattern you build on top of replay, not a
separate API: replay the cohort with your override, then score the replay outputs
with whatever judge you already trust and aggregate the metrics yourself. Kitaru
gives you the faithful reruns and the cost/latency deltas; the quality bar is
yours to define.
{% endhint %}

## The shape of the workflow

1. **Select** a cohort — `executions.list(...)` for a raw slice, or
   `executions.cohort(...).resolve()` to pre-filter to runs that reached your cut
   point.
2. **Replay** it with one override and a `tag`, `on_error="collect"`.
3. **Diff** with `diff_cohort` for per-run deltas, and `executions.statistics` for
   the aggregate.
4. **Keep the winner** — promote the change if the cohort held, using
   [Deploy & Invoke](deployments.md).

## Related

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Debug and test on real runs</strong></td><td>The single-run loop, every override level, and reading a diff.</td><td><a href="replay-and-overrides.md">replay-and-overrides.md</a></td></tr><tr><td><strong>Inspect &#x26; Manage Executions</strong></td><td>Listing, statistics, and lifecycle actions in depth.</td><td><a href="execution-management.md">execution-management.md</a></td></tr><tr><td><strong>Track cost and model usage</strong></td><td>What each model call records, and how totals roll up.</td><td><a href="llm-calls.md">llm-calls.md</a></td></tr></tbody></table>
