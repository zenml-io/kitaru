---
description: Every model call recorded — tokens, cost, and call counts on every session, and how to total them across cohorts and experiments.
icon: coins
---

# Track cost and model usage

Every model call in a recorded run lands as an `llm_call` node on the
[session](../concepts/agents-and-sessions.md): the requested and resolved
model, inputs and outputs, token usage (input, output, cached, reasoning),
and cost. The session rolls them up as it goes, so the totals are already
there when you read a run:

```python
session = await client.sessions.get(session_id)
print(session.cost)             # Decimal, summed across the run's model calls
print(session.tokens)           # input / output / cached_input / reasoning
print(session.llm_call_count, session.tool_call_count)
```

Imported sessions get the same treatment — when your Langfuse export
carries usage and cost, the importer preserves them, so your history is
costed the moment it lands.

## Per-call detail

When the total isn't enough, the nodes have the breakdown:

```python
nodes = await client.sessions.list_nodes(session_id, include_payloads=True)
for node in nodes.items:
    if node.node_type == "llm_call":
        print(node.requested_model, node.model, node.tokens, node.cost)
```

`requested_model` vs `model` is worth watching: it's how you see an alias
or a replay [override](replay-and-overrides.md) resolving to the model
that actually served the call.

## Cost as an experiment metric

Cost earns its place in the loop as a *delta*. Every replay's result
session carries its own rollups, so "did the cheaper model hold?" is a
pass-rate comparison and a cost comparison from the same rows:

```python
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.replay import ReplayListParams

baseline_cost = fork_cost = 0
async for r in client.replays.iter(ReplayListParams(
    filter=FilterCondition(field="experiment_run_id", op=FilterOp.EQ, value=RUN_ID)
)):
    baseline = await client.sessions.get(r.baseline_session_id)
    fork = await client.sessions.get(r.result_session_id)
    baseline_cost += baseline.cost or 0
    fork_cost += fork.cost or 0

print(f"cohort cost: ${baseline_cost} -> ${fork_cost}")
```

A negative delta across a [cohort](../concepts/cohorts.md) is the cheaper
model paying for itself — with the pass rates from your
[evaluators](write-an-evaluator.md) sitting right next to it saying
whether the savings were free.

{% hint style="info" %}
Recorded cost is an observability number derived from provider usage
data, not an invoice. Treat deltas as reliable and absolute values as
estimates.
{% endhint %}

Budget-minded evaluator runs matter too: code evaluators cost nothing to
run, while [LLM judges](write-an-evaluator.md) spend judge tokens per
session — size your per-PR cohort accordingly and save the wide sweep for
the nightly run.
