---
description: Wrap an agent, look at the recording, replay it, fork it, and turn a week of runs into a regression test.
icon: rocket
---

# Quickstart

You will take an agent you already have, wrap it so every run becomes an
[execution](../concepts/executions.md), and then do the one thing Kitaru is for:
replay that recording. Reproduce it exactly, fork it with one thing changed, diff
the two, then widen the same experiment across a week of real runs. Fifteen
minutes, one sitting.

The agent is a support copilot. The ticket is always the same one:

> Refund order #4821 — the card reader was double-charged.

If you haven't installed Kitaru yet, start with [Installation](installation.md).

## Wrap the agent

Install Kitaru with the PydanticAI adapter and initialize the project:

{% tabs %}
{% tab title="uv (recommended)" %}
```bash
uv add "kitaru[pydantic-ai]"
kitaru init
```
{% endtab %}

{% tab title="pip" %}
```bash
pip install "kitaru[pydantic-ai]"
kitaru init
```
{% endtab %}
{% endtabs %}

The agent calls an OpenAI model, so give it a key:

```bash
export OPENAI_API_KEY=sk-...
```

No decorators, no graph, no rewrite. Wrap the agent you already have with
`KitaruAgent` and run it. Kitaru opens a flow around the call and records every
model request and tool call as a checkpoint.

```python
# support.py
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent(
    "openai:gpt-5-nano",
    name="support-agent",
    system_prompt="You resolve support tickets. Issue refunds when the customer was overcharged.",
)

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    # Your real refund API. Returns a human-readable confirmation.
    return payments.refund(order_id)

support = KitaruAgent(agent)

if __name__ == "__main__":
    result = support.run_sync(
        "Refund order #4821 — the card reader was double-charged."
    )
    print(result.output)
```

Run it:

{% tabs %}
{% tab title="uv (recommended)" %}
```bash
uv run support.py
```
{% endtab %}

{% tab title="pip environment" %}
```bash
python support.py
```
{% endtab %}
{% endtabs %}

{% hint style="info" %}
The adapter's auto-flow is local-only. On a remote stack, wrap the call in an
explicit `@kitaru.flow`. See the
[PydanticAI adapter](../adapters/pydantic-ai.md) for the full story.
{% endhint %}

## Look at the recording

That run left behind one execution. List it and open it up:

```bash
kitaru executions list
kitaru executions get <exec-id>
```

`kitaru executions get` prints the checkpoints the run crossed. With the default
per-call strategy, a support agent that thinks and then refunds records something
like:

```text
Checkpoints
  support-agent_model_request  (completed)
  refund_payment_tool          (completed)
  support-agent_model_request_2 (completed)
```

Those names — the model request and the `refund_payment_tool` call — are your
replay cut points. The same view is in the dashboard, with each checkpoint's
prompt, response, tool arguments, token usage, and cost. Everywhere below, read
the exact names from your own `kitaru executions get` output.

Grab the execution from Python too — you'll need its `exec_id` for replay:

```python
from kitaru import KitaruClient

client = KitaruClient()
trace = client.executions.latest()
print(trace.exec_id, trace.status)
```

## Replay it unchanged — the baseline

First, prove the recording is faithful. Replay from the agent's first model call
with nothing changed:

```python
baseline = client.executions.replay(trace.exec_id, at="support-agent_model_request")
baseline_id = baseline.results[0].replay_exec_id
print(baseline_id)
```

Your real code runs again, but every recorded checkpoint answers exactly as it
did the first time, so the replay reproduces the original. **This is the
discipline: if the unchanged replay does not reproduce, stop — nothing you fork
from it can be trusted.** A faithful baseline is what makes the next step mean
something.

## Fork it with one thing changed

Now ask a counterfactual: what would the agent have done if the refund had
already succeeded? Replay from the tool call and patch its recorded output,
leaving everything else identical:

```python
fork = client.executions.replay(
    trace.exec_id,
    at="refund_payment_tool",
    checkpoint_overrides={
        "refund_payment_tool": {"output": "refund issued: $129.00"},
    },
)
fork_id = fork.results[0].replay_exec_id
```

Kitaru skips the real `refund_payment` call, injects your value as its result,
and reruns the model request that consumes it. The agent now composes its reply
against a successful refund — same run, one thing changed.

## Diff the two

Compare the original, the faithful baseline, and the fork:

```python
import kitaru

execution_diff = kitaru.diff(trace.exec_id, baseline_id, fork_id)
print(execution_diff.urls)
```

Because the baseline reproduced, the difference between it and the fork is your
change — the successful-refund path — not replay noise. The diff shows which
checkpoints changed, plus token and cost deltas per run. This is the whole loop:
reproduce a real run, change exactly one thing, trust the diff.

## Widen it into a regression test

One replay tells you about one ticket. The same call takes a list, so point it at
your last week of real runs and tag the batch:

```python
recent = client.executions.list(limit=20)

submission = client.executions.replay(
    [e.exec_id for e in recent],
    at="support-agent_model_request",
    tag="refund-regression",
    on_error="collect",
)

for row in submission.results:
    print(row.original_exec_id, "->", row.replay_exec_id, row.status)
```

`on_error="collect"` keeps going when a run doesn't contain that checkpoint,
recording it in `submission.skipped` instead of failing the batch. Now the cohort
is a regression test: replay last week's production traffic against the code in
your working tree and see what moved. Ranking the results by cost or a quality
judge is the subject of
[Build a regression suite from production](../guides/regression-suite.md).

Already collecting traces elsewhere? You don't have to run the agent through
Kitaru to get a recording — imported traces land as executions too. See
[Executions — the recording](../concepts/executions.md) for how imports work and
what they can and can't do yet.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Debug and test on real runs</strong></td><td>Every override level, selector rules, side-effect guards, and reading a diff.</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Build a regression suite from production</strong></td><td>Batch replay a cohort, tag it, and rank the results.</td><td><a href="../guides/regression-suite.md">../guides/regression-suite.md</a></td></tr><tr><td><strong>Track cost and model usage</strong></td><td>What every model call records, and how to total it.</td><td><a href="../guides/llm-calls.md">../guides/llm-calls.md</a></td></tr><tr><td><strong>Drive it from your coding agent</strong></td><td>Run the whole loop from Claude Code, Codex, or Cursor over MCP.</td><td><a href="../agent-native/mcp-server.md">../agent-native/mcp-server.md</a></td></tr><tr><td><strong>Deploy &#x26; Invoke</strong></td><td>Move from local runs to a versioned, remotely invocable deployment.</td><td><a href="../guides/deployments.md">../guides/deployments.md</a></td></tr><tr><td><strong>Agents Guide</strong></td><td>The end-to-end narrative tour in the ZenML Learn section.</td><td><a href="https://docs.zenml.io/user-guides/agents-guide">https://docs.zenml.io/user-guides/agents-guide</a></td></tr></tbody></table>
