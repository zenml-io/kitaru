---
description: Replay-based evals for AI agents. Traces you can run, not just read.
icon: hand-wave
---

# Welcome to Kitaru

Most traces are transcripts: you read them. Kitaru records an agent run as
a **session** — every model call, tool call, and decision — and a session
**re-executes**: your real code runs again, with the recording answering
for the world the original run saw. Reproduce a run, fork it with one
thing changed, and trust that the difference you see is your change.

That single verb — **replay** — is what turns production's past into your
test bench. You can't unit-test an agent that writes to real systems, and
your agent left the test suite behind the day it shipped. But it has been
generating test cases all along: every production run. Kitaru is the
machinery that makes them runnable — a debugger with a memory, sitting
beside your observability stack. Traces tell you what happened; Kitaru
re-runs it.

Kitaru comes from the team behind [ZenML](https://zenml.io): ZenML is
for ML pipelines, Kitaru is for agents. The full split is in
[Where ZenML fits](#where-zenml-fits).

## The loop

* **Record** — wrap the agent you already have with an
  [adapter](adapters/README.md) (one wrapper, no rewrite), or
  [import the traces you already collect](getting-started/import-your-traces.md).
  Langfuse stays your system of record; Kitaru gets a runnable copy.
  Either way, runs land as [sessions](concepts/agents-and-sessions.md).
* **Replay** — [re-execute a session](concepts/replay.md) against your
  real code. Tool calls are answered from the recording, so nothing
  touches real systems. Unchanged, the replay reproduces the original —
  the faithful baseline that makes a diff trustworthy. Then fork it: a
  different model, a new prompt, your working tree's code.
* **Improve** — [evaluators](concepts/evaluators.md) score both sides;
  [cohorts](concepts/cohorts.md) freeze the population;
  [experiments](concepts/experiments.md) replay a cohort against a change
  and show what improved and what regressed. The cohort that caught a
  failure becomes the regression gate that keeps it caught.

```python
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5.4", name="support-agent",
              system_prompt="You resolve support tickets.")

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    return payments.refund(order_id)  # your real API

support = KitaruAgent(agent, agent_id=AGENT_ID)
support.run_sync("Refund order #4821 — the card reader was double-charged.")
```

Every run is now a session you can replay — the
[Quickstart](getting-started/quickstart.md) takes you from this wrapper to
a measured model-swap experiment in one sitting.

## Built to sit in your stack

* **Self-hosted, open source (Apache 2.0).** One FastAPI + Postgres
  server on your infrastructure. Replays, imports, and evaluations execute
  on [workers](concepts/workers.md) in *your* environment — your traces
  and credentials don't leave your systems.
* **Beside your observability, not instead of it.** Langfuse, LangSmith,
  and Braintrust remain where you watch production. Kitaru is where you
  re-run it.
* **Framework-agnostic by design.** Adapters wrap your existing agent —
  [PydanticAI today](adapters/README.md), more on the way — and the
  [import path](getting-started/import-your-traces.md) works regardless of
  framework.
* **Three ways to drive it:** the `kitaru` CLI and Python SDK, and your
  [coding agent](agent-native/mcp-server.md) — Kitaru observes your
  production agents; your coding assistant is how you talk to Kitaru.

## Where ZenML fits

Kitaru is built by the team behind [ZenML](https://docs.zenml.io), as a
ZenML sub-brand, and the split is clean: **ZenML runs agents durably;
Kitaru replays and improves them.** If you need durable execution,
checkpointed pipelines, or agent orchestration in production, that's
ZenML. Kitaru assumes your agent already runs somewhere — its job is what
happens to the recordings.

## Next steps

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Installation</strong></td><td>SDK, CLI, a local server, and a login.</td><td><a href="getting-started/installation.md">getting-started/installation.md</a></td></tr><tr><td><strong>Quickstart</strong></td><td>Wrap, record, replay, fork, diff — one sitting.</td><td><a href="getting-started/quickstart.md">getting-started/quickstart.md</a></td></tr><tr><td><strong>Import your traces</strong></td><td>Start from the history you already have.</td><td><a href="getting-started/import-your-traces.md">getting-started/import-your-traces.md</a></td></tr><tr><td><strong>Core Concepts</strong></td><td>Sessions, replay, evaluators, cohorts, experiments.</td><td><a href="concepts/README.md">concepts/README.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Production traffic as your test suite.</td><td><a href="guides/regression-suite.md">guides/regression-suite.md</a></td></tr><tr><td><strong>Run the Server</strong></td><td>Self-host for your team.</td><td><a href="deploy/README.md">deploy/README.md</a></td></tr></tbody></table>
