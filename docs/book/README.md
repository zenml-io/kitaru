---
description: Replay-based evals for AI agents, in Python and TypeScript. Test your next change against what your agent already did in production.
icon: hand-wave
---

# Welcome to Kitaru

Your agent has already been tested thousands of times, in production. Each of those runs is sitting in a trace store as a transcript you can read but not run. Kitaru makes them runnable: it records (or imports) each run as a **session**, then **replays** it against your real code, with the recording answering for the world the original run saw. Change the prompt, swap the model, or point replay at the fix in your working tree, and see what improved and what broke before it ships.

**Who it's for:** teams with an agent in front of real users, where regression testing today means re-running a few samples and eyeballing the output. Kitaru replaces that with [evaluators](concepts/evaluators.md), [cohorts](concepts/cohorts.md), and [experiments](concepts/experiments.md) over your actual traffic. If you're prototyping and haven't shipped, it will feel like more machinery than you need.

**Languages:** Python and TypeScript SDKs, against the same server. The CLI is Python.

**Frameworks:** adapters ship for [PydanticAI](adapters/pydantic-ai.md), [LangGraph](adapters/langgraph.md), and the [OpenAI Agents SDK](adapters/openai-agents.md) in Python, and for [Mastra](adapters/mastra.md) and the [Vercel AI SDK](adapters/vercel-ai.md) in TypeScript. Any other framework still works: [import your traces](getting-started/import-your-traces.md) (Langfuse, LangSmith, Braintrust, and plain JSONL importers are built in) or [build a small adapter](adapters/custom.md) (the recording API is two client calls).

Kitaru is open source (Apache 2.0) and self-hosted, from the team behind [ZenML](https://zenml.io): ZenML is for ML pipelines, Kitaru is for agents.

## Do I have to run it in production?

No. There are two ways to get sessions, and they end in the same place:

- **Import the history you already have.** If your agent logs to Langfuse or anything else you can export from, import it. Nothing in your production path changes: your trace store stays your system of record, and Kitaru gets a runnable copy.
- **Record with an adapter.** Wrap the agent once, no rewrite, and every run becomes a session wherever the agent runs: production, staging, or your laptop.

{% tabs %}
{% tab title="Import traces" %}
```bash
kitaru session import langfuse-export.jsonl \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest --wait
```
{% endtab %}

{% tab title="Record with an adapter" %}
```python
from pydantic_ai import Agent
from kitaru_pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5.4", name="support-agent",
              system_prompt="You resolve support tickets.")

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    return payments.refund(order_id)  # your real API

support = KitaruAgent(agent, agent_id=AGENT_ID)
support.run_sync("Refund order #4821, the card reader double-charged me.")
```
{% endtab %}
{% endtabs %}

Replays, imports, and evaluations all execute offline, on [workers](concepts/workers.md) in your environment. None of it touches your production traffic. An adapter does run inside your agent's process to record; if you don't want anything of Kitaru's near production, the import path never gets close to it.

## The loop

- **Record.** Wrap or import, as above. Either way, runs land as [sessions](concepts/agents-and-sessions.md).
- **Replay.** [Re-execute a session](concepts/replay.md) against your real code. Tool calls are answered from the recording, so nothing touches real systems. Unchanged, the replay reproduces the original: the faithful baseline that makes a diff trustworthy. Then fork it with a different model, a new prompt, or your working tree's code.
- **Improve.** [Evaluators](concepts/evaluators.md) score both sides; [cohorts](concepts/cohorts.md) freeze the population; [experiments](concepts/experiments.md) replay a cohort against a change and show what improved and what regressed. The cohort that caught a failure becomes the regression gate that keeps it caught.

Read the [Quickstart](getting-started/quickstart.md) for Kitaru's five-step method. To try it in a controlled environment, prepare the public [`kitaru-template`](https://github.com/zenml-io/kitaru-template), then follow the [complete returns-agent tutorial](tutorials/returns-agent/README.md).

## Built to sit in your stack

- **Self-hosted.** One FastAPI + Postgres server on your infrastructure. Your traces and credentials don't leave your systems.
- **Beside your observability, not instead of it.** Langfuse, LangSmith, and Braintrust remain where you watch production. Kitaru is where you re-run it.
- **Four ways to drive it:** the `kitaru` CLI, Python SDK, TypeScript SDK, and your [coding agent](agent-native/mcp-server.md). Kitaru observes your production agents; your coding assistant is how you talk to Kitaru.

## Next steps

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Installation</strong></td><td>SDK, CLI, a local server, and a login.</td><td><a href="getting-started/installation.md">getting-started/installation.md</a></td></tr><tr><td><strong>Quickstart</strong></td><td>Understand the five-step method before running commands.</td><td><a href="getting-started/quickstart.md">getting-started/quickstart.md</a></td></tr><tr><td><strong>Kitaru template</strong></td><td>Prepare a ready PydanticAI agent and checked-in Langfuse traces.</td><td><a href="https://github.com/zenml-io/kitaru-template">https://github.com/zenml-io/kitaru-template</a></td></tr><tr><td><strong>Complete tutorial</strong></td><td>Investigate and replay the template's synthetic returns agent.</td><td><a href="tutorials/returns-agent/README.md">tutorials/returns-agent/README.md</a></td></tr><tr><td><strong>Import your traces</strong></td><td>Start from the history you already have.</td><td><a href="getting-started/import-your-traces.md">getting-started/import-your-traces.md</a></td></tr><tr><td><strong>Core Concepts</strong></td><td>Sessions, replay, evaluators, cohorts, experiments.</td><td><a href="concepts/README.md">concepts/README.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Production traffic as your test suite.</td><td><a href="guides/regression-suite.md">guides/regression-suite.md</a></td></tr><tr><td><strong>Deploy Kitaru</strong></td><td>Self-host for your team.</td><td><a href="deploy/README.md">deploy/README.md</a></td></tr></tbody></table>
