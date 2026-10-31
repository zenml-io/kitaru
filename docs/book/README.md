---
description: Insights, checks, and tested fixes from your AI agent's traces, in Python and TypeScript. Find the patterns across every session and test your next change against what your agent already did in production.
icon: hand-wave
---

# Welcome to Kitaru

Your tracing tool shows you one session at a time, and your evals score each session against the failures you thought of in advance. Kitaru reads across all of them. It records or imports each run as a **session**, and [analyzers](concepts/analyzers.md) surface the patterns across every session as **insights**. You turn what you find into [evaluators](concepts/evaluators.md) that check every session, then **replay** your real code against a fix, with the recording answering for the world the original run saw. Change the prompt, swap the model, or point replay at the fix in your working tree, and see what improved and what broke before it ships.

{% hint style="success" %}
**Who it's for:** Teams with an agent in front of real users, where today you read traces one at a time and re-run a few samples by eye. Kitaru replaces that with [analyzers](concepts/analyzers.md), [evaluators](concepts/evaluators.md), [cohorts](concepts/cohorts.md), and [experiments](concepts/experiments.md) over your actual traffic. If you're prototyping and haven't shipped, it will feel like more machinery than you need.
{% endhint %}

**Frameworks:** adapters ship for [PydanticAI](adapters/pydantic-ai.md), [LangGraph](adapters/langgraph.md), and the [OpenAI Agents SDK](adapters/openai-agents.md) in Python, and for [Mastra](adapters/mastra.md) and the [Vercel AI SDK](adapters/vercel-ai.md) in TypeScript. Other frameworks still work: [import your traces](getting-started/import-your-traces.md) with the built-in Langfuse, LangSmith, Braintrust, Logfire, Arize Phoenix, MLflow, or JSONL importers; [write a custom importer](guides/custom-importer.md), usually about a page of Python; or [build a small adapter](adapters/custom.md), where the recording API is two client calls.

{% hint style="info" %}
Kitaru has both a Python and a TypeScript SDK, and both talk to the same server. The CLI ships with the Python package.
{% endhint %}

**Kitaru is built to be driven by agents.** The MCP server gives Claude Code, Codex, Cursor, and other coding assistants bounded Kitaru operations. The agent skills teach the procedures, and the CLI speaks JSON when a shell command is the right tool. You bring the judgment; your assistant handles the investigation work. [Set up your coding agent](agent-native/setup.md) takes a few minutes.

Kitaru is open source (Apache 2.0) and self-hosted, from the team behind [ZenML](https://zenml.io): ZenML is for ML pipelines, Kitaru is for agents.

## The loop

- **Bring your traces.** Wrap your agent or import your traces (both shown below). Either way, runs land as [sessions](concepts/agents-and-sessions.md).
- **Get insights.** [Analyzers](concepts/analyzers.md) read every session at once and write insights about the whole set: failed tool calls, repeated calls, how outcomes split. Each insight points to the sessions behind it, so you know where to look first. The deterministic [post-import insights](guides/post-import-insights.md) analyzer needs no model or API key.
- **Turn findings into checks.** This is where your judgment enters. In an [investigation](concepts/investigations.md), your coding assistant authors the review, walks you through the evidence, asks the questions Kitaru needs answered, and pins your answers to the exact trace as annotations. Those judgments calibrate the [evaluators](concepts/evaluators.md) that check every session; [cohorts](concepts/cohorts.md) freeze the population so the same sessions get checked every time.
- **Replay a fix.** [Re-execute a session](concepts/replay.md) against your real code. Tool calls are answered from the recording, so nothing touches real systems. An unchanged replay gives you the faithful baseline. Then fork it with a different model, a new prompt, or your working tree's code. [Experiments](concepts/experiments.md) replay a cohort against a change and show what improved and what regressed. The cohort that caught a failure becomes the regression gate that keeps it caught.

In daily work, that loop becomes five steps: **observe** a recorded behavior, **judge** what should have happened, **define** the behavior to test, **replay** the changed agent, and **compare** the evidence. Sessions and their insights give you the raw material; observe, judge, and define turn human judgment into durable criteria; replay and compare close the loop. The [Quickstart](getting-started/quickstart.md) walks all five. To try it in a controlled environment, ask your assistant for the `kitaru-guided-tour` skill, which runs the loop on the [`PydanticAI returns agent example`](https://github.com/zenml-io/kitaru/tree/main/examples/python/pydantic_ai_ticket_resolver), or follow the [complete returns agent tutorial](tutorials/returns-agent/README.md) manually.

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

agent = Agent(
    "openai:gpt-5.4", name="support-agent", system_prompt="You resolve support tickets."
)


@agent.tool_plain
def refund_payment(order_id: str) -> str:
    return payments.refund(order_id)  # your real API


support = KitaruAgent(agent, agent_id=AGENT_ID)
support.run_sync("Refund order #4821, the card reader double-charged me.")
```
{% endtab %}
{% endtabs %}

Replays, imports, and evaluations run offline on [workers](concepts/workers.md) in your environment. None of that touches your production traffic. An adapter does run inside your agent's process to record; if you do not want Kitaru near production, the import path never gets close to it.

## Built to sit in your stack

- **Self-hosted.** One FastAPI + Postgres server on your infrastructure. Your traces and credentials don't leave your systems.
- **Beside your observability, not instead of it.** Langfuse, LangSmith, Braintrust, Logfire, Arize Phoenix, and MLflow remain where you watch production. Kitaru is where you find the patterns across it and re-run it.
- **Choose how you drive it:** the `kitaru` CLI, Python SDK, TypeScript SDK, and your [coding agent](agent-native/setup.md). Kitaru observes your production agents; your coding assistant is how you talk to Kitaru.

**Questions, bugs, feedback?** Join the [Slack community](https://kitaru.ai/slack), report bugs at [kitaru.ai/help](https://kitaru.ai/help) (it goes straight to GitHub issues), or email [support@kitaru.ai](mailto:support@kitaru.ai). All three reach a human.

## Next steps

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Installation</strong></td><td>SDK, CLI, a local server, and a login.</td><td><a href="getting-started/installation.md">getting-started/installation.md</a></td></tr><tr><td><strong>Quickstart</strong></td><td>Understand the five-step method before running commands.</td><td><a href="getting-started/quickstart.md">getting-started/quickstart.md</a></td></tr><tr><td><strong>PydanticAI returns agent</strong></td><td>Prepare a ready agent and checked-in Langfuse traces.</td><td><a href="https://github.com/zenml-io/kitaru/tree/main/examples/python/pydantic_ai_ticket_resolver">https://github.com/zenml-io/kitaru/tree/main/examples/python/pydantic_ai_ticket_resolver</a></td></tr><tr><td><strong>Complete tutorial</strong></td><td>Investigate and replay the example's synthetic returns agent.</td><td><a href="tutorials/returns-agent/README.md">tutorials/returns-agent/README.md</a></td></tr><tr><td><strong>Import your traces</strong></td><td>Start from the history you already have.</td><td><a href="getting-started/import-your-traces.md">getting-started/import-your-traces.md</a></td></tr><tr><td><strong>Core Concepts</strong></td><td>Sessions, replay, evaluators, cohorts, experiments.</td><td><a href="concepts/README.md">concepts/README.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Production traffic as your test suite.</td><td><a href="guides/regression-suite.md">guides/regression-suite.md</a></td></tr><tr><td><strong>Deploy Kitaru</strong></td><td>Self-host for your team.</td><td><a href="deploy/README.md">deploy/README.md</a></td></tr></tbody></table>
