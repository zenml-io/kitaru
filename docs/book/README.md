---
description: Replay-based evals for AI agents, in Python and TypeScript. Test your next change against what your agent already did in production.
icon: hand-wave
---

# Welcome to Kitaru

Your agent has already been tested thousands of times, in production. Each of those runs is sitting in a trace store as a transcript you can read but not run. Kitaru makes them runnable: it records (or imports) each run as a **session**, then **replays** it against your real code, with the recording answering for the world the original run saw. Change the prompt, swap the model, or point replay at the fix in your working tree, and see what improved and what broke before it ships.

{% hint style="success" %}
**Who it's for:** Teams with an agent in front of real users, where regression testing today means re-running a few samples and eyeballing the output. Kitaru replaces that with [evaluators](concepts/evaluators.md), [cohorts](concepts/cohorts.md), and [experiments](concepts/experiments.md) over your actual traffic. If you're prototyping and haven't shipped, it will feel like more machinery than you need.
{% endhint %}

**Frameworks:** adapters ship for [PydanticAI](adapters/pydantic-ai.md), [LangGraph](adapters/langgraph.md), and the [OpenAI Agents SDK](adapters/openai-agents.md) in Python, and for [Mastra](adapters/mastra.md) and the [Vercel AI SDK](adapters/vercel-ai.md) in TypeScript. Any other framework still works: [import your traces](getting-started/import-your-traces.md) (Langfuse, LangSmith, Braintrust, Logfire, and plain JSONL importers are built in), [write a custom importer](guides/custom-importer.md) (about a page of Python, and an agent skill drafts it), or [build a small adapter](adapters/custom.md) (the recording API is two client calls).

{% hint style="info" %}
Kitaru has both a Python and a TypeScript SDK, and both talk to the same server. The CLI ships with the Python package.
{% endhint %}

**Kitaru is built to be driven by agents.** The whole product is written for coding assistants as much as for you: the MCP server gives Claude Code, Codex, or Cursor bounded Kitaru operations, the agent skills teach it the procedures, and the CLI speaks JSON. You supply the judgment; your assistant does the legwork. [Set up your coding agent](agent-native/setup.md) takes a few minutes.

Kitaru is open source (Apache 2.0) and self-hosted, from the team behind [ZenML](https://zenml.io): ZenML is for ML pipelines, Kitaru is for agents.

## The loop

- **Record.** Wrap your agent or import your traces (both shown below). Either way, runs land as [sessions](concepts/agents-and-sessions.md).
- **Replay.** [Re-execute a session](concepts/replay.md) against your real code. Tool calls are answered from the recording, so nothing touches real systems. Unchanged, the replay reproduces the original: the faithful baseline that makes a diff trustworthy. Then fork it with a different model, a new prompt, or your working tree's code.
- **Improve.** This is where your judgment enters. In an [investigation](concepts/investigations.md), your coding assistant authors the review and interviews you against the evidence, asking the questions Kitaru needs clarity on, and pins your answers to the exact trace as annotations. Those judgments calibrate the [evaluators](concepts/evaluators.md) that score both sides; [cohorts](concepts/cohorts.md) freeze the population; [experiments](concepts/experiments.md) replay a cohort against a change and show what improved and what regressed. The cohort that caught a failure becomes the regression gate that keeps it caught.

Day to day, you work this loop as five steps: **observe** a recorded behavior, **judge** what should have happened, **define** the behavior to test, **replay** the changed agent, and **compare** the evidence. Record gets you the raw material; observe, judge, and define are Improve's judgment capture; replay and compare close the loop. The [Quickstart](getting-started/quickstart.md) walks all five. To try it in a controlled environment, prepare the public [`kitaru-template`](https://github.com/zenml-io/kitaru-template), then follow the [complete returns-agent tutorial](tutorials/returns-agent/README.md).

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

Replays, imports, and evaluations all execute offline, on [workers](concepts/workers.md) in your environment. None of it touches your production traffic. An adapter does run inside your agent's process to record; if you don't want anything of Kitaru's near production, the import path never gets close to it.

## Built to sit in your stack

- **Self-hosted.** One FastAPI + Postgres server on your infrastructure. Your traces and credentials don't leave your systems.
- **Beside your observability, not instead of it.** Langfuse, LangSmith, Braintrust, and Logfire remain where you watch production. Kitaru is where you re-run it.
- **Choose how you drive it:** the `kitaru` CLI, Python SDK, TypeScript SDK, and your [coding agent](agent-native/setup.md). Kitaru observes your production agents; your coding assistant is how you talk to Kitaru.

**Questions, bugs, feedback?** Join the [Slack community](https://kitaru.ai/slack), report bugs at [kitaru.ai/help](https://kitaru.ai/help) (it goes straight to GitHub issues), or email [support@kitaru.ai](mailto:support@kitaru.ai). All three reach a human.

## Next steps

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Installation</strong></td><td>SDK, CLI, a local server, and a login.</td><td><a href="getting-started/installation.md">getting-started/installation.md</a></td></tr><tr><td><strong>Quickstart</strong></td><td>Understand the five-step method before running commands.</td><td><a href="getting-started/quickstart.md">getting-started/quickstart.md</a></td></tr><tr><td><strong>Kitaru template</strong></td><td>Prepare a ready PydanticAI agent and checked-in Langfuse traces.</td><td><a href="https://github.com/zenml-io/kitaru-template">https://github.com/zenml-io/kitaru-template</a></td></tr><tr><td><strong>Complete tutorial</strong></td><td>Investigate and replay the template's synthetic returns agent.</td><td><a href="tutorials/returns-agent/README.md">tutorials/returns-agent/README.md</a></td></tr><tr><td><strong>Import your traces</strong></td><td>Start from the history you already have.</td><td><a href="getting-started/import-your-traces.md">getting-started/import-your-traces.md</a></td></tr><tr><td><strong>Core Concepts</strong></td><td>Sessions, replay, evaluators, cohorts, experiments.</td><td><a href="concepts/README.md">concepts/README.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Production traffic as your test suite.</td><td><a href="guides/regression-suite.md">guides/regression-suite.md</a></td></tr><tr><td><strong>Deploy Kitaru</strong></td><td>Self-host for your team.</td><td><a href="deploy/README.md">deploy/README.md</a></td></tr></tbody></table>
