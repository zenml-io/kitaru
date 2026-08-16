---
description: Investigate supplied returns-agent traces and test one evidence-led improvement.
icon: graduation-cap
---

# Investigate and improve a returns agent

This tutorial applies Kitaru's complete method to a small customer-support agent. The agent looks up orders, return policies, and shipments, then chooses whether to refund, replace, or escalate each request.

You begin with ten recorded PydanticAI sessions exported from Langfuse. The walkthrough does not reveal or use the example's test-only expected outcomes. You will survey the population, inspect complete traces, record your own judgments, define one observable behavior, freeze its reviewed evidence, and test one bounded agent change.

The tutorial is intentionally more detailed than the [Quickstart](../../getting-started/quickstart.md). It explains what each resource preserves and why each command is part of the evidence chain. Your exact sessions, questions, evaluator, candidate, and result will depend on what you observe.

## What you will build

| Phase | You will create | Why it exists |
| --- | --- | --- |
| [1. Observe](observe.md) | A registered [agent version](../../concepts/agents-and-sessions.md#agents-and-agent-versions), ten imported [sessions](../../concepts/agents-and-sessions.md), and descriptive [evaluations](../../concepts/evaluators.md) | Preserve what happened and select a bounded, varied review worklist. |
| [2. Judge](judge.md) | An [investigation](../../concepts/investigations.md), evidence-linked annotations, and verdicts | Store what a human concluded without rewriting the trace. |
| [3. Define](define.md) | One accepted behavior, an immutable [cohort version](../../concepts/cohorts.md), and an evaluator version | Turn reviewed evidence into a repeatable measurement. |
| [4. Replay](replay.md) | A candidate agent version, [experiment](../../concepts/experiments.md), and experiment run | Run one bounded change against the frozen population under an explicit tool policy. |
| [5. Compare](compare.md) | Paired baseline and replay evidence | Decide whether the result is improved, regressed, a trade-off, or inconclusive. |

Each page begins with the same five-step map. The first four phase pages end with a **Checkpoint**, and the final page summarizes the complete evidence chain. Because this is evidence-led, placeholders such as `YOUR_SESSION_UUID` are deliberate: substitute IDs produced by your own review rather than copying a predetermined ticket list.

## Before you start

Install Git, Docker, [`uv`](https://docs.astral.sh/uv/), and `jq`. The commands use Bash or Zsh on macOS or Linux.

Clone the repository and install the example's locked dependencies:

```bash
git clone --branch develop https://github.com/zenml-io/kitaru.git
cd kitaru/examples/pydantic_ai_ticket_resolver
uv sync
```

The example uses synthetic customers, orders, shipments, and actions. Refund and replacement tools modify only an isolated in-memory store.

Start a local Kitaru workspace and confirm the connection:

```bash
uv run kitaru login --local
uv run kitaru status
```

The workspace opens at [http://localhost:8000](http://localhost:8000). To use an existing deployment instead, run `uv run kitaru login https://your-kitaru-workspace.example.com`.

This tutorial uses stable names and assumes a fresh workspace in which `returns-resolver`, `returns-discovery`, `returns-regression`, `returns-behavior`, and `returns-candidate` do not already exist. Stop if `kitaru status` selects a workspace containing an earlier run. Use another workspace, or, only if you intend to erase all data in the CLI-managed local workspace, run `uv run kitaru logout --volumes` and then log in locally again.

{% hint style="danger" %} `kitaru logout --volumes` permanently deletes the CLI-managed local PostgreSQL volume, including unrelated Kitaru data in that workspace. Do not run it against data you need. {% endhint %}

## Keep two terminals open

Some commands create jobs. A [worker](../../concepts/workers.md) claims those jobs and performs the work in your environment. This lets the Kitaru server coordinate work without receiving your agent code or model credentials.

| Terminal | Purpose |
| --- | --- |
| **Terminal 1** | Run CLI commands and inspect results. |
| **Terminal 2** | Keep the worker running and watch task output. |

In Terminal 2, from `examples/pydantic_ai_ticket_resolver`, start the worker:

```bash
uv run kitaru worker start --name returns-tutorial-worker
```

Leave it running. Imports and deterministic evaluations use recorded data and do not need an OpenAI key. In the Replay phase you will restart the worker with `OPENAI_API_KEY` before any paid model call.

## Prefer a coding agent?

The pages that follow teach the manual path so you can see each object and boundary. If you want an agent to guide the same evidence loop, install the [Kitaru skills](../../agent-native/skills.md) and use the short `kitaru-investigation` prompt from the [Quickstart](../../getting-started/quickstart.md#use-kitaru-on-your-agent).

## Start the investigation

Continue to [1. Observe the recorded behavior](observe.md).
