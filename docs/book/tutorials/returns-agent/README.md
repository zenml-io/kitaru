---
description: Complete a guided investigation and replay experiment with a synthetic returns agent.
icon: graduation-cap
---

# Improve a returns agent

This tutorial applies Kitaru's complete method to a small, controlled customer-support agent. The agent looks up orders and return policies, then chooses whether to refund, replace, or escalate each request. You will start with ten recorded support sessions, review an unsafe refund, turn the policy into a regression test, replay changed agent code, and compare the original and new behavior.

The tutorial is intentionally more detailed than the [Quickstart](../../getting-started/quickstart.md). It explains what each command creates, why the resource exists, and how the pieces connect. You will make model calls only during replay. The earlier investigation and evaluation steps operate on supplied traces.

## The scenario

A returns agent handled ten synthetic customer emails. Eight outcomes are correct. In two sessions, it issued a refund when company policy required human approval. The repository includes the ten runs as a Langfuse export, so everyone begins with the same evidence.

The proposed change makes the agent check the policy before acting. A useful test must show both sides of that claim:

- The two unsafe refunds should change to escalations.
- Three valid refunds should remain refunds.

If the agent prevents unsafe refunds by escalating every request, it has not passed the test.

## What you will build

| Phase | You will create | Why it exists |
| --- | --- | --- |
| [1. Observe](observe.md) | An [agent version](../../concepts/agents-and-sessions.md#agents-and-agent-versions), ten imported [sessions](../../concepts/agents-and-sessions.md), and descriptive [evaluations](../../concepts/evaluators.md) | Preserve what happened and find evidence worth reviewing. |
| [2. Judge](judge.md) | An [investigation](../../concepts/investigations.md), annotations, and a verdict | Record what should have happened and why. |
| [3. Define](define.md) | A policy evaluator plus target and control [cohort versions](../../concepts/cohorts.md) | Turn the judgment into a repeatable test. |
| [4. Replay](replay.md) | A candidate agent version, [experiment](../../concepts/experiments.md), and two experiment runs | Run changed code against the same recorded situations. |
| [5. Compare](compare.md) | A baseline-versus-replay result | Decide what the evidence supports and what remains uncertain. |

Each page begins with the same map and marks your current phase. Each ends with a **Checkpoint** that lists the state you should have before continuing. If you lose track, return to that checkpoint rather than re-running earlier commands.

## Before you start

Install Git, Docker, [`uv`](https://docs.astral.sh/uv/), and `jq`. The commands use Bash or Zsh on macOS or Linux.

Clone the repository and install the example's locked dependencies:

```bash
git clone --branch develop https://github.com/zenml-io/kitaru.git
cd kitaru/examples/pydantic_ai_ticket_resolver
uv sync
```

The lockfile installs the published Kitaru packages used to test this tutorial; it does not install the cloned repository source. The agent uses synthetic customers, orders, and actions. Its refund and escalation tools modify only an in-memory store.

Start a local Kitaru workspace and confirm the connection:

```bash
uv run kitaru login --local
uv run kitaru status
```

The workspace opens at [http://localhost:8000](http://localhost:8000). To use an existing deployment instead, run `uv run kitaru login https://your-kitaru-workspace.example.com`.

You do not need an OpenAI API key yet. The supplied traces let you complete Observe, Judge, and Define without calling the model. Replay explains when to add credentials and which commands incur cost.

## Keep two terminals open

Some Kitaru commands create jobs. A [worker](../../concepts/workers.md) in your environment claims those jobs and performs the work. This separation lets Kitaru's server coordinate work without receiving your agent code or model credentials.

Use the terminals for different jobs:

| Terminal | Purpose |
| --- | --- |
| **Terminal 1** | Run the tutorial's CLI commands and inspect results. |
| **Terminal 2** | Keep the worker running and watch task output. |

In Terminal 2, from `examples/pydantic_ai_ticket_resolver`, start the worker:

```bash
uv run kitaru worker start --name returns-tutorial-worker
```

Leave it running. For now, it will process imports and evaluations over recorded data. It will not execute the agent or call a model until the Replay phase.

## Start the investigation

Continue to [1. Observe the recorded behavior](observe.md).
