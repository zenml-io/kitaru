---
description: Investigate supplied returns agent traces and test one evidence-led improvement.
icon: graduation-cap
---

# Investigate and improve a returns agent

This tutorial applies Kitaru's complete method to a small customer-support agent. The agent looks up orders, return policies, and shipments, then chooses whether to refund, replace, or escalate each request.

You begin with ten recorded PydanticAI sessions exported from Langfuse. The walkthrough does not reveal or use the example's test-only expected outcomes. You will survey the population, inspect complete traces, record your own judgments, define one observable behavior, freeze its reviewed evidence, and test one bounded agent change.

The tutorial is intentionally more detailed than the [Quickstart](../../getting-started/quickstart.md). It explains what each resource preserves and why each command is part of the evidence chain. Your exact sessions, questions, evaluator, candidate, and result will depend on what you observe.

## Meet the example agent

Each session starts with one synthetic customer ticket. The agent looks up the order, gathers the relevant policy or shipping evidence, chooses one terminal outcome, and returns a structured resolution with a customer reply.

<figure><img src="https://assets.kitaru.ai/docs/diagrams/returns-agent-overview.png" alt="The returns agent reads a ticket, looks up the order, checks shipping or return policy, chooses a resolution, then acts before replying."><figcaption>The lookup tools gather evidence. The action tools record whether a refund, replacement, or escalation actually succeeded.</figcaption></figure>

For return and refund requests, `get_return_policy` supplies the rules for the order's product category. **Final sale** means the item is not eligible for an ordinary return. A reported defect can still qualify when the category has a final-sale defect exception.

| Category | Return window | Final-sale defect exception | Human approval threshold |
| --- | --- | --- | --- |
| Footwear | 30 days | Yes | $150 |
| Apparel | 30 days | Yes | $150 |
| Accessories | 14 days | No | $100 |
| Luggage | 45 days | Yes | $200 |

These values are evidence available to the agent, not guarantees enforced by Kitaru. The tutorial asks you to inspect whether the agent used that evidence correctly and whether the recorded action agrees with its final response.

## What you will build

| Phase | You will create | Why it exists |
| --- | --- | --- |
| [1. Observe](observe.md) | A verified [agent version](../../concepts/agents-and-sessions.md), ten imported [sessions](../../concepts/agents-and-sessions.md), and descriptive [evaluations](../../concepts/evaluators.md) | Confirm what the example preserved and select a bounded, varied review worklist. |
| [2. Judge](judge.md) | An [investigation](../../concepts/investigations.md), evidence-linked annotations, and verdicts | Store what a human concluded without rewriting the trace. |
| [3. Define](define.md) | One accepted behavior, an immutable [cohort version](../../concepts/cohorts.md), and an evaluator version | Turn reviewed evidence into a repeatable measurement. |
| [4. Replay](replay.md) | A candidate agent version, [experiment](../../concepts/experiments.md), and experiment run | Run one bounded change against the frozen population under an explicit tool policy. |
| [5. Compare](compare.md) | Paired baseline and replay evidence | Decide whether the result is improved, regressed, a trade-off, or inconclusive. |

Each page begins with the same five-step map. The first four phase pages end with a **Checkpoint**, and the final page summarizes the complete evidence chain. Because this is evidence-led, placeholders such as `YOUR_SESSION_UUID` are deliberate: substitute IDs produced by your own review rather than copying a predetermined ticket list.

## Prepare the PydanticAI returns agent

Install `jq`, then open the [PydanticAI returns agent README](https://github.com/zenml-io/kitaru/tree/main/examples/python/pydantic_ai_ticket_resolver) and complete its setup through the ten-session confirmation. That README is the source of truth for cloning, entering the example directory, the frozen environment, workspace selection, agent registration, worker startup, and the checked-in Langfuse import. Keep running the commands below from the example directory.

The example uses synthetic customers, orders, shipments, and actions. Refund and replacement tools modify only an isolated in-memory store. No model-provider or Langfuse credentials are needed for setup, import, or the deterministic parts of this tutorial.

Before continuing, confirm these conditions from the example README:

- the selected workspace does not already contain tutorial resources named `returns-resolver`, `returns-discovery`, `returns-regression`, `returns-behavior`, or `returns-candidate`;
- `returns-resolver@1` is registered from the example directory;
- ten imported sessions have the `returns-baseline` tag; and
- the example worker remains running in the second terminal.

Stop and select another workspace if those resource names already exist. Do not delete an existing workspace merely to make its names available.

Some tutorial commands create jobs. The [worker](../../concepts/workers.md) claims those jobs and performs the work in your environment, so the Kitaru server does not receive your agent code or model credentials. Keep the example worker running while you Observe, Judge, and Define. In the Replay phase you will restart it with `OPENAI_API_KEY` before any paid model call.

## Prefer a coding agent?

The pages that follow teach the manual path so you can see each object and boundary. If you want an agent to guide the same evidence loop, install the [Kitaru skills](../../agent-native/setup.md) and use the guided-tour prompt in the [PydanticAI returns agent README](https://github.com/zenml-io/kitaru/tree/main/examples/python/pydantic_ai_ticket_resolver).

## Start the investigation

Continue to [1. Observe the recorded behavior](observe.md).
