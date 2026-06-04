---
description: Where Kitaru fits — and doesn't — in an agent stack.
icon: layer-group
---

# Harness, Runtime, Platform

Agent tooling spans four layers. Confusion between them is where most "is Kitaru
a competitor to X?" questions come from.

<figure><img src="https://assets.kitaru.ai/docs/diagrams/harness-runtime-platform.png" alt="The four layers of an agent stack: model, harness, runtime, and platform."><figcaption></figcaption></figure>

* **Model layer** — the LLM itself. A compute unit over a context window, picked
  per-call or per-agent: OpenAI, Anthropic, Google, open-weights, fine-tuned
  in-house.
* **Harness layer** — the loop around the model. Prompts, tools, model loop,
  context management, structured outputs, in-turn memory. Picked per-agent or
  per-team.
* **Runtime layer** — how the agent survives and executes over time. Checkpoints,
  replay, resume, wait states, versioned deployments, invocation routing,
  artifact + state handling, execution placement.
* **Platform layer** — how the organization governs. Auth, entitlements,
  interceptors, observability, product UI, policy. Usually lives in your existing
  stack.

{% hint style="info" %}
**Kitaru sits in the runtime layer.** It is not a harness and it is not a
packaged platform. It gives platform teams the durable execution primitives they
attach to the harness their app teams picked and the platform their org already
runs.
{% endhint %}

## Where Kitaru is — and isn't

| Tool | Primary layer | What it optimizes for |
|---|---|---|
| Pydantic AI / Pydantic AI Harness | Harness | Typed, ergonomic Python agent logic |
| Claude Agent SDK | Harness | Claude-native autonomous coding / tool loops |
| OpenAI Agents SDK | Harness | Hosted-tool agents on the OpenAI stack |
| LangGraph | Harness + runtime (in its own model) | Graph-native agents with built-in checkpointer |
| Deep Agents | Harness (on LangGraph) | Opinionated multi-agent pattern |
| LangSmith Deployment | Runtime + platform (packaged) | Adopting the LangChain-hosted stack |
| Temporal | Runtime (general-purpose) | Polyglot, deterministic workflow engine |
| DBOS | Runtime (general-purpose) | Postgres-backed durable workflows |
| **Kitaru** | **Runtime (Python-agent-shaped)** | **Framework-agnostic durable execution primitives** |

## The overlap

Several tools in the runtime row are real alternatives to Kitaru. Worth naming
the overlap before drawing the distinction.

* **LangGraph** has its own checkpointer, resume, and time-travel — powerful
  inside its graph/state-machine model. Kitaru's difference is that
  `@checkpoint` wraps ordinary Python boundaries independent of any harness.
* **LangSmith Deployment** delivers durable execution + sandboxes + auth proxy as
  a packaged platform. Kitaru ships just the runtime primitives so platform teams
  bring their own auth, sandbox provider, and governance.
* **Temporal** is a battle-tested polyglot durable workflow engine. Kitaru is
  Python-first, agent-shaped (first-class `kitaru.llm()`, `kitaru.wait()`,
  artifact lineage), with a simpler single-service deployment.
* **DBOS** is a Postgres-backed durable workflow library with deterministic
  workflow bodies. Kitaru flows are plain Python with no determinism requirement;
  state and artifacts live in your own cloud bucket, not Postgres.

## Two worldviews

```text
Harness-first
  "Let's give developers a better way to build agents"
    → agent logic → tools → state → deployment

Runtime-first (Kitaru)
  "Agent work is long-running infrastructure work"
    → runtime → checkpoints → execution targets → harness integration
```

Neither is universally better. They optimize for different buyers.

<figure><img src="https://assets.kitaru.ai/docs/diagrams/buyer-matrix.png" alt="A matrix contrasting harness-first and runtime-first buyers."><figcaption></figcaption></figure>

## What Kitaru owns vs integrates with

Platform teams rightly push back on tools that try to own *everything*. What
Kitaru actually takes responsibility for:

| Concern | Kitaru owns? | Kitaru's stance |
|---|---|---|
| Checkpoint / replay / resume | Yes | Core product |
| Flow versioning and invocation routing | Yes | Core product |
| Execution placement per checkpoint | Yes, as config | `@checkpoint(runtime="isolated")` today; richer policy evolving |
| Sandbox implementation | No | Provide adapters; don't mandate a vendor |
| Secrets storage | Partly | Alias-linked secret resolution for `kitaru.llm()`; integrate with your secret manager |
| Auth to invoke flows | Yes | Workspace keys / service accounts; no per-deployment tokens |
| Enterprise entitlements / RBAC | No | Integrate with your platform |
| Network egress policy | No | Determined by the execution target your stack provides; Kitaru does not enforce it |
| Interceptors / guardrails | No | Harness or your platform owns this |
| Observability | Partly | Runtime metadata, logs, artifact lineage; integrate with your tracing |
| Data compliance policy | No | Policy stays with your platform; Kitaru does not mandate one |

The line to remember:

> Durability without execution policy is not enough for production agents — but
> Kitaru should make policy **attachable** to execution boundaries, not mandate
> the policy itself.

## Concrete split in code

A Python research agent, with each layer doing its part:

```python
from kitaru import flow, checkpoint, wait

@checkpoint
def plan(question: str) -> dict:
    # Harness (Pydantic AI / raw LLM / whatever) lives INSIDE the checkpoint.
    return pydantic_agent.run_sync(question).output

@checkpoint
def retrieve(plan: dict) -> list[dict]:
    return search_docs(plan)

@checkpoint
def synthesize(docs: list[dict]) -> str:
    return claude_agent.answer(docs)

@flow
def research_agent(question: str) -> str:
    p = plan(question)
    docs = retrieve(p)
    approved = wait(name="approve", question="Looks right?", schema=bool)
    return synthesize(docs) if approved else "rejected"
```

* **Harness** decides how `plan`, `retrieve`, `synthesize` reason.
* **Kitaru runtime** decides what is durable, what can replay, what waits, where
  each checkpoint runs.
* **Your platform** decides who can invoke `research_agent`, which stack it runs
  on, and what gets logged where.

## When Kitaru is the wrong size

* If your whole org standardizes on LangGraph + LangSmith, Kitaru adds less. Use
  what you have.
* If you are building one agent for yourself and never leave your laptop, a
  harness alone is enough.
* If you want a hosted, all-in-one agent platform and don't need to self-host
  anything, a packaged platform is the better buy.

## When Kitaru fits

* Application teams across your org pick different harnesses (Pydantic AI,
  Langchain's Deep Agents, Claude Agent SDK, internal).
* Infra must be self-hosted (regulated industry, on-prem requirements,
  sovereignty).
* The platform team wants runtime primitives, not a packaged platform that
  replaces the one they already operate.
* Deployment must plug into existing Kubernetes, secret manager, observability,
  and data policy — not live in someone else's control plane.
* Durable execution needs to be independent of any single framework's worldview.

## Shorthand

> **Harnesses define behavior. Kitaru defines durable execution. Platforms define
> governance.**

Or the even shorter version:

> Use a harness to build the agent. Use Kitaru when that agent becomes a durable,
> versioned, self-hosted production workload.

## Related

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>How It Works</strong></td><td>Server, runner, execution targets; three planes; local dev vs production.</td><td><a href="how-it-works.md">how-it-works.md</a></td></tr><tr><td><strong>Flows</strong></td><td>The outer durable boundary.</td><td><a href="flows.md">flows.md</a></td></tr><tr><td><strong>Checkpoints</strong></td><td>The contract between the runtime and your code.</td><td><a href="checkpoints.md">checkpoints.md</a></td></tr></tbody></table>
