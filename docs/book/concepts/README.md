---
description: The mental model behind Kitaru's durable execution primitives.
icon: lightbulb
---

# Core Concepts

Kitaru sits in the **runtime layer** of your agent stack. Underneath it are the
**harness** your team picked (Pydantic AI, LangGraph, Claude Agent SDK, raw
Python) and the **model(s)** that harness drives. On top sits the **platform**
your org already runs (auth, observability, policy, UI). Kitaru's job is to make
the run durable — checkpoints, replay, resume, wait states, versioned
deployments — without forcing a graph DSL or a rewrite of your control flow.

Start with [Harness, Runtime, Platform](harness-runtime-platform.md) if you want
the big-picture split, or [How It Works](how-it-works.md) for the three-planes
model (control / orchestration / execution) and what-runs-where in local dev vs
production.

## Core ideas

| Concept | What it is |
|---|---|
| **Flow** | The outer durable boundary around your workflow |
| **Checkpoint** | A unit of work inside a flow whose output is persisted |
| **Execution** | A single run of a flow, identified by a unique ID |
| **Structured metadata** | Key-value data you attach to executions and checkpoints with `kitaru.log()` |
| **Runtime log storage** | Where runtime logs are sent (configured separately from structured metadata) |
| **Active stack** | The default execution target used when no per-run `stack=...` override is passed |

## What you can use today

Kitaru's current release includes:

* `@flow` — mark a function as a durable workflow
* `@checkpoint` — mark a function as a persisted work unit
* `kitaru.log()` — attach structured metadata to the current scope
* `kitaru.wait()` — pause a flow until external input is supplied
* `kitaru.llm()` — make tracked model calls with prompt/response capture
* `kitaru.connect()` — connect to a Kitaru server
* `kitaru.configure()` — set process-local runtime defaults
* `kitaru.save()` / `kitaru.load()` — persist and load named artifacts in checkpoints
* `kitaru.list_stacks()` / `kitaru.current_stack()` / `kitaru.use_stack()` — manage the default stack
* `KitaruClient` — inspect executions, fetch logs, resolve waits, retry, replay, and browse artifacts
* `FlowHandle` — interact with a running or finished execution

{% hint style="info" %}
All of the primitives listed here ship today. Some capabilities are
backend-dependent — for example, runtime log retrieval requires a server-backed
connection — but they are part of the supported Kitaru surface.
{% endhint %}

## Explore the concepts

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Harness, Runtime, Platform</strong></td><td>Where Kitaru fits in an agent stack, and where it doesn't.</td><td><a href="harness-runtime-platform.md">harness-runtime-platform.md</a></td></tr><tr><td><strong>How It Works</strong></td><td>Server, runner, execution targets; three planes; local dev vs production.</td><td><a href="how-it-works.md">how-it-works.md</a></td></tr><tr><td><strong>Flows</strong></td><td>Define durable execution boundaries and control how workflows run.</td><td><a href="flows.md">flows.md</a></td></tr><tr><td><strong>Checkpoints</strong></td><td>Break work into persisted units with concurrency support.</td><td><a href="checkpoints.md">checkpoints.md</a></td></tr><tr><td><strong>Logging and Metadata</strong></td><td>Attach structured data to executions and checkpoints.</td><td><a href="logging.md">logging.md</a></td></tr></tbody></table>
