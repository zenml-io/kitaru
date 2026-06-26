---
description: The mental model behind Kitaru — run, replay, improve.
icon: lightbulb
---

# Core Concepts

Kitaru is the runtime for production AI agents. It records every run as durable
checkpoints, lets you replay a real run with one thing changed, and helps you
roll the winning change across recent runs. The loop is **run → replay →
improve**.

A Kitaru flow is a dynamic ZenML pipeline and a checkpoint is like a step, so
agents run on the same stacks, server, and dashboard as your ZenML pipelines.

1. **Run (durable).** Every model call and tool call is recorded as a
   checkpoint. This is the enabler for everything below, not the headline.
2. **Replay (the differentiator).** Re-execute a real run from a checkpoint. A
   rerun with no change reproduces the original — that faithful baseline is your
   control. Replay again with one input changed (a different model, a different
   prompt) and diff the two. Because the baseline reproduced, the difference is
   your change, not replay noise. This re-executes the real run; it is not
   re-scoring outputs like an eval.
3. **Improve.** Apply the same change across a cohort of recent runs, measure
   cost, latency, and quality, and keep the winner.

Durable execution is the mechanism that makes replay faithful. Start with
[Harness, Runtime, Platform](harness-runtime-platform.md) for where Kitaru fits
in an agent stack, or [How It Works](how-it-works.md) for the three-planes model
(control / orchestration / execution) and what runs where in local dev vs
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
* `flow.run(...).wait()` — run a flow to completion; the handle carries `.exec_id`
* `flow.replay(exec_id, from_="<checkpoint>", **overrides)` — re-execute a recorded run from a checkpoint, optionally overriding flow inputs such as `model` or `prompt_profile`
* `kitaru.log()` — attach structured metadata to the current scope
* `kitaru.wait()` — pause a flow until external input is supplied
* `kitaru.llm()` — make tracked model calls with prompt/response capture
* `kitaru.connect()` — connect to a Kitaru server
* `kitaru.configure()` — set process-local runtime defaults
* `kitaru.save()` / `kitaru.load()` — persist and load named artifacts in checkpoints
* `kitaru.list_stacks()` / `kitaru.current_stack()` / `kitaru.use_stack()` — manage the default stack
* `KitaruClient` — inspect executions, fetch logs, resolve waits, retry, replay, and browse artifacts
* `FlowHandle` — interact with a running or finished execution

Replay and diff are also exposed over an MCP server and the `kitaru` CLI
(`kitaru executions replay --from <checkpoint> <id> --args <json>`), so a coding
agent can drive the run → replay → improve loop directly.

{% hint style="info" %}
All of the primitives listed here ship today. Some capabilities are
backend-dependent — runtime log retrieval, for example, requires a server-backed
connection — but they are part of the supported Kitaru surface.
{% endhint %}

## Explore the concepts

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Harness, Runtime, Platform</strong></td><td>Where Kitaru fits in an agent stack, and where it doesn't.</td><td><a href="harness-runtime-platform.md">harness-runtime-platform.md</a></td></tr><tr><td><strong>How It Works</strong></td><td>Server, runner, execution targets; three planes; local dev vs production.</td><td><a href="how-it-works.md">how-it-works.md</a></td></tr><tr><td><strong>Flows</strong></td><td>Define durable execution boundaries and control how workflows run.</td><td><a href="flows.md">flows.md</a></td></tr><tr><td><strong>Checkpoints</strong></td><td>Break work into persisted units with concurrency support.</td><td><a href="checkpoints.md">checkpoints.md</a></td></tr><tr><td><strong>Logging and Metadata</strong></td><td>Attach structured data to executions and checkpoints.</td><td><a href="logging.md">logging.md</a></td></tr></tbody></table>
