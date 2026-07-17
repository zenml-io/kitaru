---
description: The one artifact Kitaru produces — the execution — and the verb you apply to it.
icon: lightbulb
---

# Core Concepts

Kitaru produces exactly one artifact: the **execution**, a recording of a run in
which every model call and tool call is captured at a checkpoint boundary. The
verb you apply to it is **replay** — re-execute the recorded run with your real
code, unchanged for a faithful baseline or with one thing swapped to isolate a
change. Everything else on this page exists to make that recording durable and
that replay faithful.

A Kitaru flow is a dynamic ZenML pipeline and a checkpoint is like a step, so
agents run on the same stacks, server, and dashboard as your ZenML pipelines.

1. **Run (durable).** Every model call and tool call is recorded as a
   checkpoint. This is the enabler for everything below, not the headline.
2. **Replay (the verb).** Re-execute a real run from a checkpoint. A rerun with
   no change reproduces the original — that faithful baseline is your control.
   Replay again with one input changed (a different model, a different prompt)
   and diff the two. Because the baseline reproduced, the difference is your
   change, not replay noise. This re-executes the real run; it is not re-scoring
   outputs like an eval.
3. **Improve.** Apply the same change across a cohort of recent runs, measure
   cost, latency, and quality, and keep the winner.

Durable execution is the mechanism that makes replay faithful. Start with
[Executions — the recording](executions.md) for what a finished run leaves
behind, or [Under the Hood](under-the-hood.md) for the server/runner/execution-target
architecture and where Kitaru sits in an agent stack.

## Core ideas

| Concept | What it is |
|---|---|
| **Execution** | The one artifact — a recording of a single run, identified by a unique `exec_id` |
| **Flow** | The outer durable boundary that produces an execution |
| **Checkpoint** | A recorded boundary inside a flow whose output replay reads back |
| **Structured metadata** | Key-value data you attach to executions and checkpoints with `kitaru.log()` |
| **Runtime log storage** | Where runtime logs are sent (configured separately from structured metadata) |
| **Active stack** | The default execution target used when no per-run `stack=...` override is passed |

## What you can use today

Kitaru's current release includes:

* `@flow` — mark a function as a durable workflow
* `@checkpoint` — mark a function as a persisted work unit
* `flow.run(...).wait()` — run a flow to completion; the handle carries `.exec_id`
* `flow.replay(exec_id, at="<checkpoint>", flow_overrides={...})` — re-execute a recorded run from a checkpoint, optionally overriding flow inputs such as `model` or `prompt_profile`
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
(`kitaru executions replay <id> --at <checkpoint> --flow-overrides <json>`),
so a coding agent can drive the run → replay → improve loop directly.

{% hint style="info" %}
All of the primitives listed here ship today. Some capabilities are
backend-dependent — runtime log retrieval, for example, requires a server-backed
connection — but they are part of the supported Kitaru surface.
{% endhint %}

## Explore the concepts

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Executions — the recording</strong></td><td>What a finished run leaves behind, and why imported traces are read-only.</td><td><a href="executions.md">executions.md</a></td></tr><tr><td><strong>Flows</strong></td><td>The boundary that produces an execution.</td><td><a href="flows.md">flows.md</a></td></tr><tr><td><strong>Checkpoints</strong></td><td>The recorded boundaries replay reads back.</td><td><a href="checkpoints.md">checkpoints.md</a></td></tr><tr><td><strong>Wait, Input &#x26; Resume</strong></td><td>Pause a run for external input and continue the same execution.</td><td><a href="wait-and-input.md">wait-and-input.md</a></td></tr><tr><td><strong>Logging &#x26; Metadata</strong></td><td>Attach structured data to executions and checkpoints.</td><td><a href="logging.md">logging.md</a></td></tr><tr><td><strong>Under the Hood</strong></td><td>Server, runner, execution targets, and where Kitaru fits in an agent stack.</td><td><a href="under-the-hood.md">under-the-hood.md</a></td></tr></tbody></table>
