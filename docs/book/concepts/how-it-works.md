---
description: The Kitaru mental model — a flow is a dynamic pipeline, a checkpoint is a step, and the record → replay → improve loop runs on top.
icon: gears
---

# How It Works

Observability tools watch your agent. Kitaru turns your production traces into a
faithful environment you can re-run — so when you change your agent, you catch
what breaks before your users do. Re-scoring saved outputs against a dataset is
an eval; faithfully re-running the real run is only possible from the runtime
that executed it. This page is the mental model behind that claim, and the
architecture that makes the rebuilt environment faithful enough to trust.

## The mental model

A **Kitaru flow is a dynamic ZenML pipeline**, and a **checkpoint is like a
step**. Your agent runs on the same stacks, the same server, and the same
dashboard as your ZenML pipelines — there is no separate agent runtime to
operate.

The difference from a classical pipeline is that a flow's shape is decided at
runtime by the agent, not fixed in advance. Each `@checkpoint` you cross records
its inputs and output as a durable unit. That recording is what the rest of
Kitaru is built on.

The loop:

1. **Run (record).** Every model call and tool call is recorded as a durable
   checkpoint. This is the enabler, not the headline.
2. **Replay (the point).** Re-execute a real run from a checkpoint with exactly
   one input changed — a different model, a different prompt. Compare it against a
   faithful baseline rerun (the same run with nothing changed). Because the
   baseline reproduces, the diff is your change, not noise. See
   [Replay and overrides](../guides/replay-and-overrides.md).
3. **Improve.** Apply the winning change across a cohort of recent runs, measure
   cost / latency / quality, keep what wins.

{% hint style="info" %}
**Durable execution is the _how_, replay is the _why_.** Recording every
checkpoint is what lets Kitaru reconstruct a run's exact starting state and
re-execute it with one input swapped. Without durable checkpoints you can
re-score outputs (an eval); you cannot faithfully re-run the agent. Replay
re-executes the real run — it is not output re-scoring.
{% endhint %}

The rest of this page is the architecture that makes recording (and therefore
replay) durable.

## Components

When you call `.run()` on a flow, three things work together to make it durable:
the **Kitaru server** (shared metadata, auth, deployment registry), the
**runner** (per-run durable control flow), and one or more **execution targets**
(where each checkpoint's code actually executes). During local development all
three collapse into a single Python process. In production they separate across
your infrastructure.

<figure><img src="https://assets.kitaru.ai/docs/diagrams/components.png" alt="The Kitaru server, runner, and execution targets and how they relate."><figcaption></figcaption></figure>

Kitaru separates **durable control flow** from **code execution**:

* The **Kitaru server** stores shared metadata, deployment snapshots, checkpoint
  state, execution logs, and control-plane data.
* For each run, a **runner** (the durable brain of an execution) executes the
  selected flow snapshot, manages checkpoint order, persists state, and handles
  retry, replay, resume, and wait.
* Individual checkpoints can run **inline** in the runner or in an **isolated**
  runtime (a separate container, Kubernetes job, or cloud job on the configured
  stack). The runner/target split is also where sandboxes, external tools, and
  custom compute backends conceptually plug in — the two shipped execution
  targets today are `inline` and `isolated`.

{% hint style="info" %}
**Key idea.** The runner owns the durable run: checkpoint order, state, retry,
replay, resume, and wait. Execution targets do the work. **Checkpoints are the
contract between the two.**
{% endhint %}

<figure><img src="https://assets.kitaru.ai/docs/diagrams/execution-architecture.png" alt="Execution architecture: the runner delegates checkpoint work to inline or isolated execution targets."><figcaption></figcaption></figure>

## Control / orchestration / execution

Kitaru splits runtime responsibilities into three planes. (This is separate from
the [harness / runtime / platform](harness-runtime-platform.md) split, which is
about where Kitaru sits in the broader agent stack — not about how a single run
executes.)

<figure><img src="https://assets.kitaru.ai/docs/diagrams/three-planes.png" alt="The control, orchestration, and execution planes of a Kitaru run."><figcaption></figcaption></figure>

| Plane | What lives here | Responsibility |
|---|---|---|
| **Control plane** | Kitaru server, UI, metadata DB, deployment registry, CLI/SDK/MCP APIs, auth and credential brokering | Knows what exists and who can call what |
| **Orchestration plane** | The runner for a single execution | Owns durable control flow for one run |
| **Execution plane** | Inline process or isolated container (shipped today); sandboxes, external tools, and custom backends are conceptual extensions of the same contract | Performs work |

The control plane is long-lived and shared. The orchestration plane is per-run
and durable. The execution plane is where your code (and your agent's code)
actually executes.

## What runs where?

The most common confusion is which component runs user code. This table makes it
explicit.

| Component | What it does | Runs user code? |
|---|---|---|
| Kitaru server (control plane) | Stores deployment registry, execution metadata, checkpoint state, log metadata, auth and session state | **No** |
| Runner (orchestration plane) | Runs the selected flow snapshot, controls checkpoint order, persists durable state, handles retry / replay / resume / wait | **Yes**, for inline checkpoints |
| Inline execution | Runs a checkpoint inside the runner process/pod | Yes |
| Isolated runtime | Runs a checkpoint in a separate container, job, pod, or remote compute backend | Yes |
| Sandbox (conceptual) | The same contract as isolated, tightened with stronger isolation or restricted egress. Not a shipped Kitaru execution target today — provided via adapters / your platform. | Yes, where integrated |
| External tool / MCP server | Performs work through a remote API or capability | Outside Kitaru |
| Metadata store | Stores runs, versions, checkpoint statuses, replay lineage | No |
| Artifact / state store | Stores checkpoint outputs, files, logs, replay lineage | No |

## The run, step by step

Here is what actually happens when a consumer invokes a flow.

{% stepper %}
{% step %}
**Request arrives.** A user, service, or upstream agent calls the Kitaru
invocation API (via CLI, SDK, MCP, or HTTP).
{% endstep %}

{% step %}
**Server resolves the flow.** The server authenticates the caller, resolves the
target flow (and optionally a version or tag), validates the input schema, and
creates a run record plus a `FlowHandle`.
{% endstep %}

{% step %}
**Runner starts.** The control plane schedules a runner on your configured stack
— a Kubernetes pod, a cloud job, or the local process in dev. The runner loads
the selected flow snapshot.
{% endstep %}

{% step %}
**Runner executes checkpoints in order.** For each checkpoint, the runner either
executes inline or delegates to an isolated target. It waits for the result,
persists the output to the artifact/state store, and advances.
{% endstep %}

{% step %}
**State is durable the entire time.** If a checkpoint fails, if the runner dies,
or if a `kitaru.wait()` suspends the run, the server retains everything needed to
retry, replay, or resume later.
{% endstep %}

{% step %}
**Consumer observes results.** The caller uses the returned `FlowHandle` (or the
UI / CLI / SDK / MCP) to tail logs, inspect checkpoints, provide human input,
replay, or cancel.
{% endstep %}
{% endstepper %}

## Runner vs sandbox

This is the idea that tends to click last and matter most.

> The runner is the durable brain of a run.<br>
> The sandbox (or isolated runtime) is the hands that perform work.

If a sandbox dies mid-execution — a container evicted, a network partition, a pod
OOM — **the runner still holds durable checkpoint state** and can retry that
single checkpoint, resume from the last known boundary, or replay the run with a
modified input or code version. The sandbox's failure is localized to the
checkpoint that was executing, not the whole agent.

This is why platform teams should not confuse "I have a sandbox provider" with "I
have durable execution". A sandbox is a bounded execution environment. Durable
execution is a property of the surrounding runner — and of the checkpoints it
persists.

## Inline vs isolated checkpoints

Every checkpoint picks an execution target. Two are built in today: `inline`
(same process as the runner) and `isolated` (a separate container or job on the
configured stack). Code examples, decision rules for when to reach for
`isolated`, and the interaction with `.submit()` live on the
[Checkpoints page](checkpoints.md#isolated-runtime).

## A failed checkpoint is durable context

In classical pipelines, a failed step is a crash. In Kitaru, a failed checkpoint
is **durable context** — something the runner, the agent loop, a human, or a
retry policy can reason about.

Consider a document-synthesis agent:

<figure><img src="https://assets.kitaru.ai/docs/diagrams/failed-checkpoint.png" alt="A failed checkpoint is persisted as a typed artifact the runner, agent, or human can act on."><figcaption></figcaption></figure>

Because the retrieval checkpoint's failure is persisted as a typed artifact, a
downstream consumer has several real options:

* Retry the same checkpoint with the same input
* [Replay](../guides/replay-and-overrides.md) the run from a checkpoint with one
  input overridden (e.g. a corrected document id or a different model)
* Replay with modified code (e.g. a new retrieval strategy)
* Feed the error artifact back into the agent loop so it can self-correct
* Wait for a human to provide a correction via `kitaru.wait()`, then resume

This is what "agent-native error handling" means in practice: failures become
data, durable state survives them, and the same recorded run can be re-executed
with one thing changed.

## How deep do you integrate?

You don't have to restructure your agent to get value. Pick the depth that fits.

### Level 0 — Black-box harness

Wrap the entire agent run as one checkpoint.

```text
flow
└── checkpoint: run_agent()
    └── PydanticAI / LangGraph / Claude Agent SDK / custom loop
```

* Fastest integration
* Minimal code changes
* Framework-agnostic

The tradeoff: replay boundary is coarse (one per agent run) and you see less of
the agent's internal state.

### Level 1 — Coarse workflow checkpoints

Add checkpoints around the phases that matter to your team.

```text
flow
├── checkpoint: plan()
├── checkpoint: retrieve()
├── checkpoint: act()
├── checkpoint: synthesize()
└── checkpoint: validate()
```

* Useful replay points
* Better audit trail
* Good balance of portability and durability

The tradeoff: you (not the framework) decide where the boundaries go.

### Level 2 — Framework-aware adapter

Use a Kitaru adapter that tracks the framework's internals (model calls, tool
calls, intermediate state) as child events under the enclosing checkpoint.

```text
flow
└── checkpointed framework runtime
    ├── model calls
    ├── tool calls
    ├── intermediate state
    └── final output
```

* Richer introspection
* Better debugging
* Tighter developer experience

The tradeoff: adapters are per-framework and need maintenance. Supported
framework integrations now live in the [Adapters section](../adapters/README.md).

## Framework-agnostic by construction

Kitaru does not require your agent to be written as a graph. `@checkpoint` wraps
ordinary Python function boundaries, independent of the harness.

That means a platform team supporting multiple harnesses — PydanticAI here,
LangGraph there, Claude Agent SDK for one team, a raw-Python loop for another —
can still standardize **durability, replay, and execution metadata** on a single
runtime primitive. The harness choice stays a per-team decision.

## Fits behind your platform

Kitaru can be used directly through its invocation API, or placed **behind** your
existing platform/gateway:

<figure><img src="https://assets.kitaru.ai/docs/diagrams/gateway-stack.png" alt="Kitaru placed behind an existing platform gateway that owns auth, entitlements, and UI."><figcaption></figcaption></figure>

You keep your auth, entitlements, interceptors, observability, and UI. Kitaru
handles the durable execution layer underneath. This is how Kitaru drops into a
finance- or regulated-industry-style internal agent platform without asking you
to rebuild the surrounding system.

## Local development

When you are developing locally, all three components run inside a single Python
process on your machine. The server is embedded — no separate service to start,
no database to configure. Checkpoint outputs are written to your local
filesystem.

This means you can install Kitaru, run `kitaru init`, and have a fully working
durable execution environment in under a minute. Your flows behave exactly the
same as they will in production — same checkpointing, same replay, same
observability — just without the cloud infrastructure underneath.

Optionally, you can run a
[local server and UI](../getting-started/installation.md#local-ui) to browse
executions in a web UI.

## Production

In production, the three components separate across your infrastructure:

* The **server** runs as a long-lived Kubernetes pod (deployed via
  [Helm](../deploy/helm.md)). It stores execution state in a database and serves
  the UI. Your whole team connects to it.
* The **runner** runs on the compute backend defined by your
  [stack](../stacks/README.md) — Kubernetes, Vertex AI, SageMaker, AzureML. When
  you call `.run()`, the client fetches short-lived credentials from the server
  and dispatches the execution directly to the compute backend. The runner
  executes your checkpoints and writes outputs to cloud storage. If the execution
  crashes, replay picks up from the last completed checkpoint.
* **Artifacts and state** live in your own S3 / GCS / Azure Blob bucket. The
  server tracks metadata but does not access storage directly; when a client
  needs to read files, it fetches temporary credentials brokered by the server.

There is no mandatory SaaS control plane in the path of your agent's data.

## Why an observability tool can't do this

An observability tool that only ever ingested your agent's run data has the logs
— but no way to stand them up. Scoring recorded outputs against a dataset you assembled is one
thing; testing a change against the production history that actually happened
requires rebuilding the conditions the agent ran in — every checkpoint output,
every model call, every tool result — as an environment you can re-run. Only the
runtime that executed the agent holds enough of the run to do that. That is what
the durable machinery on this page exists for: it is what has to be true for the
[replay](../guides/replay-and-overrides.md) environment to be faithful enough
that a change can be tested against what really happened — not a synthetic
dataset — before your users see it.

## Related

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Harness, Runtime, Platform</strong></td><td>Where Kitaru fits in the broader agent stack.</td><td><a href="harness-runtime-platform.md">harness-runtime-platform.md</a></td></tr><tr><td><strong>Flows</strong></td><td>The outer durable boundary of a Kitaru run.</td><td><a href="flows.md">flows.md</a></td></tr><tr><td><strong>Checkpoints</strong></td><td>Durable work units. The contract between the runner and execution targets.</td><td><a href="checkpoints.md">checkpoints.md</a></td></tr><tr><td><strong>Wait and Input</strong></td><td>Pause a run, release compute, resume when input arrives.</td><td><a href="wait-and-input.md">wait-and-input.md</a></td></tr></tbody></table>
