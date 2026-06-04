---
description: "The core model: runtime layer, flows, checkpoints, waits, and deployments."
icon: graduation-cap
---

# Learn Kitaru

Learn Kitaru in this order. The goal is not to memorize every primitive; it is
to know where each boundary lives before you start wiring production agents.

## The model in one screen

| Question | Short answer | Deep dive |
|---|---|---|
| Where does Kitaru fit? | Runtime layer under your harness and behind your platform | [Harness, Runtime, Platform](../concepts/harness-runtime-platform.md) |
| What runs user code? | The runner for inline checkpoints; execution targets for isolated work | [How It Works](../concepts/how-it-works.md) |
| What is durable? | Checkpoint outputs, wait state, artifacts, logs, replay lineage | [Checkpoints](../concepts/checkpoints.md) |
| What is not automatic? | Side effects in raw flow-body Python and external systems | [Flows](../concepts/flows.md) |

## Core primitives

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Flows</strong></td><td>The outer durable boundary your app or platform invokes.</td><td><a href="../concepts/flows.md">../concepts/flows.md</a></td></tr><tr><td><strong>Checkpoints</strong></td><td>Replay boundaries where work output is persisted.</td><td><a href="../concepts/checkpoints.md">../concepts/checkpoints.md</a></td></tr><tr><td><strong>Wait and Input</strong></td><td>Suspend a run for a human, webhook, or another system.</td><td><a href="../concepts/wait-and-input.md">../concepts/wait-and-input.md</a></td></tr><tr><td><strong>Logging</strong></td><td>Attach structured metadata and runtime logs to executions.</td><td><a href="../concepts/logging.md">../concepts/logging.md</a></td></tr><tr><td><strong>Deployments</strong></td><td>Versioned flow entrypoints with tags and invocation routing.</td><td><a href="../concepts/deployments.md">../concepts/deployments.md</a></td></tr></tbody></table>

## Mental guardrails

* A flow body is orchestration. Put durable work behind checkpoints.
* A checkpoint is a work boundary. Do not call checkpoints from checkpoints.
* A wait pauses the flow. Keep it at flow scope or adapter-managed flow scope.
* A deployment is a saved versioned entrypoint. Invocation starts a new
  execution from that saved version.
* A stack decides where work runs and where artifacts live.

## Next

Once the model is clear, go to [Build Workflows](../build/README.md) for task
recipes or [Integrations](../integrations/README.md) to wrap an existing harness.
