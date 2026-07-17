---
description: Version and share durable flow entrypoints for remote invocation.
icon: rocket
---

# Deployments

A **deployment** is a versioned, remotely invocable entrypoint for a Kitaru flow.
It lets a producer publish a flow once and consumers run it from anywhere by name
— without importing the source or owning a long-lived service. Each invocation
starts a fresh durable [execution](executions.md) from a saved snapshot, so every
deployed run is recorded, replayable, and improvable like any other Kitaru flow
run.

The flow source is the recipe, a deployment version is one immutable saved copy
of it, and an invocation starts a fresh execution from that copy.

You can create deployments from three surfaces:

* CLI: `kitaru deploy path/to/file.py:flow_name`
* Python SDK: `flow_name.deploy(...)`
* MCP: `kitaru_deployments_deploy(target="path/to/file.py:flow_name", ...)`

The CLI also has `kitaru build path/to/file.py:flow_name` for the narrower case
where you want to create an immutable deployment version **without** attaching a
route yet. Think of it as putting a sealed build artifact on the shelf. It
exists, it has a version, but nobody reaches it through `default`, `stable`, or
`canary` until you attach a tag later with `kitaru flow tag`.

Deployments are created and invoked in the active Kitaru project, resolved from
your persisted login/project selection, `KITARU_PROJECT`, or an explicit
process-local override. If your active project is `staging`, `kitaru deploy`
creates the deployment in `staging`; switch to `production` and the same command
targets `production` instead.

This page is the concept. For the end-to-end producer/consumer walkthrough —
invocation from CLI, SDK, MCP, and curl, plus auth and server access — see
[Deploy & Invoke](../guides/deployments.md).

## What gets saved

Deploying a flow creates a Kitaru-managed saved snapshot that Kitaru treats as an
immutable deployment version. Kitaru records the public flow name, an integer
version, representative deployment-time input values, deploy-time image config
(when provided), the stack context, and any public routing tags.

Deployment-time inputs should be representative values. They let Kitaru prepare
the saved deployment snapshot, especially for flows whose shape depends on
concrete parameters. Later invocations can override those values by passing new
inputs. Image config is part of the saved snapshot; later invokes can override
flow inputs for each execution, but they do not rewrite the deployment image.

## Auto-versioning

Kitaru assigns deployment versions automatically per flow:

1. The first deployment of `research_agent` becomes version `1`.
2. The next deployment of `research_agent` becomes version `2`.
3. Another flow gets its own independent version sequence.

If two deploys race and both try the same next version, Kitaru retries with the
next available one.

## Tags and routing

Tags are human-readable selectors that point at deployment versions. They are how
producers publish a route and consumers invoke it without memorizing version
numbers.

There are two tag modes:

| Mode | Meaning | Example use |
|---|---|---|
| **Exclusive** | The tag can point to only one version at a time. Adding it to a new version moves it away from older versions. | `default`, `stable`, `prod` |
| **Shared** | The tag can point to multiple versions. Invoking by that tag is only valid when it resolves to one version. | `experiment`, `team-a`, `benchmark` |

The `default` tag is special:

* `default` is reserved by Kitaru.
* `default` is always exclusive, even if you pass `exclusive=False`.
* The first deployment of a flow gets `default` automatically.
* `default` cannot be removed.
* A deployment that still has any exclusive tag cannot be deleted. Move or remove
  the exclusive tag first. Because `default` cannot be removed, move it to another
  version before deleting the old default version.

Deployment creation is only supported for stacks the Kitaru server can execute
remotely from a saved snapshot. If the selected stack is local or otherwise not
remotely executable, deployment creation is rejected (CLI, SDK, and MCP). This
guard keeps deploy-time behavior aligned with invoke behavior.

## Serverless routing

Invoking a deployment starts a new durable Kitaru execution from a saved version.
It does **not** call a long-lived Python process owned by the producer, and it
does **not** create a separate always-on service for each version. The resulting
run records checkpoints exactly like a locally launched flow, so you can replay
and diff it later.

The route is just: **flow name + tag/version selector**.

1. The consumer invokes one flow route, for example `research_agent` + `stable`.
2. Kitaru resolves that route to the saved snapshot for the selected deployment
   version.
3. Kitaru starts a normal execution from that saved snapshot and returns a normal
   execution handle.

That gives a clean producer/consumer split:

* The producer owns source code, deploys versions, and moves tags.
* The consumer only needs a flow name plus a selector (`default`, another tag, or
  an exact version).
* There is no long-lived per-version service and no per-deployment token — access
  is controlled by the same active Kitaru server connection the CLI, SDK, and MCP
  server already use.

Inputs passed at invocation time override the deployment-time defaults for that
new execution.
