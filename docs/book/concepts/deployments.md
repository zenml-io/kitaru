---
description: Version and share durable flow entrypoints for remote invocation.
icon: rocket
---

# Deployments

A **deployment** is a reusable, versioned entrypoint for a Kitaru flow. Think of
it like pinning a recipe card to the wall: the flow source is the recipe, a
deployment version is one saved copy of that recipe, and an invocation starts a
fresh execution from that saved copy.

You can create deployments from three surfaces:

* CLI: `kitaru deploy path/to/file.py:flow_name`
* Python SDK: `flow_name.deploy(...)`
* MCP: `kitaru_deployments_deploy(target="path/to/file.py:flow_name", ...)`

The CLI also has `kitaru build path/to/file.py:flow_name` for the narrower case
where you want to create an immutable deployment version **without** attaching a
route yet. Think of it as putting a sealed build artifact on the shelf. It
exists, it has a version, but nobody reaches it through `default`, `stable`, or
`canary` until you attach a tag later with `kitaru flow tag`.

You can then invoke the deployed flow without the original target path:

* CLI: `kitaru invoke flow_name`
* Python SDK: `flow_name.invoke(...)` or `deployment.invoke(...)`
* MCP: `kitaru_deployments_invoke(flow="flow_name", ...)`

If you want a step-by-step producer/consumer walkthrough, see the
[Deploy and invoke flows guide](../guides/deployments.md).

When you deploy from source targets (`path.py:flow_name`) via CLI, run
`kitaru init` in the repository first so build/deploy-from-source metadata can be
resolved correctly.

## What gets saved

Deploying a flow creates a Kitaru-managed saved snapshot that Kitaru treats as an
immutable deployment version. Kitaru records the public flow name, an integer
version, representative deployment-time input values, deploy-time image config
(when provided), the stack context, and any public routing tags.

Deployment-time inputs should be representative values. They let Kitaru prepare
the saved deployment snapshot, especially for flows whose shape depends on
concrete parameters. Later invocations can override those values by passing new
inputs.

Deploy-time image config follows the same shape across CLI, SDK, and MCP: a base
image string or an object matching `kitaru.ImageSettings`.

Image config is part of the saved deployment snapshot. Later `kitaru invoke` or
`.invoke(...)` calls can override flow inputs for each execution, but they do not
rewrite the deployment image.

Each `kitaru deploy` call attaches exactly one routing tag at deploy time. If you
want to add another tag later, or move an existing route after testing, use
`kitaru flow tag` against the deployed version instead of redeploying.

```bash
kitaru deploy flows/research.py:research_agent \
  --input '{"topic": "durable execution"}' \
  --image '{"requirements":["kitaru[openai]"],"secret_environment_from":["openai-creds"]}'
```

```python
from flows.research import research_agent

research_agent.deploy(
    topic="durable execution",
    image={
        "requirements": ["kitaru[openai]"],
        "secret_environment_from": ["openai-creds"],
    },
)
```

## Auto-versioning

Kitaru assigns deployment versions automatically per flow:

1. The first deployment of `research_agent` becomes version `1`.
2. The next deployment of `research_agent` becomes version `2`.
3. Another flow gets its own independent version sequence.

Internally, Kitaru injects the version into the backend snapshot name using this
shape:

```text
kitaru::<flow>::v<N>
```

For example, `research_agent` version `3` is stored as:

```text
kitaru::research_agent::v3
```

That name is an implementation detail, but it explains the behavior: Kitaru can
scan the existing deployment snapshots for a flow, find the highest `v<N>`, and
allocate the next version. If two deploys race and both try the same next name,
Kitaru retries with the next available version.

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

Concrete routing story:

1. You deploy `research_agent` for the first time. Kitaru creates `v1` and tags
   it `default`.
2. You deploy a new candidate with `--tag canary --exclusive`. Kitaru creates
   `v2` and tags it `canary`.
3. You invoke `kitaru invoke research_agent --tag canary` to test `v2`.
4. When you are happy, you move the stable route:

```bash
kitaru flow tag research_agent stable --version 2 --exclusive
```

That split keeps deploy-time routing simple: create the version with one route,
then use `kitaru flow tag` to mix in later routing changes. For example, you
might deploy version `2` with an exclusive `canary` route, then add a shared
`benchmark` label afterward:

```bash
kitaru flow tag research_agent benchmark --version 2
```

## Remote-executable stack requirement

Deployment creation is only supported for stacks that the Kitaru server can
execute remotely from a saved snapshot. If the selected stack is local or
otherwise not remotely executable by the Kitaru server, deployment creation is
rejected (CLI, SDK, and MCP).

This guard keeps deploy-time behavior aligned with invoke/curl behavior.

## Invocation model

`kitaru invoke` is the primary CLI command for deployed flows:

```bash
kitaru invoke research_agent \
  --tag default \
  --input '{"topic": "serverless routing"}'
```

If you omit both `--version` and `--tag`, Kitaru tries the implicit `default`
route:

```bash
kitaru invoke research_agent --input '{"topic": "default route"}'
```

If the flow has no deployments, Kitaru tells you that directly. If deployments
exist but none is currently routed as `default`, invoke with an explicit tag or
version, or move `default` with `kitaru flow tag ... --exclusive`.

You can pin an exact version when you need reproducibility:

```bash
kitaru invoke research_agent --version 2 --input '{"topic": "pinned run"}'
```

In Python, `.invoke()` is the remote invocation verb for deployed flows:

```python
handle = research_agent.invoke(topic="serverless routing")  # uses tag="default"
result = handle.wait()
```

A `Deployment` object invokes its pinned version:

```python
deployment = research_agent.deployment(version=2)
handle = deployment.invoke(topic="pinned run")
```

At the client level, use the deployment API when the producer flow object is not
imported in the consumer process:

```python
from kitaru import KitaruClient

handle = KitaruClient().deployments.invoke(
    flow="research_agent",
    tag="stable",
    inputs={"topic": "consumer request"},
)
```

## Serverless routing

Deployment invocation starts a new Kitaru execution from a saved deployment
version. It does **not** call a long-lived Python process owned by the producer,
and it does **not** create a separate always-on service for each version.

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
* There is no long-lived per-version service and no per-deployment token.

Inputs passed at invocation time override the deployment-time defaults for that
new execution.

## Authentication and Kitaru server access

Deployments do not have per-deployment tokens. Access is controlled by the same
active Kitaru server connection that the CLI, SDK, and MCP server already use.

For a remote Kitaru server, authenticate once and choose the project you want to
work in:

```bash
kitaru login https://kitaru.example.com --api-key KITARU_API_KEY_VALUE --project production
kitaru status
```

For headless environments, configure the same connection with environment
variables:

```bash
export KITARU_SERVER_URL=https://kitaru.example.com
export KITARU_AUTH_TOKEN=KITARU_API_KEY_VALUE
export KITARU_PROJECT=production
```

For automation, `KITARU_AUTH_TOKEN` should normally be a service-account API key
created with `kitaru auth service-accounts create` and
`kitaru auth api-keys create`. Those three values are the whole connection
puzzle: where the Kitaru server is, how to authenticate to it, and which project
to use once you are there. If any piece is missing, commands that need the server
fail with a short error telling you what to set. `kitaru info` shows which
connection values Kitaru currently sees.

After that, `kitaru invoke`, `KitaruClient().deployments.invoke(...)`, and MCP
`kitaru_deployments_invoke(...)` all use the active Kitaru server connection. The
invocation request does not carry a separate deployment-specific token.

For shell scripts or CI jobs, `kitaru flow deployments curl FLOW` generates a
copy-pasteable curl command for the active Kitaru server. Kitaru resolves the
requested tag or version first, then prints a command that starts a new execution
for that resolved deployment version. The generated snippet calls
`kitaru auth token` to get a short-lived server bearer token from your active
connection, but the curl generator itself does not inline real token values. That
bearer token is temporary; create or rotate long-lived automation credentials
with `kitaru auth api-keys`.

When you generate curl from a tag such as `default` or `stable`, the printed
command is pinned to the deployment version that tag resolved to at generation
time. Regenerate the command if the producer moves the tag later.

Snapshot-backed invocation (`kitaru invoke`,
`KitaruClient().deployments.invoke(...)`, and `kitaru flow deployments curl`)
depends on server workload-manager support. The official `zenmldocker/kitaru`
image already enables this. If you run a custom image or plain ZenML server
setup, preserve or configure workload-manager support explicitly (for example via
`ZENML_SERVER_WORKLOAD_MANAGER_IMPLEMENTATION_SOURCE`).

Runtime secrets for the flow itself should live in Kitaru secrets or stack
configuration, not in deployment tags or invocation examples.

## Worked example: producer deploys and shares

A producer has a flow in `flows/research.py`:

```python
from kitaru import flow

@flow
def research_agent(topic: str) -> str:
    ...
```

They deploy the first default version:

```bash
kitaru deploy flows/research.py:research_agent \
  --input '{"topic": "durable agents"}'
```

They deploy a canary candidate:

```bash
kitaru deploy flows/research.py:research_agent \
  --tag canary \
  --exclusive \
  --input '{"topic": "durable agents"}'
```

They inspect versions and promote the canary to the stable route:

```bash
kitaru flow deployments list research_agent
kitaru flow tag research_agent stable --version 2 --exclusive
```

They can now tell consumers: "Invoke `research_agent` with tag `stable`."

## Worked example: consumer invokes

A CLI consumer invokes the shared route:

```bash
kitaru invoke research_agent \
  --tag stable \
  --input '{"topic": "deployment routing"}'
```

A Python consumer invokes the same route without importing the producer's source
module:

```python
from kitaru import KitaruClient

handle = KitaruClient().deployments.invoke(
    flow="research_agent",
    tag="stable",
    inputs={"topic": "deployment routing"},
)
print(handle.exec_id)
```

An MCP-capable assistant can do the same with structured tool input:

```json
{
  "flow": "research_agent",
  "tag": "stable",
  "inputs": {"topic": "deployment routing"}
}
```

Use `kitaru_deployments_list(flow="research_agent")` or
`kitaru_deployments_get(flow="research_agent", tag="stable")` when the assistant
needs to inspect the available routes before invoking.
