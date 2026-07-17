---
description: Move a flow from local runs to shared cloud infrastructure, then deploy it as a versioned route others invoke, replay, and improve against.
icon: rocket
---

# Deploy & Invoke

A deployment is a saved, versioned entrypoint for a flow, so other people, systems, or agents can run it by name without importing your source. It turns a flow you run locally into a stable route others invoke, replay, and improve against.

This guide covers both halves of going to production: standing up the shared infrastructure a flow runs on, then the deployment workflow a real team uses — one person publishes a flow, tests a canary version, promotes it, and then others invoke it by name.

The deployment mental model is:

1. **Producer deploys** from source: `kitaru deploy flows/research.py:research_agent`.
2. **Kitaru versions** the deployment automatically: `v1`, then `v2`, then `v3`.
3. **Producer moves tags** like `default`, `canary`, or `stable` to control routes.
4. **Consumer invokes one route** by flow name plus a selector: usually a tag,
   sometimes an exact version. Kitaru resolves that route to the saved snapshot.

## From local to production

Running locally, the Kitaru server is embedded in your Python process. Production
adds three things so your team shares one source of truth and agents run
independently of your laptop. Your flow code does not change.

### 1. Deploy a Kitaru server

Deploy the server as a standalone service. It stores execution metadata,
checkpoint state, and logs; it brokers temporary credentials so clients and the
UI can read artifacts, and never accesses your cloud storage directly.

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Deploy with Helm</strong></td><td>Install the Kitaru server on any Kubernetes cluster.</td><td><a href="../deploy/helm.md">../deploy/helm.md</a></td></tr><tr><td><strong>Deploy with Docker</strong></td><td>Run the server as a container.</td><td><a href="../deploy/docker.md">../deploy/docker.md</a></td></tr></tbody></table>

### 2. Connect to the server

Point your local client at the deployed server:

```bash
kitaru login https://kitaru.your-company.com
```

From here, the CLI, `KitaruClient`, and the UI all talk to the same server, and
any executions you start are visible to your whole team.

### 3. Set up a cloud stack

A [stack](../stacks/README.md) is a named runtime that tells Kitaru where to run
your agent code and where to store its outputs. Pick the compute backend that
matches your cloud — [Kubernetes](../stacks/kubernetes-stacks.md),
[SageMaker](../stacks/sagemaker-stacks.md), [Vertex AI](../stacks/vertex-stacks.md),
or [AzureML](../stacks/azureml-stacks.md) — then switch to it:

```bash
kitaru stack use prod-k8s
```

Now `research_agent.run(...)` executes on that stack: the client fetches
short-lived credentials from the server and dispatches the execution to your
compute backend, which writes checkpoint outputs to your cloud storage. Every
cloud run records the same durable checkpoints as a local run, so you can replay a
production execution with one input changed and diff it against the original.

## The story: a research flow you want to share

Imagine you have this flow in `flows/research.py`:

```python
from kitaru import checkpoint, flow

@checkpoint
def collect_notes(topic: str) -> str:
    ...

@checkpoint
def write_summary(notes: str) -> str:
    ...

@flow
def research_agent(topic: str) -> str:
    notes = collect_notes(topic)
    return write_summary(notes)
```

While developing locally, you run it directly from source:

```python
research_agent.run(topic="durable execution").wait()
```

A deployment is the next step: a saved, versioned entrypoint other processes can
invoke without importing `flows/research.py`.

## Step 1: deploy the first version

If you deploy from a source target (`path.py:flow_name`), initialize the
repository first:

```bash
kitaru init
```

Then deploy from the source target:

```bash
kitaru deploy flows/research.py:research_agent \
  --input '{"topic": "durable execution"}' \
  --image '{"requirements":["kitaru[openai]"],"secret_environment_from":["openai-creds"]}'
```

The `--input` value should contain representative deployment-time input values.
Kitaru uses them to prepare the saved deployment snapshot. Consumers can
override those values later when they invoke the deployment.

`--image` is for deploy-time image config. It accepts a base image string, a
JSON object matching `kitaru.ImageSettings`, or `@file`. For `@file`, you can
use either plain text (for example a file containing `python:3.12-slim`) or a
JSON string/object. That image config is saved into the deployment snapshot.
Later invokes can override flow inputs, but they do not rewrite the deployment
image.

Each `kitaru deploy` command attaches exactly one routing tag at deploy time.
That first deploy gets the reserved `default` route automatically. Later deploys
can choose one other route such as `canary`, and any extra tags are added or
moved afterward with `kitaru flow tag`.

Because this is the first deployment for `research_agent`, Kitaru creates:

| Flow | Version | Tags |
|---|---:|---|
| `research_agent` | `1` | `default*` |

The `*` means the tag is **exclusive**. Exclusive tags point to exactly one
version at a time.

You can inspect the versions:

```bash
kitaru flow deployments list research_agent
kitaru flow deployments show research_agent
```

`show` defaults to the `default` tag when you do not pass `--version` or `--tag`.

### Build a version without routing it

Sometimes you want the saved deployment version first, but you do **not** want consumers to hit it yet. Use `kitaru build` for that:

```bash
kitaru build flows/research.py:research_agent \
  --input '{"topic": "durable execution"}'
```

That creates the immutable version but leaves it untagged. Later, after review or smoke testing, attach a route explicitly:

```bash
kitaru flow tag research_agent canary --version 2 --exclusive
```

The practical difference is simple: `kitaru deploy` creates a version **and** one route; `kitaru build` creates only the version.

## Step 2: invoke the default route

Now invoke the deployed flow by name:

```bash
kitaru invoke research_agent \
  --input '{"topic": "serverless routing"}'
```

Because you did not specify `--version` or `--tag`, Kitaru tries the reserved
implicit `default` route. It resolves `research_agent` + `default` to the saved
deployment snapshot for that version, starts a new execution from it, and
returns an execution ID.

If the flow has no deployments yet, Kitaru says so directly. If deployments
exist but none is currently routed as `default`, invoke with `--tag` or
`--version`, or move `default` with `kitaru flow tag ... --exclusive`.

The same invocation from Python looks like this:

```python
from kitaru import KitaruClient

handle = KitaruClient().deployments.invoke(
    flow="research_agent",
    inputs={"topic": "serverless routing"},
)
print(handle.exec_id)
```

If you have the flow object imported in the current process, `.invoke()` is the
remote invocation verb:

```python
from flows.research import research_agent

handle = research_agent.invoke(topic="serverless routing")
```

## Step 3: deploy a canary version

Now imagine you improve the flow and want to test the new behavior without
moving the default route yet. Deploy it with an exclusive `canary` tag:

```bash
kitaru deploy flows/research.py:research_agent \
  --tag canary \
  --exclusive \
  --input '{"topic": "durable execution"}'
```

Kitaru creates version `2` and attaches `canary*`:

| Flow | Version | Tags |
|---|---:|---|
| `research_agent` | `1` | `default*` |
| `research_agent` | `2` | `canary*` |

At deploy time, that is still just one route. If you later want to add another
label without redeploying, do it with `kitaru flow tag`, for example:

```bash
kitaru flow tag research_agent benchmark --version 2
```

That gives version `2` an exclusive `canary` route plus a shared `benchmark`
label.

Now you can test only the canary route:

```bash
kitaru invoke research_agent \
  --tag canary \
  --input '{"topic": "tag routing"}'
```

This is the safe pattern: deploy a candidate, invoke it explicitly, inspect the
execution, and only then move the stable route.

## Step 4: promote a version

There are two common promotion styles.

### Option A: move `default`

If your consumers use the implicit default route, move `default` to version `2`:

```bash
kitaru flow tag research_agent default --version 2 --exclusive
```

After that, plain `kitaru invoke research_agent ...` routes to version `2`.
The old version no longer has `default`.

### Option B: publish a separate `stable` route

If you prefer an explicit production route, move `stable` to version `2`:

```bash
kitaru flow tag research_agent stable --version 2 --exclusive
```

Consumers then invoke:

```bash
kitaru invoke research_agent \
  --tag stable \
  --input '{"topic": "consumer request"}'
```

This is nice when you want `default` for interactive testing, while `stable` is
what other systems use.

## Producer vs consumer responsibilities

A useful way to keep the model straight:

| Role | Needs source target? | Typical commands |
|---|---|---|
| Producer | Yes, for deploys | `kitaru deploy`, `kitaru flow tag`, `kitaru flow deployments list` |
| Consumer | No | `kitaru invoke`, `KitaruClient().deployments.invoke(...)`, MCP `kitaru_deployments_invoke` |

The producer knows `flows/research.py:research_agent`. The consumer only needs
`research_agent` plus a selector such as `default`, `stable`, `canary`, or `v2`.

## When selectors matter

Most of the time, consumers should use a tag:

```bash
kitaru invoke research_agent --tag stable --input '{"topic": "..."}'
```

Use an exact version when you need reproducibility:

```bash
kitaru invoke research_agent --version 2 --input '{"topic": "..."}'
```

A version is a hard pin. It will not move when someone promotes `stable` or
`default` later. Tags are movable route names; versions are the saved deployment
snapshots those routes resolve to.

### Exclusive tags

Use exclusive tags for routes that should point to one version:

- `default`
- `stable`
- `prod`
- `canary` when there is only one current canary

`default` is always exclusive and reserved. You cannot remove it directly. To
move it, attach `default` to another version with `--exclusive`.

A deployment that still holds any exclusive tag cannot be deleted. Move the tag
first, then delete the old version:

```bash
kitaru flow tag research_agent default --version 2 --exclusive
kitaru flow deployments delete research_agent --version 1
```

### Shared tags

Non-exclusive tags can point to more than one version. A common mixed pattern
is: deploy one exclusive route at create time (`--tag canary --exclusive`), then
add shared labels later with `kitaru flow tag`. Shared tags are useful for
marking a group of deployments:

```bash
kitaru flow tag research_agent benchmark --version 1
kitaru flow tag research_agent benchmark --version 2
```

But shared tags are not good invocation routes once they point to multiple
versions. If `benchmark` points to both `v1` and `v2`, then this selector is
ambiguous:

```bash
kitaru invoke research_agent --tag benchmark --input '{"topic": "..."}'
```

When a tag should be invocable by consumers, make it exclusive.

## Generate a curl command

If you need to trigger a deployment from a shell script, CI job, or another
system that can make HTTP requests, ask Kitaru to generate the curl command for
the active Kitaru server.

Before you do that, make sure Kitaru knows three things:

1. the Kitaru server URL,
2. an auth token that can access that server, usually a service-account API key,
3. the project to use on that server.

For CI and other automation, create a service-account API key first:

```bash
kitaru auth service-accounts create ci-runner \
  --description "CI runner for deployment invocation"
kitaru auth api-keys create ci-runner default -o json
```

Store the one-time `key` value from the create response in your secret manager.
Then provide the connection by logging in once:

```bash
kitaru login https://kitaru.example.com --api-key KITARU_API_KEY_VALUE --project production
```

Or, in CI and other headless environments, with environment variables:

```bash
export KITARU_SERVER_URL=https://kitaru.example.com
export KITARU_AUTH_TOKEN=KITARU_API_KEY_VALUE
export KITARU_PROJECT=production
```

Run `kitaru info` if you want to check which connection Kitaru is using. If one
of those three pieces is missing, `kitaru auth token` exits with a short error
that says what to set next.

Then generate the curl command:

```bash
kitaru flow deployments curl research_agent \
  --tag stable \
  --input '{"topic": "consumer request"}'
```

The output is copy-pasteable and uses your active connection's server URL:

```bash
# Resolved research_agent tag 'stable' to deployment version v2.
# This command is pinned to v2. Regenerate it if you move the tag.

KITARU_SERVER_ACCESS_TOKEN="$(kitaru auth token)"

curl -sS -X POST \
  'https://kitaru.example.com/api/v1/pipeline_snapshots/7d3176d6-7453-411b-a3f5-91ca5c663d1c/runs' \
  -H "Authorization: Bearer ${KITARU_SERVER_ACCESS_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{"run_configuration":{"parameters":{"topic":"consumer request"}}}'
```

Kitaru resolves `research_agent` plus `stable` before printing the command. That
means the generated URL is pinned to the resolved deployment version. If the
producer later moves `stable` to another version, regenerate the command.

`kitaru auth token` exchanges your active Kitaru connection for a short-lived
server bearer token. It does not create a long-lived API key; use
`kitaru auth api-keys create` or `kitaru auth api-keys rotate` for that. The
generated curl snippet stores the short-lived bearer token in
`KITARU_SERVER_ACCESS_TOKEN` for the current shell command only; the curl
generator itself does not print or store the real token.

One important practical detail: deployment creation is rejected up front when
the selected stack is local or otherwise not executable remotely by the Kitaru
server. Use a stack the Kitaru server can execute remotely (for example
Kubernetes, Vertex, SageMaker, or AzureML) for deployment routes you plan to
invoke remotely.

For older deployments created before this guard existed, `kitaru invoke` and
`kitaru flow deployments curl` may fail with a stack-compatibility error until
you redeploy using a stack the Kitaru server can execute remotely.

The official `zenmldocker/kitaru` server image already enables workload-manager
support for snapshot-backed invocation. If you run a custom image or a plain
ZenML server setup, preserve or configure workload-manager support explicitly
(e.g. set `ZENML_SERVER_WORKLOAD_MANAGER_IMPLEMENTATION_SOURCE`) so deployment
invoke/curl routes remain executable.

For machine-readable output, add `-o json`:

```bash
kitaru flow deployments curl research_agent --version 2 -o json
```

## Invoke from MCP

MCP clients use the same deployment model with structured tool calls.

Deploy a canary:

```json
{
  "target": "flows/research.py:research_agent",
  "inputs": {"topic": "durable execution"},
  "tag": "canary",
  "exclusive": true,
  "image": {
    "requirements": ["kitaru[openai]"],
    "secret_environment_from": ["openai-creds"]
  }
}
```

MCP `image` accepts the same shapes as CLI/SDK deploy: a base image string or
an object matching `kitaru.ImageSettings`.

Invoke the stable route:

```json
{
  "flow": "research_agent",
  "tag": "stable",
  "inputs": {"topic": "consumer request"}
}
```

Use `kitaru_deployments_list` and `kitaru_deployments_get` before invoking when
the assistant needs to discover available versions or confirm where a tag points.

## Auth: one workspace context, no deployment tokens

Deployments do not have their own tokens. The CLI, SDK, and MCP server all use
the active Kitaru connection context.

For a local server:

```bash
kitaru login
kitaru status
```

For a remote workspace:

```bash
kitaru login my-workspace --api-key KITARU_API_KEY_VALUE --project production
kitaru status
```

For headless environments:

```bash
export KITARU_SERVER_URL=https://kitaru.example.com
export KITARU_AUTH_TOKEN=KITARU_API_KEY_VALUE
export KITARU_PROJECT=production
```

Once that context is configured, deployment invocation uses it automatically.
A consumer invokes one route by flow name plus tag/version; Kitaru resolves that
route to the saved snapshot. There is no long-lived per-version service and no
extra per-deployment token to pass to `kitaru invoke`, `.invoke()`, or
`kitaru_deployments_invoke`.

## A practical checklist

Before sharing a route with other people or agents:

1. `kitaru deploy ... --tag canary --exclusive` for the candidate.
2. `kitaru invoke FLOW --tag canary ...` and inspect the execution.
3. Move `stable` or `default` with `kitaru flow tag FLOW TAG --version N --exclusive`.
4. Tell consumers the flow name and tag, not the source file path.
5. Use exact `--version N` only for reproducible one-off runs.

If you remember just one thing: deploys create Kitaru-managed saved versions;
tags are the movable signposts consumers follow.
