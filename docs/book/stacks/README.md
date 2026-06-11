---
description: Create, inspect, switch, and delete the stacks Kitaru uses for execution
icon: layer-group
---

# Stacks

A **stack** is where the runner places work and where artifacts live. It
bundles these concerns:

- **Execution placement** — the compute backend the runner uses for the run
  itself and for any `runtime="isolated"` checkpoints (local, Kubernetes, AWS,
  GCP, Azure)
- **Artifact persistence** — the bucket or filesystem where checkpoint outputs
  and saved data are written (local, S3, GCS, Azure Blob)
- **Container registry** — where Kitaru pushes the image it builds for remote
  execution
- **Sandbox** — the isolated execution environment Kitaru can use when a
  checkpoint asks for sandboxed work

The active stack is the default; per-flow and per-run overrides can bind a
different stack for a single execution. See
[How It Works](../concepts/how-it-works.md) for how execution
placement interacts with the runner.

## The default stack

After `kitaru init`, you get a `default` stack that runs everything locally:

```bash
kitaru stack current
```

This is enough to develop and test flows on your machine. No cloud accounts or
containers required.

## List available stacks

```bash
kitaru stack list
```

The table view shows each stack ID and marks the active one.

If you need machine-readable output, use JSON:

```bash
kitaru stack list --output json
```

Each list item includes:

- `id`
- `name`
- `is_active`
- `is_managed`

`is_managed` is `true` for stacks created by Kitaru's `stack create` command.

## Switching stacks

```bash
kitaru stack use prod-k8s
```

You can pass either a stack name or a stack ID. The selected stack is persisted
as your default until you switch it again.

Now every `.run()` call uses that stack. You can also override per-run:

```python
my_agent.run(topic="...", stack="prod-k8s")
```

This command changes the fallback stack Kitaru will use when no higher-precedence
override is present. It does **not** rewrite any per-flow or per-run overrides.

## Create a local stack

```bash
kitaru stack create dev
```

By default, Kitaru creates:

- a local orchestrator named `dev`
- a local artifact store named `dev`
- a local sandbox named `dev`
- a stack named `dev`

Then it automatically activates the new stack. The local sandbox uses Kitaru's
installed local sandbox provider. If another sandbox provider is installed in
your environment, you can choose it explicitly:

```bash
kitaru stack create dev --sandbox local
```

You will see output like:

```text
Created stack: dev
Active stack: default → dev
```

If you want to create the stack without switching to it yet:

```bash
kitaru stack create dev --no-activate
```

## Create a remote stack

Today, the CLI and MCP server can provision five shipped stack types:

- `local`
- `kubernetes`
- `vertex`
- `sagemaker`
- `azureml`

These remote stack commands assume you are already connected to the Kitaru
server that should own the stack. If you already have a deployed server,
connect first with `kitaru login ...` and verify with `kitaru status`.

In story form: Kitaru can assemble the stack definition and cloud connector for you, but it still expects the bucket, registry, and any cluster you point at to already exist.

### Kubernetes example

```bash
kitaru stack create prod-k8s \
  --type kubernetes \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com \
  --cluster prod-cluster \
  --region eu-west-1
```

For the end-to-end Kubernetes setup, see [Kubernetes](kubernetes-stacks.md).
For all available orchestrator fields (useful with `--extra`), see the [ZenML Kubernetes orchestrator reference](https://docs.zenml.io/stacks/stack-components/orchestrators/kubernetes).

### Vertex example

```bash
kitaru stack create prod-vertex \
  --type vertex \
  --artifact-store gs://my-bucket/kitaru \
  --container-registry us-central1-docker.pkg.dev/my-project/my-repo \
  --region us-central1
```

Vertex uses a managed runner, so there is no `--cluster` or `--namespace` flag. `kitaru stack show prod-vertex` will report the runner `location` that ZenML stores for the Vertex orchestrator. For all available orchestrator fields (useful with `--extra`), see the [ZenML Vertex orchestrator reference](https://docs.zenml.io/stacks/stack-components/orchestrators/vertex).

### SageMaker example

```bash
kitaru stack create prod-sagemaker \
  --type sagemaker \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com \
  --region eu-west-1 \
  --execution-role arn:aws:iam::123456789012:role/SageMakerExecutionRole
```

SageMaker is also a managed-runner path, so there is no `--cluster` or `--namespace` flag. `kitaru stack show prod-sagemaker` will report the runner region and execution role. For all available orchestrator fields (useful with `--extra`), see the [ZenML SageMaker orchestrator reference](https://docs.zenml.io/stacks/stack-components/orchestrators/sagemaker).

### AzureML example

```bash
kitaru stack create prod-azureml \
  --type azureml \
  --artifact-store az://my-container/kitaru \
  --container-registry demo.azurecr.io/my-team/my-image \
  --subscription-id 00000000-0000-0000-0000-000000000123 \
  --resource-group ml-platform \
  --workspace team-ml \
  --region westeurope
```

AzureML is another managed-runner path, so there is no `--cluster`, `--namespace`, or `--execution-role` flag. `kitaru stack show prod-azureml` will report the runner subscription, resource group, workspace, and location that ZenML stores for the AzureML orchestrator. For all available orchestrator fields (useful with `--extra`), see the [ZenML AzureML orchestrator reference](https://docs.zenml.io/stacks/stack-components/orchestrators/azureml).

Remote stacks do not get a sandbox by default. If you want one, pass
`--sandbox FLAVOR`:

```bash
kitaru stack create prod-k8s \
  --type kubernetes \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com \
  --cluster prod-cluster \
  --region eu-west-1 \
  --sandbox local
```

Kitaru validates the sandbox name and configuration against the providers
available in your current environment. It does not keep a separate hardcoded
list of cloud sandbox providers. The practical consequence is: if the provider
is installed and accepts the configuration, Kitaru can use it; if the provider is
missing or rejects the configuration, Kitaru reports the validation error.

## Use the active stack sandbox from Python

`kitaru.run_sandbox_command(...)` uses the sandbox attached to the currently
active stack. It does not choose by stack type. It reads the stack Kitaru is
already using, finds the one attached sandbox component, creates a temporary
session, runs the command, collects output, and then closes or destroys that
session.

```python
import kitaru

result = kitaru.run_sandbox_command("python --version")
print(result.stdout)
```

If the active stack has no sandbox, Kitaru raises an error and does not run the
command. If the active stack has more than one sandbox, Kitaru also raises an
error instead of guessing. That avoids the bad version of the story: you thought
the command was going to a cheap local sandbox, but Kitaru silently picked a GPU
sandbox and ran it somewhere expensive.

The Gemini Interactions adapter can use this same active-stack sandbox for
caller-owned custom functions. Gemini returns `requires_action`, your code
matches the requested function name against an explicit registry, and Kitaru runs
the registered command through `kitaru.run_sandbox_command(...)`. This does not
redirect Antigravity internals, built-in Gemini code execution, hosted MCP, web
execution, or Google-owned tools into Kitaru; it only covers the function body
your application explicitly executes.

The result is intentionally plain and serializable. Check `exit_code` yourself,
and use `stdout_truncated` / `stderr_truncated` to detect when output hit the
collection limit. If a provider does not support destroying sessions, Kitaru
returns the command output with `cleanup_succeeded=False` and a `cleanup_error`
message after best-effort close.

You can also keep the same inputs in a YAML file and create the stack with:

```bash
kitaru stack create -f stack.yaml
```

CLI flags still override YAML values when both are provided.

## Advanced stack defaults with `--extra` and `--async`

The named stack flags cover the common story: where artifacts live, which registry to use, which cluster or cloud region to target.

Sometimes you need one layer deeper. That is what `--extra` is for.

Think of it like this:

- the named flags are the front desk
- `--extra` is the side door into the underlying stack component defaults

You pass overrides as `TARGET.FIELD=VALUE`, where `TARGET` is one of:

- `orchestrator`
- `artifact_store`
- `container_registry`
- `sandbox`

For example, this Vertex stack sets a pipeline root and leaves the orchestrator asynchronous by default:

```bash
kitaru stack create prod-vertex \
  --type vertex \
  --artifact-store gs://my-bucket/kitaru \
  --container-registry us-central1-docker.pkg.dev/my-project/my-repo \
  --region us-central1 \
  --async \
  --extra orchestrator.pipeline_root=gs://my-bucket/vertex-root
```

`--async` is just a convenience flag for the common case `orchestrator.synchronous=false`.

If you need the explicit setting instead, `--extra` wins:

```bash
kitaru stack create prod-vertex \
  --type vertex \
  --artifact-store gs://my-bucket/kitaru \
  --container-registry us-central1-docker.pkg.dev/my-project/my-repo \
  --region us-central1 \
  --async \
  --extra orchestrator.synchronous=true
```

You can also keep the same advanced defaults in YAML:

```yaml
name: prod-vertex
type: vertex
artifact_store: gs://my-bucket/kitaru
container_registry: us-central1-docker.pkg.dev/my-project/my-repo
region: us-central1
sandbox: local
async: true
extra:
  orchestrator:
    pipeline_root: gs://my-bucket/vertex-root
  container_registry:
    default_repository: agents
  sandbox:
    forward_env: false
    sandbox_environment:
      MY_VAR: value
```

CLI `--extra` values merge on top of YAML `extra:` values instead of replacing the whole object. In story form: the YAML file is your saved blueprint, and the CLI extras are the sticky notes you add for this one build.

Kitaru does not try to duplicate every underlying field in its own docs. For full field inventories, see the ZenML component reference for your orchestrator type: [Kubernetes](https://docs.zenml.io/stacks/stack-components/orchestrators/kubernetes), [Vertex](https://docs.zenml.io/stacks/stack-components/orchestrators/vertex), [SageMaker](https://docs.zenml.io/stacks/stack-components/orchestrators/sagemaker), [AzureML](https://docs.zenml.io/stacks/stack-components/orchestrators/azureml).

## Delete a stack

To delete only the stack record and keep its components:

```bash
kitaru stack delete dev
```

To also remove Kitaru-managed components that are not shared with other stacks:

```bash
kitaru stack delete dev --recursive
```

If the stack you are deleting is currently active, Kitaru protects you by default. Use `--force` to switch back to the default stack first and then continue:

```bash
kitaru stack delete dev --recursive --force
```

## Use the Python SDK

```python
import kitaru

print(kitaru.current_stack())
print(kitaru.list_stacks())

kitaru.create_stack("dev")
kitaru.use_stack("production")
kitaru.delete_stack("dev", recursive=True, force=True)
```

The SDK keeps `StackInfo` intentionally small: `id`, `name`, and `is_active`.

That means `is_managed` is part of structured list output, not part of `StackInfo` itself.

One important scope note: the public Python SDK `kitaru.create_stack(...)` currently provisions local stacks only. Kubernetes, Vertex, SageMaker, and AzureML stack creation are exposed through the CLI and MCP surfaces.

## Precedence with flow-level stack overrides

The active stack is only one layer in the execution precedence chain. Higher layers can override it:

1. `my_flow.run(..., stack="gpu-cluster")`
2. `@flow(stack="gpu-cluster")`
3. `kitaru.configure(stack="gpu-cluster")`
4. `KITARU_STACK`
5. `pyproject.toml` (`[tool.kitaru].stack`)
6. currently active stack

In story form:

- `kitaru stack use prod` changes your persisted default stack
- `kitaru.configure(stack="gpu-cluster")` changes the default only for the current Python process
- `@flow(stack="gpu-cluster")` binds a default to one specific flow definition
- `my_flow.run(stack="gpu-cluster")` overrides everything else for that one execution

Those higher-precedence overrides do **not** change the active stack you see in `kitaru stack current`; they are temporary execution-time bindings.

## Related pages

- [Kubernetes](kubernetes-stacks.md)
- [Containerization](../guides/containerization.md)
- [Flows](../concepts/flows.md)
- [CLI stack commands](https://sdkdocs.kitaru.ai)
