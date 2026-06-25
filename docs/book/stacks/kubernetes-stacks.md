---
description: Create, inspect, use, and clean up Kubernetes-backed stacks in Kitaru
icon: dharmachakra
---

# Kubernetes

A Kubernetes stack gives your flows a remote runner on your own cluster, so a flow you run, replay, and diff locally executes the same way against production infrastructure. This guide covers the full lifecycle: create a stack, inspect it, set it as your default, and delete it safely.

Kubernetes is one shipped remote-stack path. For the broader stack model or a managed-runner option without `--cluster`, start with [Stacks](README.md), which also covers Vertex, SageMaker, and AzureML.

Use this page for the workflow and the happy path. For exact flag syntax and every supported option, see the generated CLI reference for [`kitaru stack create`](https://sdkdocs.kitaru.ai), [`kitaru stack show`](https://sdkdocs.kitaru.ai), and [`kitaru stack delete`](https://sdkdocs.kitaru.ai).

## Before you start

Kitaru assembles the stack for you, but it does not create the bucket, registry, or cluster — those must exist first. You need:

- a Kitaru environment you can already use locally
- a Kubernetes cluster you want Kitaru to run against
- an artifact store URI such as `s3://...` or `gs://...`
- a container registry URI that your cluster can pull from
- cloud credentials, if your setup needs them

## Fast path: create a Kubernetes stack from flags

Here is a realistic AWS-flavored example:

```bash
kitaru stack create prod-k8s \
  --type kubernetes \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com \
  --cluster prod-cluster \
  --region eu-west-1 \
  --namespace ml
```

By default, Kitaru activates the new stack as soon as creation succeeds.

If you want to create it without switching your persisted default stack yet:

```bash
kitaru stack create prod-k8s \
  --type kubernetes \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com \
  --cluster prod-cluster \
  --region eu-west-1 \
  --namespace ml \
  --no-activate
```

The same flow works for GCP-backed stacks. The main difference is that your artifact store starts with `gs://...` and your registry URI should be a GCP registry URI.

If your environment needs explicit credentials, pass them at create time:

```bash
kitaru stack create prod-k8s \
  --type kubernetes \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com \
  --cluster prod-cluster \
  --region eu-west-1 \
  --credentials aws-profile:production
```

For the full option list, see [`kitaru stack create`](https://sdkdocs.kitaru.ai).

## Advanced Kubernetes defaults

Kitaru's named Kubernetes flags cover the basics. When you need a more specific pod or runner default, use `--extra`.

For example, this keeps the normal named flags but adds two deeper orchestrator defaults:

```bash
kitaru stack create prod-k8s \
  --type kubernetes \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com \
  --cluster prod-cluster \
  --region eu-west-1 \
  --namespace ml \
  --async \
  --extra orchestrator.pod_settings.node_selectors.pool=gpu \
  --extra orchestrator.pod_settings.tolerations='[{key: gpu, operator: Exists}]'
```

The named flags say *which* cluster and namespace to use; `--extra` passes additional orchestrator configuration through to the runtime.

You can keep the same advanced defaults in YAML too:

```yaml
name: prod-k8s
type: kubernetes
artifact_store: s3://my-bucket/kitaru
container_registry: 123456789012.dkr.ecr.eu-west-1.amazonaws.com
cluster: prod-cluster
region: eu-west-1
namespace: ml
async: true
extra:
  orchestrator:
    pod_settings:
      node_selectors:
        pool: gpu
```

If you then add a CLI extra on top, the nested mappings merge instead of the CLI replacing the entire YAML `extra:` block.

For the full list of Kubernetes orchestrator fields available to `--extra`, see the [ZenML Kubernetes orchestrator reference](https://docs.zenml.io/stacks/stack-components/orchestrators/kubernetes).

## Inspect what Kitaru created

Once the stack exists, three commands give you different views:

### Show one stack in detail

```bash
kitaru stack show prod-k8s
```

Use this when you want the translated Kitaru view of one stack: runner, storage, image registry, and any additional components.

### Show your current persisted default stack

```bash
kitaru stack current
```

Use this when you want to know which stack Kitaru will fall back to if nothing higher in the precedence chain overrides it.

### List available stacks

```bash
kitaru stack list
```

Use this when you want the wider picture: what exists, which one is active, and which stacks were created as Kitaru-managed stacks.

Reference pages:

- [`kitaru stack show`](https://sdkdocs.kitaru.ai)
- [`kitaru stack current`](https://sdkdocs.kitaru.ai)
- [`kitaru stack list`](https://sdkdocs.kitaru.ai)

## Repeatable path: create from YAML

If you want a stack definition you can keep in the repo or reuse across environments, put the inputs in a YAML file.

Example `stack.yaml`:

```yaml
name: prod-k8s
type: kubernetes
artifact_store: s3://my-bucket/kitaru
container_registry: 123456789012.dkr.ecr.eu-west-1.amazonaws.com
cluster: prod-cluster
region: eu-west-1
namespace: ml
credentials: aws-profile:production
verify: false
activate: false
```

Then create the stack with:

```bash
kitaru stack create -f stack.yaml
```

In YAML, use `snake_case` keys such as `artifact_store` and `container_registry`, and use `verify: false` if you want verification disabled. The file schema does not accept CLI-style keys such as `artifact-store`, `container-registry`, or `no_verify`.

If you provide both YAML values and CLI flags, the CLI values win. That means you can keep most of the configuration in the file and still override one or two fields when needed:

```bash
kitaru stack create prod-k8s-staging \
  -f stack.yaml \
  --region eu-central-1 \
  --namespace staging \
  --no-activate
```

Treat the YAML file as your saved baseline and the CLI flags as per-run overrides.

## Use the stack permanently vs temporarily

There are two distinct moves here. Keep them separate.

### Make it your persisted default

```bash
kitaru stack use prod-k8s
```

This changes the stack Kitaru falls back to when no higher-precedence override is present.

### Use it only for one execution

```python
my_flow.run(stack="prod-k8s")
```

This uses `prod-k8s` for that one execution only — useful when you want a single run or replay on remote infrastructure without changing your persisted default.

The distinction:

- `kitaru stack use ...` changes your persisted fallback stack
- `.run(stack=...)` changes only that one execution
- flow-level and runtime-level stack overrides also remain temporary

So if you do a one-off remote run, `kitaru stack current` should still show the same persisted default afterward.

If you want the full precedence story, see:

- [Configuration](../guides/configuration.md)
- [Flows](../concepts/flows.md)
- [`kitaru stack use`](https://sdkdocs.kitaru.ai)

## Delete safely

There are three common delete paths.

### Delete only the stack record

```bash
kitaru stack delete prod-k8s
```

Use this when you want to remove the stack entry but leave the underlying components alone.

### Delete the stack and clean up Kitaru-managed components

```bash
kitaru stack delete prod-k8s --recursive
```

Use this when you want Kitaru to also remove Kitaru-managed components that are not shared with other stacks.

### Delete an active stack and force a safe switch first

```bash
kitaru stack delete prod-k8s --recursive --force
```

Use this when the stack you are deleting is currently active. Kitaru will switch away first and then continue.

For exact behavior and flags, see [`kitaru stack delete`](https://sdkdocs.kitaru.ai).

## Full reference

When you need exact command syntax instead of the walkthrough, jump to:

- [Stack command overview](https://sdkdocs.kitaru.ai)
- [`kitaru stack create`](https://sdkdocs.kitaru.ai)
- [`kitaru stack show`](https://sdkdocs.kitaru.ai)
- [`kitaru stack use`](https://sdkdocs.kitaru.ai)
- [`kitaru stack delete`](https://sdkdocs.kitaru.ai)
