---
description: Create, inspect, and use Vertex AI-backed stacks with GCS storage
icon: google
---

# GCP (Vertex AI)

A Vertex AI stack runs each Kitaru execution as a managed Vertex AI job and stores checkpoint outputs in GCS.

Use this page when your team wants Google-managed compute rather than a Kubernetes cluster. If you want the broader stack model first, start with [Stacks](README.md).

## Prerequisites

Before creating the stack, make sure these resources already exist:

- a Kitaru server you are connected to with `kitaru login ...`
- a GCS bucket or prefix for artifacts, for example `gs://my-bucket/kitaru`
- an Artifact Registry repository that can store the execution image, for example `us-central1-docker.pkg.dev/my-project/my-repo`
- Google Cloud credentials available to the Kitaru server / stack setup path
- a Vertex AI region, for example `us-central1`

Kitaru creates the stack definition and component records. It does not create your GCS bucket, Artifact Registry repository, IAM roles, or project-level Vertex AI setup for you.

## Create the stack

```bash
kitaru stack create prod-vertex \
  --type vertex \
  --artifact-store gs://my-bucket/kitaru \
  --container-registry us-central1-docker.pkg.dev/my-project/my-repo \
  --region us-central1
```

The required Vertex fields are:

| Field | Meaning |
|---|---|
| `--artifact-store` | GCS URI where Kitaru writes checkpoint outputs and saved artifacts |
| `--container-registry` | Artifact Registry repository where Kitaru pushes the run image |
| `--region` | Vertex AI region for managed jobs |

You can add an optional credentials reference with `--credentials` when your server setup uses named cloud credentials.

## Set advanced Vertex defaults

Named flags cover the common setup. Use `--extra` for lower-level component fields that Kitaru does not expose as first-class flags.

For example, set a Vertex pipeline root and make the orchestrator asynchronous by default:

```bash
kitaru stack create prod-vertex \
  --type vertex \
  --artifact-store gs://my-bucket/kitaru \
  --container-registry us-central1-docker.pkg.dev/my-project/my-repo \
  --region us-central1 \
  --async \
  --extra orchestrator.pipeline_root=gs://my-bucket/vertex-root
```

`--async` is shorthand for `--extra orchestrator.synchronous=false`. If you provide both, the explicit `--extra` value wins.

If you need provider-specific settings not shown here, keep them in a reviewed stack YAML template and pass them through `extra:` / `--extra`.

## Use YAML for repeatable setup

```yaml
name: prod-vertex
type: vertex
artifact_store: gs://my-bucket/kitaru
container_registry: us-central1-docker.pkg.dev/my-project/my-repo
region: us-central1
async: true
extra:
  orchestrator:
    pipeline_root: gs://my-bucket/vertex-root
```

Create it with:

```bash
kitaru stack create -f stack.yaml
```

CLI flags override YAML values, and `--extra` values merge on top of the YAML `extra:` block.

## Inspect and use it

```bash
kitaru stack show prod-vertex
kitaru stack use prod-vertex
kitaru stack current
```

`kitaru stack show` reports the translated Kitaru view: runner, storage, image registry, region, active status, and whether the stack was created by Kitaru.

Once active, normal flow runs use the Vertex stack unless a flow-level or run-level stack override is present.

## Delete it

```bash
kitaru stack delete prod-vertex
```

Use `--recursive` if you want Kitaru to remove Kitaru-managed component records too. Kitaru does not delete your cloud bucket, registry repository, or IAM resources.

## Related

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Stacks</strong></td><td>The shared stack model, precedence rules, YAML, --extra, and --async</td><td><a href="README.md">README.md</a></td></tr><tr><td><strong>Containerization</strong></td><td>How Kitaru builds and configures remote execution images</td><td><a href="../guides/containerization.md">../guides/containerization.md</a></td></tr></tbody></table>
