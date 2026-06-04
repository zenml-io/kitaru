---
description: Server, auth, stacks, deployments, containers, secrets, and production runtime concerns.
icon: sliders
---

# Operate Kitaru

Use this section when Kitaru is becoming shared infrastructure for a team.

## Operating model

| Concern | Owns | Start here |
|---|---|---|
| Shared metadata, auth, UI, deployment registry | Kitaru server | [Deploy a Kitaru server](../deploy/helm.md) |
| Where executions run | Stack | [Stacks](../stacks/README.md) |
| How code gets packaged | Flow `image=` config | [Containerization](../guides/containerization.md) |
| Who can invoke and manage runs | Server auth + project context | [Authentication](../guides/authentication.md) |
| How consumers call stable versions | Deployment tags and versions | [Deployments](../guides/deployments.md) |
| How provider credentials reach code | Secrets and model aliases | [Secrets + Model Registration](../guides/secrets-and-model-registration.md) |

## Production path

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>1. Deploy the server</strong></td><td>Run Kitaru with Docker or Helm, backed by persistent storage.</td><td><a href="../deploy/helm.md">../deploy/helm.md</a></td></tr><tr><td><strong>2. Authenticate automation</strong></td><td>Create service-account API keys and configure headless environments.</td><td><a href="../guides/authentication.md">../guides/authentication.md</a></td></tr><tr><td><strong>3. Create a remote stack</strong></td><td>Choose Kubernetes, Vertex AI, SageMaker, or AzureML.</td><td><a href="../stacks/README.md">../stacks/README.md</a></td></tr><tr><td><strong>4. Package the flow</strong></td><td>Set base image, requirements, apt packages, and secret-backed environment.</td><td><a href="../guides/containerization.md">../guides/containerization.md</a></td></tr><tr><td><strong>5. Deploy and invoke</strong></td><td>Publish a versioned flow route and invoke by tag.</td><td><a href="../guides/deployments.md">../guides/deployments.md</a></td></tr></tbody></table>

## Setup pages

| Area | Pages |
|---|---|
| Server | [Docker](../deploy/docker.md), [Helm](../deploy/helm.md) |
| Stacks | [Kubernetes](../stacks/kubernetes-stacks.md), [Vertex](../stacks/vertex-stacks.md), [SageMaker](../stacks/sagemaker-stacks.md), [AzureML](../stacks/azureml-stacks.md) |
| Runtime config | [Configuration](../guides/configuration.md), [Log Store](../stacks/log-store.md) |
| Secrets | [Manage Secrets](../guides/secrets.md), [Secrets + Model Registration](../guides/secrets-and-model-registration.md) |
| Run operations | [Execution Management](../guides/execution-management.md), [Execution Logs](../guides/execution-logs.md) |

## Guardrails

* Prefer `KITARU_*` environment variables in user-facing setup.
* Keep credentials in secrets, not image environment.
* Use service-account API keys for automation.
* Treat deploy-time image config as part of the saved deployment snapshot.
* Use tags for movable routes and versions for reproducibility.
