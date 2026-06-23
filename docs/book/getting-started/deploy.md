---
description: Move from local development to running agents in production
icon: server
---

# Deploy Your Agent

Production agents need to survive restarts, share one source of truth across
your team, and record every run so you can replay and improve it later.
Deploying moves your flow off your laptop onto shared cloud compute without
changing your code. Three steps.

## 1. Deploy a Kitaru server

Locally, the server runs embedded in your Python process. In production, you
deploy it as a standalone service so your team shares a single view of all
executions and agents run independently of your machine.

The server stores execution metadata, checkpoint state, and logs. It does not
access your cloud storage directly; it brokers temporary credentials so clients
and the UI can read artifacts when needed.

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Deploy with Helm</strong></td><td>Install the Kitaru server on any Kubernetes cluster</td><td><a href="../deploy/helm.md">../deploy/helm.md</a></td></tr></tbody></table>

## 2. Connect to the server

Point your local client at the deployed server:

```bash
kitaru login https://kitaru.your-company.com
```

From here, the CLI, `KitaruClient`, and the UI all talk to the same
server. Any executions you start will be visible to your whole team.

## 3. Set up a cloud stack

A [stack](../stacks/README.md) is a named runtime that tells Kitaru where
to run your agent code and where to store its outputs. Pick the compute
backend that matches your cloud:

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Kubernetes</strong></td><td>Run agents on any Kubernetes cluster with S3 or GCS storage</td><td><a href="../stacks/kubernetes-stacks.md">../stacks/kubernetes-stacks.md</a></td></tr><tr><td><strong>AWS (SageMaker)</strong></td><td>Run agents as SageMaker jobs with S3 storage</td><td><a href="../stacks/sagemaker-stacks.md">../stacks/sagemaker-stacks.md</a></td></tr><tr><td><strong>GCP (Vertex AI)</strong></td><td>Run agents as Vertex AI jobs with GCS storage</td><td><a href="../stacks/vertex-stacks.md">../stacks/vertex-stacks.md</a></td></tr><tr><td><strong>Azure (AzureML)</strong></td><td>Run agents as AzureML jobs with Azure Blob storage</td><td><a href="../stacks/azureml-stacks.md">../stacks/azureml-stacks.md</a></td></tr></tbody></table>

Once your stack is created, switch to it:

```bash
kitaru stack use prod-k8s
```

## 4. Run your agent in the cloud

Your code doesn't change. The same flow, the same checkpoints, the same
replay, now running on cloud compute with durable storage.

```python
if __name__ == "__main__":
    research_agent.run(topic="durable execution for AI agents")
```

When you call `.run()`, the client fetches short-lived credentials from the
server and dispatches the execution directly to your stack's compute backend.
Checkpoint outputs are written to cloud storage. You can observe the execution
from the UI, the CLI, or any `KitaruClient` connected to the same server.

Every cloud run records the same durable checkpoints as your local runs, so you
can `flow.replay(exec_id, from_="<checkpoint>", **overrides)` a production
execution with one input changed and diff it against the original baseline.
