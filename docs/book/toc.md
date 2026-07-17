# Table of contents

## Getting Started

* [Welcome to Kitaru](README.md)
* [Installation](getting-started/installation.md)
* [Quickstart](getting-started/quickstart.md)
* [Examples](getting-started/examples.md)
* [Troubleshooting](getting-started/troubleshooting.md)

## Core Concepts

* [Overview](concepts/README.md)
* [Executions — the recording](concepts/executions.md)
* [Flows](concepts/flows.md)
* [Checkpoints](concepts/checkpoints.md)
* [Deployments](concepts/deployments.md)
* [Wait, Input & Resume](concepts/wait-and-input.md)
* [Logging & Metadata](concepts/logging.md)
* [Under the Hood](concepts/under-the-hood.md)

## Guides

* [Debug and test on real runs](guides/replay-and-overrides.md)
* [Build a regression suite from production](guides/regression-suite.md)
* [Pause for a human approval](guides/wait-and-resume.md)
* [Track cost and model usage](guides/llm-calls.md)
* [Drive it from your coding agent](agent-native/mcp-server.md)

## Adapters

* [Overview](adapters/README.md)
* [Choose an Adapter](guides/choose-an-adapter.md)
* [Pydantic AI](adapters/pydantic-ai.md)
* [OpenAI Agents](adapters/openai-agents.md)
* [Claude Agent SDK](adapters/claude-agent-sdk.md)
* [Gemini Interactions](adapters/gemini-interactions.md)
* [Google ADK](adapters/google-adk.md)
* [LangGraph](adapters/langgraph.md)

## Running in Production

* [Deploy & Invoke](guides/deployments.md)
  * [Containerization](guides/containerization.md)
* [Run the Server](deploy/README.md)
  * [Docker](deploy/docker.md)
  * [Helm](deploy/helm.md)
* [Stacks](stacks/README.md)
  * [Kubernetes Stacks](stacks/kubernetes-stacks.md)
  * [Modal Stacks](stacks/modal-stacks.md)
  * [Vertex Stacks](stacks/vertex-stacks.md)
  * [SageMaker Stacks](stacks/sagemaker-stacks.md)
  * [AzureML Stacks](stacks/azureml-stacks.md)
  * [Log Store](stacks/log-store.md)
* [Inspect & Manage Executions](guides/execution-management.md)
  * [Persistent Artifacts](guides/artifacts.md)
  * [Execution Logs](guides/execution-logs.md)
  * [Live Events](guides/checkpoint-streaming.md)
  * [Error Handling](guides/error-handling.md)
* [Configuration](guides/configuration.md)
  * [Authentication](guides/authentication.md)
  * [Secrets](guides/secrets.md)
  * [Projects](guides/projects.md)

## Project

* [Contributing](contributing.md)
