# Table of contents

## Getting Started

- [Welcome to Kitaru](README.md)
- [Installation](getting-started/installation.md)
- [Deploy Kitaru](deploy/README.md)
  - [Docker](deploy/docker.md)
  - [Helm](deploy/helm.md)
- [Quickstart](getting-started/quickstart.md)
- [Agent skills](agent-native/skills.md)

## Core Concepts

- [Overview](concepts/README.md)
- [Agents & Sessions](concepts/agents-and-sessions.md)
- [Replay](concepts/replay.md)
- [Evaluators & Evaluations](concepts/evaluators.md)
- [Cohorts](concepts/cohorts.md)
- [Experiments](concepts/experiments.md)
- [Investigations & Annotations](concepts/investigations.md)
- [Workers](concepts/workers.md)
- [Under the Hood](concepts/under-the-hood.md)

## Guides

- [Complete returns-agent tutorial](tutorials/returns-agent/README.md)
  - [1. Observe the recorded behavior](tutorials/returns-agent/observe.md)
  - [2. Judge the selected behavior](tutorials/returns-agent/judge.md)
  - [3. Define one behavior to test](tutorials/returns-agent/define.md)
  - [4. Replay one bounded change](tutorials/returns-agent/replay.md)
  - [5. Compare the paired evidence](tutorials/returns-agent/compare.md)
- [Replay a failure and fork it](guides/replay-and-overrides.md)
- [Build a regression suite from production](guides/regression-suite.md)
- [Write an evaluator](guides/write-an-evaluator.md)
- [Import your traces](getting-started/import-your-traces.md)
- [Import Langfuse traces](guides/import-langfuse-traces.md)
- [Import any trace format](guides/importing-sessions.md)
- [Deterministic evaluations](guides/deterministic-evaluations.md)
- [Tool policies](guides/tool-policies.md)
- [Track cost and model usage](guides/llm-calls.md)
- [Drive it from your coding agent](agent-native/mcp-server.md)

## Adapters

- [Overview](adapters/README.md)
- [Pydantic AI](adapters/pydantic-ai.md)
- [LangGraph](adapters/langgraph.md)
- [OpenAI Agents SDK](adapters/openai-agents.md)
- [Mastra](adapters/mastra.md)
- [Vercel AI SDK](adapters/vercel-ai.md)
- [TypeScript SDK](adapters/typescript-sdk.md)
- [No adapter for your framework](adapters/custom.md)

## Running in production

- [Workers in production](deploy/workers.md)
- [Authentication & API keys](deploy/authentication.md)
- [Secrets](deploy/secrets.md)
- [Configuration](deploy/configuration.md)
- [Troubleshooting](getting-started/troubleshooting.md)

## Project

- [Contributing](contributing.md)
