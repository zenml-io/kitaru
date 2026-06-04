---
description: The runtime layer underneath your agent stack.
icon: hand-wave
---

# Welcome to Kitaru

Kitaru is the runtime layer underneath your agent stack. It gives you durable
execution for Python agents — checkpoints, replay, resume, `wait()`, versioned
deployments — while the harness you already picked (Pydantic AI, Deep Agents,
LangGraph, Claude Agent SDK, raw Python) keeps owning how the agent thinks, and
your existing platform keeps owning auth, observability, and policy.

Kitaru is self-host-first: a single-service server on your own Kubernetes,
artifacts in your own S3/GCS/Azure Blob. No mandatory SaaS control plane in the
path of your agent's data. See
[Harness, Runtime, Platform](concepts/harness-runtime-platform.md) for the full
picture of where Kitaru fits.

## Create a durable agent

```python
import kitaru
from kitaru import checkpoint, flow

@checkpoint
def research(topic: str) -> str:
    return kitaru.llm(f"Summarize {topic} in two sentences.")

@checkpoint
def draft_report(summary: str) -> str:
    return kitaru.llm(f"Write a short report based on: {summary}")

@flow
def research_agent(topic: str) -> str:
    summary = research(topic)
    return draft_report(summary)

if __name__ == "__main__":
    research_agent.run(topic="Why do AI agents need durable execution?")
```

Each `@checkpoint` is a durable unit of work — its output is persisted
automatically. If the flow fails at `draft_report`, replaying it skips `research`
and reuses its recorded result. `kitaru.llm()` logs model calls with prompt,
response, tokens, and latency per call.

See the [Quickstart](getting-started/quickstart.md) to install and run this
yourself.

## What your agent can do with Kitaru

These are the runtime primitives Kitaru adds on top of your existing Python agent
code. You keep your harness and your control flow; Kitaru makes the run durable.

* **Durable execution:** Wrap steps in [`@checkpoint`](concepts/checkpoints.md)
  and your agent picks up where it left off without re-running expensive work
* **Replay from failure:** Re-run only the failed part of a flow by replaying
  from a checkpoint instead of starting from scratch
* **Wait and resume:** Add [`kitaru.wait()`](guides/wait-and-resume.md) and let
  agents pause for a human, another system, or later input; after the polling
  timeout, compute is released and the run resumes when input lands
* **Artifact lineage:** Every checkpoint output is written to your object store
  as a typed, versioned artifact — step through runs, diff outputs across runs,
  and trace a bad final output back to the exact step that produced it
* **Execution management:** [`KitaruClient`](guides/execution-management.md) lets
  you inspect, replay, retry, resume, and cancel executions from code or CLI
* **Tracked LLM calls:** Use [`kitaru.llm()`](guides/llm-calls.md) and every call
  gets automatic secret resolution, prompt/response capture, and token/latency
  logging
* **Persistent data:** [`kitaru.save()` / `kitaru.load()`](guides/artifacts.md)
  let agents store and retrieve files, objects, and results across executions
* **Structured observability:** [`kitaru.log()`](concepts/logging.md) attaches
  key-value metadata to any checkpoint or flow for debugging and the UI
* **Runtime configuration:** [`kitaru.configure()`](guides/configuration.md) sets
  your model, log store, and stack defaults in one call
* **Framework and infrastructure portability:** Keep your Python control flow,
  use your preferred framework, and run locally or on remote stacks — Kubernetes,
  Vertex AI, SageMaker, AzureML

## Next steps

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Installation</strong></td><td>Install Kitaru with uv or pip.</td><td><a href="getting-started/installation.md">getting-started/installation.md</a></td></tr><tr><td><strong>Quickstart</strong></td><td>Run a tiny flow end to end.</td><td><a href="getting-started/quickstart.md">getting-started/quickstart.md</a></td></tr><tr><td><strong>Examples</strong></td><td>Browse runnable workflows grouped by goal.</td><td><a href="getting-started/examples.md">getting-started/examples.md</a></td></tr><tr><td><strong>Harness, Runtime, Platform</strong></td><td>Where Kitaru fits in an agent stack, and where it doesn't.</td><td><a href="concepts/harness-runtime-platform.md">concepts/harness-runtime-platform.md</a></td></tr><tr><td><strong>How It Works</strong></td><td>Server, runner, execution targets, and what lives where in local dev vs production.</td><td><a href="concepts/how-it-works.md">concepts/how-it-works.md</a></td></tr><tr><td><strong>Core Concepts</strong></td><td>Flows, checkpoints, and the execution model.</td><td><a href="concepts/README.md">concepts/README.md</a></td></tr><tr><td><strong>Execution Management</strong></td><td>Inspect runs, replay, retry, resume, and fetch logs.</td><td><a href="guides/execution-management.md">guides/execution-management.md</a></td></tr><tr><td><strong>Wait, Input, and Resume</strong></td><td>Pause flows for external input and continue the same execution.</td><td><a href="guides/wait-and-resume.md">guides/wait-and-resume.md</a></td></tr><tr><td><strong>Tracked LLM Calls</strong></td><td>Use kitaru.llm() with aliases, secrets, and captured artifacts.</td><td><a href="guides/llm-calls.md">guides/llm-calls.md</a></td></tr><tr><td><strong>Secrets + Model Registration</strong></td><td>Store provider credentials, register a model alias, and use kitaru.llm().</td><td><a href="guides/secrets-and-model-registration.md">guides/secrets-and-model-registration.md</a></td></tr><tr><td><strong>Configuration</strong></td><td>Set runtime defaults and understand override precedence.</td><td><a href="guides/configuration.md">guides/configuration.md</a></td></tr><tr><td><strong>Stacks</strong></td><td>Create, inspect, switch, and clean up local and remote stacks across Kubernetes, AWS, GCP, and Azure.</td><td><a href="stacks/README.md">stacks/README.md</a></td></tr><tr><td><strong>MCP Server</strong></td><td>Query and manage executions via MCP tools.</td><td><a href="agent-native/mcp-server.md">agent-native/mcp-server.md</a></td></tr><tr><td><strong>Agent Skills</strong></td><td>Install quickstart, scoping, authoring, and adapter migration skills.</td><td><a href="agent-native/claude-code-skill.md">agent-native/claude-code-skill.md</a></td></tr><tr><td><strong>CLI Reference</strong></td><td>Browse the generated command reference.</td><td><a href="https://sdkdocs.kitaru.ai">cli/README.md</a></td></tr><tr><td><strong>Blog</strong></td><td>Read essays on durable execution, long-running agents, and Kitaru's design.</td><td><a href="https://kitaru.ai/blog/">https://kitaru.ai/blog/</a></td></tr></tbody></table>
