---
description: Observability tools watch your agent. Kitaru turns your production traces into a faithful environment you can re-run — so when you change your agent, you catch what breaks before your users do.
icon: hand-wave
---

# Welcome to Kitaru

Kitaru is the replay-and-regression layer for agents. It's the runtime your
agent runs on, underneath your harness, and it durably persists your agent's
state at every intermediate point — every model call, every tool call, every
decision. That turns your production history into something you can go back
to: rebuild a real run as a faithful environment, re-execute it with one thing
changed — a different model, a different prompt — and diff the result against
the original. Because the baseline reproduces, the difference you see is your
change, not noise — you catch what breaks before your users do, and change
your agent with confidence.

Every change to your agent is a hypothesis. Replay is the controlled experiment
that tests it — against production runs that actually happened. The environment
holds everything else constant, so the diff against the real baseline is
attributable evidence, not a hunch. And because completed checkpoints are
cached, each experiment costs only the part you changed — you can test several
hypotheses against the same recorded run, or one hypothesis against a whole
cohort of runs.

The harness you already picked (PydanticAI, OpenAI Agents SDK, LangGraph, Claude
Agent SDK, raw Python) keeps owning how the agent thinks. Kitaru owns the run
record and the replay loop. A Kitaru flow is a dynamic ZenML pipeline, so agents
run on the same [stacks](stacks/README.md), server, and dashboard as your ZenML
pipelines.

## Run, replay, improve

* **Run (durable).** Every `@checkpoint` is a durable unit of work; its output is
  persisted automatically, and every model and tool call is recorded. If a flow
  fails partway, replaying it reuses recorded results instead of re-running
  expensive work.
* **Replay (the differentiator).** Re-execute a recorded run from any checkpoint.
  A plain rerun with no change reproduces the original — that is your baseline.
  Replay again with one input overridden and diff the two. This re-executes the
  real run from a checkpoint; it is not re-scoring saved outputs like an eval.
* **Improve.** Apply the same change across a cohort of recent runs, measure
  cost, latency, and quality, and keep the winner.

Kitaru is self-host-first: a single-service server on your own Kubernetes,
artifacts in your own S3/GCS/Azure Blob. No mandatory SaaS control plane in the
path of your agent's data. See
[Harness, Runtime, Platform](concepts/harness-runtime-platform.md) for where
Kitaru fits.

## The replay loop

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
    # Run, then replay from a checkpoint with one input changed.
    run = research_agent.run(topic="Why do agents need durable execution?").wait()

    baseline = research_agent.replay(run.exec_id, at="draft_report")
    variant = research_agent.replay(
        run.exec_id,
        at="draft_report",
        flow_overrides={"model": "anthropic/claude-opus-4"},
    )
    # baseline reproduces the original; diff variant against it to isolate your change.
```

`run(...)` returns a handle; `.wait()` blocks for the result and exposes
`.exec_id`. `replay(exec_id, at="<checkpoint>", flow_overrides={...})`
re-executes from that checkpoint, overriding flow inputs such as the model or
prompt profile. The same loop is available over the [CLI](https://sdkdocs.kitaru.ai) and the
[MCP server](agent-native/mcp-server.md) so a coding agent can drive it.

See the [Quickstart](getting-started/quickstart.md) to install and run this
yourself.

## Where ZenML fits

Kitaru is built by the team behind [ZenML](https://docs.zenml.io), the
open-source framework for production ML and LLM pipelines, and runs on the same
foundations. Each project works on its own — you can use Kitaru without ever
touching ZenML. If you use both, they compose rather than coexist: a Kitaru flow
is a dynamic ZenML pipeline under the hood, so your agents and pipelines run on
the same [stacks](stacks/README.md), persist artifacts to the same stores, and
show up in the same server and dashboard. If your work is ML pipelines rather
than agents, start with the [ZenML docs](https://docs.zenml.io) — and if you
want the narrative tutorial for agents, the
[Agents guide](https://docs.zenml.io/user-guides/agents-guide) sits alongside
ZenML's Starter, Production, and LLMOps guides in the shared
[Learn](https://docs.zenml.io/user-guides) section.

## Runtime primitives

These are the primitives Kitaru adds on top of your existing Python agent code.
You keep your harness and your control flow; Kitaru records the run and makes it
replayable.

* **Replay and override:** Re-execute any run from any checkpoint — to recover
  from a failure, or with [overrides](guides/replay-and-overrides.md) (a
  different model or parameter) to isolate the effect of a change before you ship
  it. Use invocation overrides when you need to change one recorded checkpoint,
  tool, or model call instead of every call with the same checkpoint name.
* **Durable execution:** Wrap steps in [`@checkpoint`](concepts/checkpoints.md)
  and your agent picks up where it left off without re-running expensive work
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
