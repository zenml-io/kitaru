---
description: The runtime for production AI agents. One artifact — the execution — that you run, not just read.
icon: hand-wave
---

# Welcome to Kitaru

Most traces are transcripts: you read them. Kitaru records a run as an
**execution** — the one artifact it produces — and an execution re-executes. Your
real code runs again, with the recording answering for everything the original
run saw. Reproduce it exactly, fork it with one thing changed, trust the diff.

A recorded or imported trace lands as an execution. From there the verb is
**replay**: rerun it unchanged for a faithful baseline, then rerun it with a
different model, prompt, or tool result — and the difference you see is your
change, not replay noise. Below the fold there is one noun to learn, the
execution, and one thing you do to it.

Kitaru wraps the agent framework you already use (PydanticAI, OpenAI Agents SDK,
LangGraph, Claude Agent SDK, Gemini, raw Python), records every model and tool
call as a checkpoint, and self-hosts on your own infrastructure. For a natively
recorded agent, Kitaru is the runtime the agent runs on; the replay-and-eval loop
that turns production traffic into tests runs off to the side of the hot path.

## Replay, improve, run

* **Replay (the verb).** Re-execute a recorded run from any checkpoint. A plain
  rerun with no change reproduces the original — that is your baseline. Replay
  again with one input overridden and diff the two. This re-executes the real run
  from a checkpoint; it is not re-scoring saved outputs like an eval.
* **Improve.** Apply the same change across a cohort of recent runs, measure
  cost, latency, and quality, and keep the winner. The cohort is a regression
  test you never had to write.
* **Run (durable).** The beat that mints the recording: every model and tool
  call lands as a durable checkpoint, persisted automatically. Recording a run
  means surviving one — if a flow fails partway, the retry reuses recorded
  results instead of re-running expensive work.

Kitaru is self-host-first: a single-service server on your own Kubernetes,
artifacts in your own S3/GCS/Azure Blob. No mandatory SaaS control plane in the
path of your agent's data. See [Under the Hood](concepts/under-the-hood.md) for
how a run is recorded and where Kitaru sits in an agent stack.

## The replay loop

No decorators, no graph, no rewrite. Wrap the agent you already have — Kitaru
opens a flow around the call and records every model and tool call as a
checkpoint:

```python
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5-nano", name="support-agent",
              system_prompt="You resolve support tickets.")

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    return payments.refund(order_id)  # your real API

support = KitaruAgent(agent)
support.run_sync("Refund order #4821 — the card reader was double-charged.")
```

Every run is now an execution you can replay:

```python
from kitaru import KitaruClient

client = KitaruClient()
trace = client.executions.latest()

# Reproduce: your real code runs again against the recorded world.
# Unchanged, it matches the original exactly. That's your baseline.
baseline = client.executions.replay(trace.exec_id, at="support-agent_model_request")

# Fork: same execution, one thing changed — patch the recorded tool output.
# What would the agent have done if the refund had succeeded?
fork = client.executions.replay(
    trace.exec_id,
    at="refund_payment_tool",
    checkpoint_overrides={
        "refund_payment_tool": {"output": "refund issued: $129.00"},
    },
)
```

The same loop is available over the [CLI](https://sdkdocs.kitaru.ai) and the
[MCP server](agent-native/mcp-server.md) so a coding agent can drive it. When
you want named replay boundaries or multi-turn workflows without an adapter,
the explicit [`@flow` and `@checkpoint`](concepts/checkpoints.md) decorators
give raw Python the same recording.

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

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Installation</strong></td><td>Install Kitaru with uv or pip.</td><td><a href="getting-started/installation.md">getting-started/installation.md</a></td></tr><tr><td><strong>Quickstart</strong></td><td>Run a tiny flow end to end.</td><td><a href="getting-started/quickstart.md">getting-started/quickstart.md</a></td></tr><tr><td><strong>Examples</strong></td><td>Browse runnable workflows grouped by goal.</td><td><a href="getting-started/examples.md">getting-started/examples.md</a></td></tr><tr><td><strong>Executions — the recording</strong></td><td>What a finished run leaves behind, and why imported traces are read-only.</td><td><a href="concepts/executions.md">concepts/executions.md</a></td></tr><tr><td><strong>Under the Hood</strong></td><td>Server, runner, execution targets, and where Kitaru sits in an agent stack.</td><td><a href="concepts/under-the-hood.md">concepts/under-the-hood.md</a></td></tr><tr><td><strong>Core Concepts</strong></td><td>Flows, checkpoints, and the execution model.</td><td><a href="concepts/README.md">concepts/README.md</a></td></tr><tr><td><strong>Execution Management</strong></td><td>Inspect runs, replay, retry, resume, and fetch logs.</td><td><a href="guides/execution-management.md">guides/execution-management.md</a></td></tr><tr><td><strong>Wait, Input, and Resume</strong></td><td>Pause flows for external input and continue the same execution.</td><td><a href="guides/wait-and-resume.md">guides/wait-and-resume.md</a></td></tr><tr><td><strong>Tracked LLM Calls</strong></td><td>Use kitaru.llm() with aliases, secrets, and captured artifacts.</td><td><a href="guides/llm-calls.md">guides/llm-calls.md</a></td></tr><tr><td><strong>Secrets</strong></td><td>Store provider credentials, register a model alias, and use kitaru.llm().</td><td><a href="guides/secrets.md">guides/secrets.md</a></td></tr><tr><td><strong>Configuration</strong></td><td>Set runtime defaults and understand override precedence.</td><td><a href="guides/configuration.md">guides/configuration.md</a></td></tr><tr><td><strong>Stacks</strong></td><td>Create, inspect, switch, and clean up local and remote stacks across Kubernetes, AWS, GCP, and Azure.</td><td><a href="stacks/README.md">stacks/README.md</a></td></tr><tr><td><strong>Drive it from your coding agent</strong></td><td>Query, replay, and diff executions from Claude Code, Codex, or Cursor via MCP and skills.</td><td><a href="agent-native/mcp-server.md">agent-native/mcp-server.md</a></td></tr><tr><td><strong>CLI Reference</strong></td><td>Browse the generated command reference.</td><td><a href="https://sdkdocs.kitaru.ai">cli/README.md</a></td></tr><tr><td><strong>Blog</strong></td><td>Read essays on durable execution, long-running agents, and Kitaru's design.</td><td><a href="https://kitaru.ai/blog/">https://kitaru.ai/blog/</a></td></tr></tbody></table>
