---
description: Wrap any PydanticAI agent once with KitaruAgent so every run is recorded as durable checkpoints you can replay
icon: cube
---

# PydanticAI Adapter

The PydanticAI adapter makes any [PydanticAI](https://ai.pydantic.dev) agent a Kitaru flow without changing its code. Wrap the agent once with `KitaruAgent` and every model request, tool call, MCP invocation, and human-in-the-loop wait becomes a durable checkpoint — so you can reproduce a run faithfully, [replay it with one input changed](../guides/replay-and-overrides.md) (a different model or prompt), and diff the two.

```python
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5-nano", name="researcher")
durable_agent = KitaruAgent(agent)

result = durable_agent.run_sync("Summarize quantum error correction.")
print(result.output)
```

No flow decorator, no checkpoint annotations. When called outside a flow, `KitaruAgent` auto-opens one for you. By default, `checkpoint_strategy="calls"` persists model, tool, and MCP calls as separate checkpoints — the boundaries you replay from later. Agents run on the same stacks, server, and dashboard as your ZenML pipelines; the dashboard shows the run, tool calls, model responses, and wait points.

## Install

Add the `pydantic-ai` extra — and `local` if you want the local dashboard:

```bash
uv add "kitaru[pydantic-ai,local]"
```

Initialize the project once:

```bash
kitaru init
kitaru login        # local server; add a URL to connect to a deployed one
kitaru status
```

{% hint style="info" %}
Migrating an existing PydanticAI project? The
[`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills) package
includes `/kitaru:kitaru-pydantic-ai-migration` for moving to `KitaruAgent` with the
right checkpoint strategy and human-in-the-loop guardrails. See
[Agent Skills](../agent-native/claude-code-skill.md).
{% endhint %}

## Usage patterns

### Zero-config

Wrap the agent and call it directly. The adapter auto-opens a flow and per-call checkpoints with the default `checkpoint_strategy="calls"` when you're outside of one. If you already have an explicit `@kitaru.flow`, call the agent directly from the flow body — not from inside another `@kitaru.checkpoint` — when you want model/tool calls to become separate checkpoint rows.

```python
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5-nano", name="researcher")
durable_agent = KitaruAgent(agent)

result = durable_agent.run_sync("What are the open questions in QEC?")
```

Best for prototyping, porting an existing agent, or single-turn interactions.

{% hint style="warning" %}
Auto-flow is local-only. On remote stacks (Kubernetes, Vertex, SageMaker,
AzureML) the in-process registry the adapter uses to stitch the auto-flow
isn't visible — wrap the call in an explicit `@kitaru.flow` there.
{% endhint %}

### Explicit boundaries

For multi-turn workflows, named replay boundaries, or coordinated waits across turns, use `@kitaru.flow` and `@kitaru.checkpoint` yourself. Inside a checkpoint, `KitaruAgent` is a straight passthrough: the whole agent call belongs to the enclosing checkpoint, and internal PydanticAI model/tool activity is recorded as events and artifacts under that checkpoint instead of opening nested `*_tool` checkpoints.

### Checkpoint an agent turn

This example shows explicit flow/checkpoint boundaries. It creates one checkpoint per `ask(...)` call. Human approval waits are covered in the next sections.

```python
import kitaru
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5-nano", name="researcher")
durable_agent = KitaruAgent(agent)

@kitaru.checkpoint
def ask(prompt: str) -> str:
    return durable_agent.run_sync(prompt).output

@kitaru.flow
def research(topic: str) -> str:
    overview = ask(f"Overview of {topic}")
    return ask(f"Open questions, given this overview:\n{overview}")

handle = research.run("quantum error correction")
print(handle.wait())
```

Replay the flow from a checkpoint with `flow.replay(exec_id, from_="ask", **overrides)` to re-execute the real run with one input changed — a different model or prompt — while completed checkpoints before that boundary are reused. See [Replay and overrides](../guides/replay-and-overrides.md).

{% hint style="warning" %}
This pattern checkpoints the whole agent turn. If the agent calls a tool, Kitaru stores that tool activity under the `ask` checkpoint as adapter events/artifacts; it does not create nested checkpoints such as `search_tool` or `lookup_price_tool`. To see ordinary PydanticAI model/tool calls as separate checkpoint rows, call the agent directly from flow scope:

```python
@kitaru.flow
def research(topic: str) -> str:
    result = durable_agent.run_sync(f"Open questions in {topic}")
    return result.output
```
{% endhint %}

### Ask the human from a tool body

`kp.wait_for_input(...)` is a thin adapter-namespaced wrapper around `kitaru.wait(...)`. The LLM can pick the question, the tool can return the human's typed answer, and the agent can continue with that value as the tool result — but the wait still has to be created at flow scope.

Two separate safety rules matter with the default `checkpoint_strategy="calls"`:

1. A regular tool body usually runs inside an adapter-created `*_tool` checkpoint, and `kitaru.wait()` is intentionally rejected from checkpoint scope.
2. Pydantic AI normally moves sync tool functions to a worker thread, while Kitaru waits must be created on the workflow thread.

If a regular sync tool body needs to call `kp.wait_for_input(...)`, configure two separate things: opt that tool out of per-call checkpointing, and explicitly opt into Pydantic AI sync-tool thread compatibility for the run:

```python
from typing import Literal
from pydantic import BaseModel
from kitaru.adapters import pydantic_ai as kp

class BugReport(BaseModel):
    title: str
    description: str
    severity: Literal["low", "medium", "high"]

def ask_user(question: str) -> str:
    """Ask the human a free-form clarifying question."""
    return kp.wait_for_input(schema=str, question=question)

def collect_bug_report() -> BugReport:
    """Collect a structured bug report."""
    return kp.wait_for_input(
        schema=BugReport,
        question="Describe the bug: title, description, severity.",
    )
```

Then construct the durable agent with a per-tool checkpoint opt-out and the explicit sync-tool wait compatibility flag:

```python
durable_agent = KitaruAgent(
    agent,
    tool_checkpoint_config_by_name={"ask_user": False, "collect_bug_report": False},
    allow_sync_tool_body_waits=True,
)
```

Both `question` and `schema` are ordinary arguments, so the tool body can compute them, branch on agent state, prepend context, or call multiple waits. The adapter attaches identifying metadata (`adapter=pydantic_ai`, `source=tool_body`) so these waits are distinguishable from hand-written `kitaru.wait()` calls in flow code.

If you do not opt the tool out, the adapter fails early with an actionable `KitaruUsageError` rather than creating a checkpoint-contained wait that would be hard to resume safely. The opt-out is checkpoint-only: it keeps the wait out of the synthetic `*_tool` checkpoint. The `allow_sync_tool_body_waits=True` flag separately asks Pydantic AI to keep supported sync tools on the workflow thread while the agent run is active. That compatibility layer applies to sync tools for the whole agent run, so Kitaru only enables it when you ask for it explicitly. The trade-off is concrete: any supported sync tool in that run may execute inline instead of using Pydantic AI's normal worker-thread path, so avoid mixing this opt-in with slow/blocking sync tools if you rely on normal tool parallelism. Another safe option is to move the human gate out of the tool body and call `kitaru.wait()` directly before or after the agent turn in your `@kitaru.flow` code.

Running locally, Kitaru prompts in the terminal. Running against a deployed server, the execution pauses and can be resumed from anywhere:

```bash
kitaru executions input <exec_id> --value '{"title": "login broken", "description": "500 on /auth", "severity": "high"}'
kitaru executions resume <exec_id>
```

### Declarative sugar: `@hitl_tool`

When a tool is *purely* a wait — nothing computed in the body, no branching — prefer the `@hitl_tool` decorator. The body is skipped entirely, and the adapter creates the wait from its own flow-scope code instead of from the user sync tool body.

```python
from kitaru.adapters.pydantic_ai import hitl_tool

@hitl_tool(question="Approve publish?", schema=bool)
def approve(summary: str) -> bool: ...

@hitl_tool(schema=str)
def ask_user(question: str) -> str: ...   # LLM-supplied question via `question_arg`

@hitl_tool(
    name="collect_bug_report",
    question="Describe the bug: title, description, severity.",
    schema=BugReport,
)
def collect_bug_report() -> BugReport: ...
```

`@hitl_tool(schema=..., question_arg=...)` picks up the LLM-supplied argument at runtime (defaults to looking for `question`). Pass `question_arg=None` to force the static prompt.


### Replay semantics

Waits belong at flow scope. `kitaru.wait()` is rejected inside `@checkpoint` bodies because the flow can pause while the enclosing checkpoint step is recorded as failed or incomplete. The default `checkpoint_strategy="calls"` splits each top-level model, tool, and MCP call into its own checkpoint, which improves visibility and retry isolation, but it also means regular tool-body waits need an explicit per-tool opt-out as shown above.

## Runtime behavior and guardrails
### Human-in-the-loop tools

The adapter bridges every PydanticAI deferred pattern into `kitaru.wait()`. A paused flow is visible from `kitaru executions list`, the dashboard, and the REST API; once input is supplied the flow resumes from the exact same point.

```python
from kitaru.adapters.pydantic_ai import hitl_tool

@hitl_tool(question="Approve publishing this brief?", schema=bool)
def publish_brief(headline: str, sources: list[str]) -> str:
    return f"published: {headline} ({len(sources)} sources)"
```

Other PydanticAI deferred patterns also route through `kitaru.wait()` **when they run at flow scope**:

- `@agent.tool(requires_approval=True)` — PydanticAI's native approval flag
- raising `pydantic_ai.exceptions.ApprovalRequired` or `CallDeferred` from a tool body
- calling `kp.wait_for_input(...)` from a tool body

With `checkpoint_strategy="calls"`, `@hitl_tool` stays flow-scope safe because the adapter deliberately skips the synthetic `*_tool` checkpoint for that call and creates the wait from adapter-managed code. Regular sync tools that call `wait_for_input()` need both `tool_checkpoint_config_by_name={"tool_name": False}` and `allow_sync_tool_body_waits=True`, or they should move the wait to explicit flow code. Regular tools that raise Pydantic AI approval/deferred exceptions also need the checkpoint opt-out unless they use `@hitl_tool`.

See [Wait, Input, and Resume](../guides/wait-and-resume.md) for how paused flows are resolved.

### Sandbox command toolset

Use `sandbox_command_toolset(...)` when a PydanticAI agent should run shell commands through the sandbox on your current Kitaru stack instead of on your laptop or server process.

```python
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import (
    SANDBOX_COMMAND_TOOL_NAME,
    KitaruAgent,
    sandbox_command_toolset,
)

agent = Agent(
    "openai:gpt-5-nano",
    name="sandboxed_agent",
    toolsets=[sandbox_command_toolset(max_chars=20_000)],
)
durable_agent = KitaruAgent(
    agent,
    tool_checkpoint_config_by_name={
        SANDBOX_COMMAND_TOOL_NAME: {"cache": False},
    },
)
```

The toolset exposes one model-facing tool named `run_sandbox_command(command, cwd=None)`. Your current Kitaru stack must have exactly one sandbox component. Each tool call creates one temporary sandbox session, runs one command, collects `stdout`, `stderr`, `exit_code`, truncation flags, and cleanup status, then closes or destroys that temporary session. By default, the adapter collects up to 20,000 characters from each output stream; pass `max_chars=` to change that limit. The factory also accepts `cleanup="destroy"` or `cleanup="close"`: the default `"destroy"` removes the temporary sandbox session after the command when the backend supports that, while `"close"` only closes the session handle.

A command that exits with a non-zero code is still a successful tool call from Kitaru's point of view: the model receives `exit_code`, `stdout`, and `stderr` and can decide what to do next. Missing sandbox components, multiple sandbox components, unsupported sandbox runtime APIs, and backend failures become failed tool calls and raise the same public Kitaru errors as `kitaru.run_sandbox_command(...)`.

The tool is a normal PydanticAI function tool under the hood, so Kitaru tracks and checkpoints it like any other function tool. Commands can create files, mutate databases, install packages, or otherwise change the sandbox, so the recommended default is to disable cache for this tool with `tool_checkpoint_config_by_name={SANDBOX_COMMAND_TOOL_NAME: {"cache": False}}`. That makes replay run the command again instead of accidentally reusing an old command result.

{% hint style="warning" %}
**The sandbox is not automatically safe just because it is called a sandbox.** The model chooses the shell command and optional working directory. Anything the sandbox process can read can be printed to `stdout` or `stderr`; that output is returned to the model and, with calls-strategy checkpoints, persisted in Kitaru execution/checkpoint artifacts. If the process can see files, environment variables, network access, or credentials, the model may cause a command to reveal them. The `local` sandbox is a development convenience, not a security boundary; it runs local subprocesses with access available to your user. For untrusted models or prompts, use an isolated sandbox provider and pass only minimal credentials.
{% endhint %}

The model can choose only the command and optional working directory. `env` is intentionally not exposed through this LLM-facing tool because tool arguments can be captured as Kitaru artifacts. If a command needs environment variables, write your own hand-authored PydanticAI tool around `kitaru.run_sandbox_command(...)` and pass only values you are comfortable handling in that code path.

### MCP servers

MCP servers attached to the agent are wrapped automatically. Their tool calls are tracked alongside native tools; with the default `checkpoint_strategy="calls"`, each top-level MCP call gets its own adapter checkpoint. `MCPServer.cache_tools=True` is honored to skip redundant `tools/list` round-trips on replay.

```python
from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio
from kitaru.adapters.pydantic_ai import KitaruAgent

server = MCPServerStdio(
    "npx",
    args=["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
    cache_tools=True,
)
agent = Agent("openai:gpt-5-nano", name="researcher", toolsets=[server])
durable_agent = KitaruAgent(agent)
```

## Checkpoint strategy

The adapter offers two strategies for how agent work maps onto Kitaru checkpoints. Pick per agent based on how you want to replay and retry.

| Strategy | How it maps | Replay unit | Best for |
|---|---|---|---|
| `"calls"` (default) | No turn checkpoint; each replay-safe model/tool/MCP call becomes its own checkpoint when the agent runs at flow scope outside any existing checkpoint | Per call | Expensive model calls, flaky tools, long tool-call chains where one failure shouldn't rewind everything |
| `"turn"` | One checkpoint per agent run; model/tool/MCP calls are child events and artifacts under that checkpoint | The full turn | Agents where one aggregated checkpoint and checkpoint artifacts like `event_log` / `run_summary` are more useful than per-call boundaries |

**Replay semantics in one sentence.** If a flow crashes on the 8th model call of a turn, `"turn"` re-runs the whole turn; `"calls"` gives the earlier calls their own completed checkpoint boundaries when Kitaru is allowed to create those boundaries. If you set `cache=True` on adapter-created checkpoint configs, repeated runs can reuse completed checkpoints when the logical inputs are the same; changed prompts, message history, model settings, tool arguments, tool call IDs, or behavior-changing run options produce different cache keys and should miss cache.

`checkpoint_strategy="calls"` cannot create nested checkpoints inside your own `@kitaru.checkpoint` body. If you wrap `durable_agent.run_sync(...)` in a checkpoint, the outer checkpoint wins and model/tool calls are recorded as adapter events/artifacts under that checkpoint.

Two concrete shapes matter:

```python
@kitaru.flow
def visible_tool_calls() -> str:
    result = durable_agent.run_sync("inspect the sandbox")
    return publish_final_answer(result.output)
```

Here the agent runs directly in the flow body, so `checkpoint_strategy="calls"` can create rows such as `researcher_model_request` and `run_sandbox_command_tool`. Because the flow explicitly returns `publish_final_answer(...)`, `FlowHandle.wait()` returns that persisted run output after the execution completes. If your flow does not return a value, inspect the persisted artifacts instead.

```python
@kitaru.checkpoint
def whole_turn(prompt: str) -> str:
    return durable_agent.run_sync(prompt).output
```

Here the whole agent call belongs to `whole_turn`. Tool activity is still tracked as adapter events/artifacts, but there is no separate `run_sandbox_command_tool` checkpoint. A setting like `tool_checkpoint_config_by_name={"run_sandbox_command": {"cache": False}}` only affects the per-tool checkpoint in the first shape. In the second shape, configure cache on `whole_turn` or call `flow.run(cache=False)`.

`checkpoint_strategy="calls"` is the default. It is shown here for clarity when setting per-call checkpoint configs:

```python
durable_agent = KitaruAgent(
    agent,
    checkpoint_strategy="calls",
    model_checkpoint_config={"retries": 3, "cache": True},
    tool_checkpoint_config={"retries": 2, "cache": True},
    tool_checkpoint_config_by_name={
        "lookup_price": {"retries": 5, "cache": True},   # flaky external API
        "fetch_secret": False,             # never checkpoint this tool
    },
    mcp_checkpoint_config={"retries": 3},
)
```

Each config is a `CheckpointConfig` TypedDict accepting:

- `cache: bool | None` — passed through to `@kitaru.checkpoint(cache=...)`. Use `True` to opt adapter-created checkpoints into step caching, `False` to disable caching for that boundary, or omit it / use `None` to inherit the stack default.
- `runtime: "inline"` — run in-process. `runtime="isolated"` is not yet supported on adapter-managed checkpoints and raises `KitaruUsageError`.
- `retries: int` — auto-retry the call on failure.
- `type: str` — dashboard grouping. Defaults to `"llm_call"`, `"tool_call"`, or `"mcp_call"` so adapter checkpoints group with native `kitaru.llm()` / `@kitaru.checkpoint(type="tool_call")` calls.

The turn checkpoint itself is configured via `turn_checkpoint_config=` with `checkpoint_strategy="turn"`. To opt into one checkpoint per agent run, pass:

```python
durable_agent = KitaruAgent(agent, checkpoint_strategy="turn")
```

{% hint style="info" %}
**Looking for `granular_checkpoints`?** It still works as a
backwards-compatible alias, not a removed feature. Prefer `checkpoint_strategy` in new code:
`granular_checkpoints=True` → `checkpoint_strategy="calls"`; and
`granular_checkpoints=False` → `checkpoint_strategy="turn"`.
{% endhint %}

{% hint style="info" %}
**Streaming exception.** `checkpoint_strategy="calls"` cannot apply to streamed
turns — per-call checkpointing around an async context manager would require
draining and replaying the stream inside a sync checkpoint. When an
`event_stream_handler` is supplied, `KitaruAgent` transparently falls back to
a turn checkpoint for that call. That fallback disables turn-checkpoint
caching for the call, because a cached final result would skip the handler's
progress side effects. `run_stream()` and `iter()` always require an explicit
`@kitaru.checkpoint`.
{% endhint %}

### Cross-adapter vocabulary

All adapters use `checkpoint_strategy`, but the values name the real boundary each framework exposes to Kitaru:

| Adapter | Per-call strategy | Coarse strategy | What the coarse name means |
|---|---|---|---|
| PydanticAI | `"calls"` | `"turn"` | One PydanticAI agent run/turn |
| OpenAI Agents | `"calls"` | `"runner_call"` | One outer OpenAI `Runner.run(...)` call |
| LangGraph | `"calls"` where sync middleware owns the handler call | `"graph_call"` | One outer graph invocation; LangGraph still owns graph-internal state |
| Claude Agent SDK | Not supported | `"invocation"` | One Claude SDK query/invocation |

So `"calls"` is a shared idea, not a promise of identical mechanics. It means Kitaru can create per-call checkpoints only where the adapter physically owns a replay-safe call body.

## Streaming

PydanticAI streaming has two records in Kitaru:

1. **Live events** are the radio chatter while the agent is running. They are
   useful for a dashboard, terminal watcher, or progress log.
2. **Checkpoint outputs and artifacts** are the saved truth. If you need to
   resume, inspect, or replay a run later, read the final result plus artifacts
   such as `pydantic_ai_events`, `pydantic_ai_run_summaries`, and
   `stream_transcript`.

Kitaru publishes these adapter-specific live event kinds when the backend
supports live event streaming:

- `pydantic_ai.stream.started`
- `pydantic_ai.stream.event`
- `pydantic_ai.stream.completed`
- `pydantic_ai.stream.failed`

The recommended PydanticAI path is `event_stream_handler` on `run()` /
`run_sync()`. PydanticAI drives the full agent graph, including tool calls, while
Kitaru watches the same event stream and publishes privacy-preserving live
updates:

```python
from typing import Any

from pydantic_ai import Agent, RunContext
from kitaru.adapters.pydantic_ai import KitaruAgent


async def drain_events(_ctx: RunContext[None], stream: Any) -> None:
    async for _event in stream:
        pass

agent = Agent("openai:gpt-5-nano", name="support_agent")
durable_agent = KitaruAgent(agent, event_stream_handler=drain_events)
result = durable_agent.run_sync("Check order ORD-1007").output
```

Watch those events from another thread or process with the normal execution
watcher. Import the event-kind tuple instead of hard-coding strings:

```python
from kitaru.client import KitaruClient
from kitaru.adapters.pydantic_ai import PYDANTIC_AI_STREAM_EVENT_KINDS

for event in KitaruClient().executions.events(
    exec_id,
    kinds=list(PYDANTIC_AI_STREAM_EVENT_KINDS),
):
    data = event.payload.get("data", {})
    print(data.get("display", event.kind))
```

`run_stream()` and `iter()` are still available, but they return async context
managers. Kitaru cannot safely auto-open a function-shaped checkpoint around a
context manager, so those surfaces require an explicit `@kitaru.checkpoint`.
PydanticAI's `run_stream()` can stop after the first output that matches the
agent output type; if you want full graph completion plus live observation, use
`run()` / `run_sync()` with `event_stream_handler` instead.

Replay and cache behavior is the same as other live events: if the checkpoint
body re-executes, live events may be published again; if Kitaru reuses a cached
checkpoint result, the body does not run and there may be no live stream events.
Use the durable result and artifacts for saved state.

Live payloads are deliberately small. Kitaru includes safe fields such as event
category, event type, tool names or IDs, short display text, and clipped text
deltas when stream transcripts are enabled. It does not publish raw prompts,
full tool arguments, full tool results, final outputs, raw upstream event dumps,
or reasoning content.

Stream transcripts are persisted as artifacts when
`CapturePolicy.save_stream_transcripts=True` (the default). Set
`CapturePolicy.emit_child_events=False` to turn off adapter-owned child/live
events while keeping normal PydanticAI execution behavior.

## Capture policy

`CapturePolicy` controls what the adapter stores per run. Defaults favor full observability. Wait records always keep minimal routing metadata (`adapter`, `tool_name`, `tool_call_id`), but tool args and exception payloads are only stored in wait metadata when `tool_capture="full"`.

| Option | Default | Description |
|---|---|---|
| `emit_child_events` | `True` | Track per-request / per-tool events. `False` disables tool-wait correlation. |
| `save_prompts` | `True` | Persist prompts sent to the model. |
| `save_responses` | `True` | Persist final model responses. |
| `save_stream_transcripts` | `True` | Persist serialized stream events + final response. |
| `tool_capture` | `"full"` | `"full"` (args + result), `"metadata"` (timing only), or `None` (skip entirely). |
| `tool_capture_overrides` | `{}` | Per-tool overrides keyed by tool name. |
| `correlate_otel_spans` | `True` | Attach Kitaru event IDs to the current OTel span. |

```python
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent

durable_agent = KitaruAgent(
    agent,
    capture=CapturePolicy(
        save_prompts=False,                               # privacy
        save_stream_transcripts=False,                    # cost
        tool_capture="metadata",                          # default for all tools
        tool_capture_overrides={"fetch_secret": None},    # never capture this tool
    ),
)
```

Capture policy is observability-only — it never changes tool execution.

## Usage and cost statistics

When `emit_child_events=True` (the default) and the wrapped model runs inside a Kitaru checkpoint, the adapter records each observed Pydantic AI model request as a canonical `llm_usage_v1` record. The record uses the model event ID as its stable identity and includes the Pydantic AI `RequestUsage` payload when the model response exposes one.

The Pydantic AI adapter does not receive provider-reported dollar cost through this path. Without a user calculator, Kitaru estimates with `genai-prices` for each model event when Pydantic AI exposes a supported provider, model name, and priceable token counts. If a model response has no usage payload, Kitaru can still record that the model request happened; the token fields simply remain empty.

If you know the pricing you want to apply, pass `cost_calculator=` to `KitaruAgent`. Kitaru calls it with a `PydanticAIUsageSummary` containing the model name, provider name when Pydantic AI exposes it, and normalized token counts. A non-negative return value is recorded as `estimated_cost_usd`, and user calculators take priority over the built-in estimate.

```python
from kitaru.adapters.pydantic_ai import KitaruAgent, PydanticAIUsageSummary


def calculate_agent_cost(usage: PydanticAIUsageSummary) -> float | None:
    if usage.input_tokens is None or usage.output_tokens is None:
        return None
    # Replace these example rates with your own contract or billing source.
    return (usage.input_tokens / 1_000_000 * 1.25) + (
        usage.output_tokens / 1_000_000 * 10.0
    )


durable_agent = KitaruAgent(agent, cost_calculator=calculate_agent_cost)
```

If Kitaru cannot identify the provider/model for a model event, or `genai-prices` cannot price it, the usage record keeps the tokens and leaves cost empty. Disable automatic estimates with `KITARU_LLM_ESTIMATED_COSTS=off` when you want token-only adapter records unless a user calculator is configured.

In `checkpoint_strategy="calls"`, cached model checkpoint results are recorded as `reused_not_incurred`, so replay and cache hits do not look like fresh spend when Kitaru builds execution-level usage summaries. Kitaru normally writes those summaries when executions finish; `FlowHandle.wait()` and `FlowHandle.get()` can populate missing summaries for older executions or executions where the finish-time summary was not written. Setting `emit_child_events=False` disables the model/tool event tracking that produces these per-model usage records.

## Message history

Pass `message_history` explicitly like any PydanticAI agent, or let the adapter thread it for you:

```python
durable_agent = KitaruAgent(agent, persist_message_history=True)

durable_agent.run_sync("Hi, I am Alice.")
durable_agent.run_sync("What's my name?")  # sees the prior turn automatically
```

With `persist_message_history=True` the adapter remembers `result.all_messages()` on the instance after each run and auto-injects it as `message_history` on the next call when the caller doesn't pass one. **One `KitaruAgent` instance = one conversation** — create separate instances for separate conversations. An explicit `message_history=` on a single call overrides the remembered history for that call only.

{% hint style="warning" %}
**Limits.** History lives on the Python instance: a restart, new process, or
replay of a prior flow starts with no history. The list grows unbounded — apply
your own truncation or summarization for long-lived conversations. Concurrent
`run` / `run_sync` calls on the same instance race on the stored history; use
one instance per conversation. If you need durable conversation state, persist
`result.all_messages()` in your own storage and pass it back as explicit
`message_history` on the next call.
{% endhint %}

## Constraints

- **Concrete model at construction time.** The wrapped agent must have a bound `Model` — late model binding and per-run `model=` overrides are not supported. To use a different model, wrap a different agent.
- **Stable agent name.** `name=` is required; the adapter uses it for artifact keys and auto-created flow/checkpoint names. Changing it orphans existing executions.
- **No nested checkpoints.** Kitaru forbids opening a checkpoint inside another, so `checkpoint_strategy="calls"` cannot coexist with an enclosing turn checkpoint — the adapter runs the agent body inline at flow scope when per-call checkpoints are enabled.

{% hint style="warning" %}
**Build the agent at run time, not module scope, when the key comes from a remote secret.** Because the model is bound at construction, `Agent("openai:...")` builds the provider client — and reads `OPENAI_API_KEY` — the moment it runs. On a remote stack the runner pod imports your module *before* the run's secret is applied to the environment, so a module-scope `KitaruAgent` crashes at import with a missing-key error. Wrap construction in a small factory and call it inside your flow or checkpoint instead:

```python
def new_agent() -> KitaruAgent:
    return KitaruAgent(Agent("openai:gpt-5-nano", name="researcher"))

@kitaru.checkpoint
def ask(prompt: str) -> str:
    return new_agent().run_sync(prompt).output
```

Locally this is moot (the key is in your shell at import), so it's easy to miss until you run remotely. See [Manage Secrets](../guides/secrets.md#use-a-secret-inside-a-checkpoint).
{% endhint %}

## Advanced composition

Most users only need `KitaruAgent`. For custom durable surfaces, the lower-level wrappers are exported:

- `KitaruModel` — wrap a PydanticAI `Model` directly.
- `KitaruToolset` / `KitaruFunctionToolset` / `KitaruMCPServer` — wrap toolsets or MCP servers independently.
- `kitaruify_toolset(toolset, capture=..., ...)` — dispatch helper that picks the right wrapper class.
- `KitaruRunContext` — `RunContext` subclass that survives isolated-runtime serialization boundaries.

## Troubleshooting

- **"KitaruAgent requires the wrapped agent to define a concrete model"** — pass `model=` to the `Agent()` constructor, not to `run()`.
- **"requires an explicit `@kitaru.checkpoint`"** — `run_stream()` and `iter()` return context managers; wrap them in a checkpoint yourself.
- **Auto-flow fails on a remote stack** — the in-process registry doesn't cross process boundaries. Use `@kitaru.flow` explicitly.
- **Missing-key / `OpenAIError` at import on a remote stack** — a module-scope `KitaruAgent` builds the provider client before the run's secret is applied. Construct the agent inside your flow or checkpoint via a factory function (see the warning under [Constraints](#constraints)).
- **Too many per-call checkpoints** — pass `checkpoint_strategy="turn"` to group a whole agent run into one turn checkpoint. Existing `granular_checkpoints=False` code still works as a compatibility alias.
- **Replay cost control** — `checkpoint_strategy="calls"` gives per-call checkpoint boundaries, not a billing guarantee. Pair it with provider-side caching or idempotency for expensive calls.
- **Tool calls show up only under artifacts or metadata** — check whether `durable_agent.run(...)` / `run_sync(...)` is inside your own `@kitaru.checkpoint`. That checkpoints the whole agent turn, so model/tool activity is recorded under that checkpoint instead of as separate `*_tool` rows. Move the agent call directly into a `@kitaru.flow` body to get per-call checkpoints.
- **Checkpoints not appearing in the dashboard** — verify `kitaru status` shows a running server and that `kitaru init` has been run in the project root.

## Runnable examples

The base adapter example uses PydanticAI's `TestModel`, so it needs no provider
key:

```bash
uv sync --extra local --extra pydantic-ai
uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py
```

The streaming example uses a real OpenAI-backed PydanticAI model so users can
watch live provider events. Set `OPENAI_API_KEY` first:

```bash
uv sync --extra local --extra pydantic-ai --extra openai
export OPENAI_API_KEY=sk-...
uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py
```

Set `PYDANTIC_AI_MODEL` to override the default `openai:gpt-5-nano` model. The
example submits a flow, watches `pydantic_ai.stream.*` events, and then prints
the durable final answer from `.wait()`.

The sandbox toolset example asks a real PydanticAI model to call
`run_sandbox_command` for `python --version` through your current stack's
sandbox:

```bash
uv sync --extra local --extra pydantic-ai --extra openai
uv run kitaru init
export OPENAI_API_KEY=sk-...
uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py
```

Your current Kitaru stack must have exactly one sandbox component. Check it
with `uv run kitaru stack current`, then inspect it with
`uv run kitaru stack show <name>`. If it has no sandbox component, create a
sandbox-enabled local stack with:

```bash
uv run kitaru stack create sandbox-demo --sandbox local
```

Set `PYDANTIC_AI_MODEL` if you want to use a model other than the default
`openai:gpt-5-nano`. The example runs the agent directly in the flow body so
`run_sandbox_command_tool` is visible as its own checkpoint, then passes the
answer into a small `publish_sandbox_answer` checkpoint for inspection. The
script calls `.wait()` and prints the persisted final run output.

For the broader catalog, see [Examples](../getting-started/examples.md).

## Related guides

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Replay and overrides</strong></td><td>Re-run a flow with cached outputs for completed checkpoints</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Wait, Input, and Resume</strong></td><td>How paused flows are resolved from Python, CLI, and MCP</td><td><a href="../guides/wait-and-resume.md">../guides/wait-and-resume.md</a></td></tr><tr><td><strong>Tracked LLM calls</strong></td><td>Use kitaru.llm() with model aliases and transported registry</td><td><a href="../guides/llm-calls.md">../guides/llm-calls.md</a></td></tr></tbody></table>
