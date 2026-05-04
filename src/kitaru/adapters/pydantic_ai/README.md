# Durable Execution with Kitaru

[Kitaru](https://kitaru.ai/) is ZenML's durable execution layer for AI agents. This adapter makes any [Pydantic AI](https://ai.pydantic.dev) agent replayable, resumable, and observable without changing its code: wrap the agent once, and every model request, tool call, MCP invocation, and human-in-the-loop wait is persisted under a Kitaru flow.

```python
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent('openai:gpt-4o', name='researcher')
durable_agent = KitaruAgent(agent)

result = durable_agent.run_sync('Summarize quantum error correction.')
print(result.output)
```

That's it. No flow decorator, no checkpoint annotations, no graph DSL — the adapter auto-opens a Kitaru flow when you call `run()` / `run_sync()` outside of an existing flow. In the default *turn* mode it also opens one `@kitaru.checkpoint` per run; in granular mode (`granular_checkpoints=True`) each model/tool/MCP call opens its own checkpoint instead. The dashboard at `http://localhost:8080` (or your deployed Kitaru server) shows every turn, tool call, model response, and wait point.

Migration note: `kp.wrap(...)` remains available as a deprecated shim for one release. Prefer `KitaruAgent(...)` directly. Legacy capture values map as `"metadata_only"` -> `"metadata"` and `"off"` -> `None`.

## Concepts in 30 seconds

Kitaru wraps agent work in three nested primitives:

- **Flow** — the top-level durable boundary for a workflow, created by `@kitaru.flow` or auto-opened by `KitaruAgent`.
- **Checkpoint** — a persisted unit of work. Every checkpoint output is stored; if the flow crashes, is replayed, or resumes after a wait, Kitaru skips completed checkpoints and re-runs from the first incomplete one.
- **Wait** — suspends a running flow until a human or external system provides input. The adapter bridges Pydantic AI's `ApprovalRequired`, `CallDeferred`, and `@hitl_tool` straight into `kitaru.wait()`.

```text
┌─────────────────── @kitaru.flow ────────────────────┐
│                                                     │
│  ┌────── @kitaru.checkpoint ──────┐                 │
│  │ KitaruAgent.run_sync(...)      │                 │
│  │   ├── model request  ─→ artifact, OTel span     │
│  │   ├── tool call      ─→ artifact, OTel span     │
│  │   ├── MCP tool       ─→ artifact, OTel span     │
│  │   └── hitl / approval ─→ kitaru.wait() suspends │
│  └────────────────────────────────┘                 │
└─────────────────────────────────────────────────────┘
```

See the [Kitaru docs](https://kitaru.ai/docs) for the full primitive set.

## Installation

Install the adapter with the `pydantic-ai` extra:

```bash
uv add "kitaru[pydantic-ai]"
# or: pip install "kitaru[pydantic-ai]"
```

For local durable execution (dashboard, REST API, replay UI), add the `local` extra:

```bash
uv add "kitaru[pydantic-ai,local]"
```

Initialize the project once — this creates a `.kitaru/` directory and a shared config:

```bash
kitaru init
kitaru login              # local server
kitaru status
```

To connect to a deployed server instead:

```bash
kitaru login https://my-server.example.com
```

That is the full setup. The base `kitaru[pydantic-ai]` install is enough for durable execution; `[local]` is only needed if you want the local server and dashboard.

## Usage patterns

### 1. Zero-config: wrap once, call directly

The simplest case — adopting durable execution requires a single line. `KitaruAgent` auto-opens a flow and checkpoint when called outside of one.

```python
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent('openai:gpt-4o', name='researcher')
durable_agent = KitaruAgent(agent)

result = durable_agent.run_sync('What are the open questions in QEC?')
```

Use this mode when you're prototyping, porting an existing agent, or running a single-turn interaction.

### 2. Explicit boundaries: multi-step flows

For multi-turn workflows, named replay boundaries, or coordinated waits across agent turns, use `@kitaru.flow` and `@kitaru.checkpoint` yourself. Inside a checkpoint, `KitaruAgent` is a straight passthrough.

```python
import kitaru
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent('openai:gpt-4o', name='researcher')
durable_agent = KitaruAgent(agent)

@kitaru.checkpoint
def ask(prompt: str) -> str:
    return durable_agent.run_sync(prompt).output

@kitaru.flow
def research(topic: str) -> str:
    overview = ask(f'Overview of {topic}')
    return ask(f'Open questions, given this overview:\n{overview}')

handle = research.run('quantum error correction')
print(handle.wait())
```

Replay from any step by re-running the flow with the original run ID — Kitaru serves cached outputs for completed checkpoints and re-executes only what changed.

### 3. Human-in-the-loop tools

The adapter bridges every Pydantic AI deferred pattern into `kitaru.wait()`. A paused flow is visible from `kitaru executions list`, the dashboard, and the REST API; once input is supplied the flow resumes from the exact same point.

```python
from kitaru.adapters.pydantic_ai import hitl_tool

@hitl_tool(question='Approve publishing this brief?', schema=bool)
def publish_brief(headline: str, sources: list[str]) -> str:
    return f'published: {headline} ({len(sources)} sources)'
```

Equivalent triggers that also route through `kitaru.wait()`:

- `@agent.tool(requires_approval=True)` — Pydantic AI's native approval flag.
- Raising `pydantic_ai.exceptions.ApprovalRequired` or `CallDeferred` from any tool body.

### 4. MCP servers

MCP servers attached to the agent are wrapped automatically. Their tool calls appear as `ToolEvent`s with `toolset_kind='mcp'` on the same checkpoint as native tools, and `MCPServer.cache_tools=True` is honored to skip redundant `tools/list` round-trips on replay.

```python
from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio
from kitaru.adapters.pydantic_ai import KitaruAgent

server = MCPServerStdio('npx', args=['-y', '@modelcontextprotocol/server-filesystem', '/tmp'], cache_tools=True)
agent = Agent('openai:gpt-4o', name='researcher', toolsets=[server])
durable_agent = KitaruAgent(agent)
```

## Checkpoint modes

The adapter offers two strategies for how agent work maps onto Kitaru checkpoints. Pick per agent based on how you want to replay and retry.

| Mode | How it maps | Replay unit | Retry unit | Best for |
|---|---|---|---|---|
| **Turn** (default) | One checkpoint per agent run; model/tool/MCP calls are child events | The full turn | The full turn | Most agents — single aggregating artifact, clean run summary |
| **Granular** | No turn checkpoint; each model/tool/MCP call becomes its own checkpoint | Per call | Per call | Expensive model calls, flaky tools, long tool-call chains where one failure shouldn't rewind everything |

**Replay semantics in one sentence.** If a flow crashes on the 8th LLM call of a turn, turn mode re-runs all 7 completed calls from scratch on replay; granular mode serves those 7 from cache and resumes at call 8. Pick granular when wasted LLM spend on replay would hurt; pick turn when a single aggregated run artifact is more valuable than cache granularity.

Turn mode is what you get by default. Granular mode trades the aggregating run artifact for per-call durability:

```python
durable_agent = KitaruAgent(
    agent,
    granular_checkpoints=True,
    model_checkpoint_config={'retries': 3},
    tool_checkpoint_config={'retries': 2},
    tool_checkpoint_config_by_name={
        'lookup_price': {'retries': 5},       # flaky external API
        'fetch_secret': False,                # never checkpoint this tool
    },
    mcp_checkpoint_config={'retries': 3},
)
```

Each config is a `CheckpointConfig` TypedDict accepting:

- `runtime: 'inline'` — run in-process. `runtime='isolated'` is a planned follow-up and currently raises `KitaruUsageError`. Supporting it requires making every adapter wrapper (`KitaruModel`, `KitaruToolset`, `KitaruMCPServer`) reconstructible from serializable construction args on the far side of the process boundary and wiring `KitaruRunContext` through the granular dispatcher — the pydantic payloads already serialize via `TypeAdapter`, so most of the work is on the wrapper-identity side.
- `retries: int` — auto-retry the step on failure.
- `type: str` — dashboard grouping. Defaults to `'llm_call'`, `'tool_call'`, or `'mcp_call'` so adapter checkpoints group with native `kitaru.llm()` / `@kitaru.checkpoint(type='tool_call')` calls.

The turn checkpoint is configured via `turn_checkpoint_config=` in turn mode.

**Streaming exception.** Granular mode cannot apply to streamed turns — per-call checkpointing around an `@asynccontextmanager` would require draining and replaying the stream inside a sync ZenML step. When an `event_stream_handler` is supplied, `KitaruAgent` transparently falls back to opening a turn checkpoint for that call so tracking and durability still work. `run_stream()` and `iter()` always require an explicit `@kitaru.checkpoint` in both modes.

## Streaming

Two patterns supported:

- **`event_stream_handler`** — recommended. Pass a handler to `run()` / `run_sync()` for live progress updates; auto-flow and auto-checkpoint still work.
- **`run_stream()` / `iter()`** — the context-manager APIs cannot auto-open a checkpoint, so wrap them in an explicit `@kitaru.checkpoint` yourself.

Stream transcripts are persisted as artifacts when `CapturePolicy.save_stream_transcripts=True` (the default).

## Capture policy

`CapturePolicy` controls what the adapter stores per run. Defaults favor full observability.

| Option | Default | Description |
|---|---|---|
| `emit_child_events` | `True` | Track per-request / per-tool events. `False` disables tool-wait correlation. |
| `save_prompts` | `True` | Persist prompts sent to the model. |
| `save_responses` | `True` | Persist final model responses. |
| `save_stream_transcripts` | `True` | Persist serialized stream events + final response. |
| `tool_capture` | `'full'` | `'full'` (args + result), `'metadata'` (timing only), `None` (skip entirely). |
| `tool_capture_overrides` | `{}` | Per-tool overrides keyed by tool name. |
| `correlate_otel_spans` | `True` | Attach Kitaru event IDs to the current OTel span. |

```python
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent

durable_agent = KitaruAgent(
    agent,
    capture=CapturePolicy(
        save_prompts=False,                          # privacy
        save_stream_transcripts=False,               # cost
        tool_capture='metadata',                     # default for all tools
        tool_capture_overrides={'fetch_secret': None},  # never capture this tool
    ),
)
```

## Message history

Pass `message_history` explicitly like any PydanticAI agent, or let the adapter thread it for you:

```python
durable_agent = KitaruAgent(agent, persist_message_history=True)

durable_agent.run_sync('Hi, I am Alice.')
durable_agent.run_sync("What's my name?")  # sees the prior turn automatically
```

With `persist_message_history=True` the adapter remembers `result.all_messages()` on the instance after each run and auto-injects it as `message_history` on the next call when the caller doesn't pass one. **One `KitaruAgent` instance = one conversation** — create separate instances for separate conversations. An explicit `message_history=` on a single call overrides the remembered history for that call only.

**Limits to be aware of:**

- **In-memory only.** History lives on the Python instance; a restart, new process, or replay of a prior flow starts with no history. For durable conversation state, persist `result.all_messages()` in your own storage and pass it explicitly.
- **Serial use.** Concurrent `run` / `run_sync` calls on the same instance race on the stored history. Gate concurrency externally, or use one instance per conversation.
- **Unbounded.** The list grows monotonically — apply your own truncation or summarization for long-lived conversations.
- **Success-only.** The instance only updates its history after a successful run. A partial failure leaves the last-successful history in place.

## Requirements and constraints

- **Concrete model at construction time.** The wrapped agent must have a bound `Model` — late model binding and per-run `model=` overrides are not supported. If you need a different model, wrap a different agent.
- **Stable agent name.** A `name` is required; the adapter uses it for artifact keys and auto-created flow/checkpoint names. Changing it orphans existing executions.
- **No nested checkpoints.** Kitaru's MVP forbids opening a checkpoint inside another. Granular mode therefore cannot coexist with an enclosing turn checkpoint — the adapter runs the agent body inline at flow scope when `granular_checkpoints=True`.
- **Auto-flow is local-only.** When called outside any flow, `KitaruAgent` auto-opens one using an in-process registry. Remote stacks (Kubernetes, Vertex, SageMaker, AzureML) cannot see that registry — wrap the call in an explicit `@kitaru.flow` for those. Serializing an arbitrary agent closure isn't worth the machinery when a one-line decorator does the job.

## Advanced composition

Most users should only need `KitaruAgent`. For custom durable surfaces, the lower-level wrappers are exported:

- `KitaruModel` — wrap a Pydantic AI `Model` directly.
- `KitaruToolset` / `KitaruFunctionToolset` / `KitaruMCPServer` — wrap toolsets or MCP servers independently.
- `kitaruify_toolset(toolset, capture=..., ...)` — dispatch helper that picks the right wrapper class.
- `KitaruRunContext` — `RunContext` subclass that survives isolated-runtime serialization boundaries.

## Troubleshooting

- **"KitaruAgent requires the wrapped agent to define a concrete model"** — pass `model=` to the `Agent()` constructor, not to `run()`.
- **"requires an explicit `@kitaru.checkpoint`"** — `run_stream()` and `iter()` return context managers; wrap them in a checkpoint yourself.
- **Auto-flow fails on a remote stack** — the in-process registry doesn't cross process boundaries. Use `@kitaru.flow` explicitly.
- **Replay re-runs every tool** — you're in turn mode. Switch to `granular_checkpoints=True` for per-call cache granularity.
- **Checkpoints not appearing in dashboard** — verify `kitaru status` shows a running server and that `kitaru init` has been run in the project root.

## API reference

All exports live under `kitaru.adapters.pydantic_ai`:

```python
from kitaru.adapters.pydantic_ai import (
    KitaruAgent,
    KitaruModel,
    KitaruToolset, KitaruFunctionToolset, KitaruMCPServer,
    KitaruRunContext,
    CapturePolicy, CaptureMode,
    CheckpointConfig, CheckpointRuntime,
    hitl_tool,
    kitaruify_toolset,
)
```

## Further reading

- [Kitaru docs](https://kitaru.ai/docs) — flows, checkpoints, waits, stacks
- [Pydantic AI docs](https://ai.pydantic.dev) — agents, tools, MCP, deferred execution
- [Example](../../../../examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py) — runnable research agent with a HITL tool
