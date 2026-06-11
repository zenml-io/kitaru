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

That's it. No flow decorator, no checkpoint annotations, no graph DSL — the adapter auto-opens a Kitaru flow when you call `run()` / `run_sync()` outside of an existing flow. By default, model, tool, and MCP calls are persisted as separate granular checkpoints. The dashboard at `http://localhost:8080` (or your deployed Kitaru server) shows the run, tool calls, model responses, and wait points.

Migration note: `kp.wrap(...)` remains available as a deprecated shim for one release. Prefer `KitaruAgent(...)` directly. Legacy capture values map as `"metadata_only"` -> `"metadata"` and `"off"` -> `None`.

## Concepts in 30 seconds

Kitaru wraps agent work in three nested primitives:

- **Flow** — the top-level durable boundary for a workflow, created by `@kitaru.flow` or auto-opened by `KitaruAgent`.
- **Checkpoint** — a persisted unit of work. Every checkpoint output is stored; if the flow crashes, is replayed, or resumes after a wait, Kitaru skips completed checkpoints and re-runs from the first incomplete one.
- **Wait** — suspends a running flow until a human or external system provides input. Waits must be created at flow scope. The adapter makes explicit `@hitl_tool` calls flow-scope safe in granular mode; native `ApprovalRequired`, `CallDeferred`, or `wait_for_input()` inside a regular granular tool checkpoint must opt out of that checkpoint.

```text
┌─────────────────── @kitaru.flow ────────────────────┐
│                                                     │
│  ┌────── @kitaru.checkpoint ──────┐                 │
│  │ KitaruAgent.run_sync(...)      │                 │
│  │   ├── model request  ─→ artifact, OTel span     │
│  │   ├── tool call      ─→ artifact, OTel span     │
│  │   ├── MCP tool       ─→ artifact, OTel span     │
│  │   └── child events    ─→ metadata/artifacts     │
│  └────────────────────────────────┘                 │
│  explicit @hitl_tool ─→ kitaru.wait() suspends      │
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

The simplest case — adopting durable execution requires a single line. `KitaruAgent` auto-opens a flow and granular per-call checkpoints when called outside of one.

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

Other PydanticAI deferred patterns can also route through `kitaru.wait()` **when they run at flow scope**:

- `@agent.tool(requires_approval=True)` — Pydantic AI's native approval flag.
- Raising `pydantic_ai.exceptions.ApprovalRequired` or `CallDeferred` from a tool body.
- Calling `kitaru.adapters.pydantic_ai.wait_for_input(...)` from a tool body.

Durable waits need a stable Pydantic AI `tool_call_id`. The adapter uses that id to make the wait name deterministic, so a resumed run looks for the same human input instead of inventing a new wait. If Pydantic AI does not provide a stable, sanitizable `tool_call_id`, Kitaru raises rather than falling back to a random wait name.

With the default `checkpoint_strategy="calls"`, explicit `@hitl_tool` calls create a wait point directly at flow scope instead of first creating an empty `*_tool` checkpoint. Concretely, the timeline shows the human wait as the durable anchor for that call. Prefer `@hitl_tool` for tools that are purely human-input gates.

Regular sync tool bodies are different. A normal tool with `checkpoint_strategy="calls"` usually runs inside an adapter-created `*_tool` checkpoint, and Pydantic AI normally runs sync tools on a worker thread. Kitaru waits need both conditions to be safe: they must be outside the synthetic checkpoint and on the workflow thread. `wait_for_input()` does not bypass these guards: if a regular tool body raises `ApprovalRequired` / `CallDeferred` or calls `wait_for_input()` from inside that checkpoint, Kitaru fails early with guidance instead of creating a confusing checkpoint-contained wait.

Use one of these safe patterns for regular tool-body waits. The per-tool `False` opt-out keeps the wait out of the synthetic `*_tool` checkpoint. The separate `allow_sync_tool_body_waits=True` flag explicitly asks Kitaru to activate Pydantic AI thread compatibility for supported sync tools during the agent run. That compatibility layer is run-wide, so Kitaru only enables it when you ask for it directly. The trade-off is concrete: any supported sync tool in that run may execute inline instead of using Pydantic AI's normal worker-thread path, so avoid mixing this opt-in with slow/blocking sync tools if you rely on normal tool parallelism:

```python
durable_agent = KitaruAgent(
    agent,
    tool_checkpoint_config_by_name={"ask_user": False},  # checkpoint opt-out only
    allow_sync_tool_body_waits=True,  # run sync tool bodies on the workflow thread
)
```

Or move the human gate outside the tool entirely — for example, call `kitaru.wait()` before or after the agent turn in your `@kitaru.flow` code.

If the compatibility layer is unavailable for a future Pydantic AI version, Kitaru fails before the agent enters user sync tool bodies and includes installed-version details plus guidance to use `@hitl_tool(...)` or move the wait into explicit flow code.

This also affects where event details land. Flow-scope explicit HITL calls are still logged as adapter event metadata, but checkpoint artifacts such as `event_log`, `run_summary`, and captured tool args/results are only saved when there is an actual checkpoint scope to attach them to.

### 4. MCP servers

MCP servers attached to the agent are wrapped automatically. Their tool calls appear as `ToolEvent`s with `toolset_kind='mcp'` alongside native tools. With the default `checkpoint_strategy="calls"`, each top-level MCP call gets its own adapter checkpoint when Kitaru can safely own the MCP call lifecycle. `MCPServer.cache_tools=True` is honored to skip redundant `tools/list` round-trips on replay.

If the PydanticAI MCP server is already running because you entered it with `async with server:` or `async with agent:`, behavior depends on where the agent run happens:

- **Inside an explicit `@kitaru.flow`:** Kitaru keeps the MCP call on the current event loop instead of moving it into the worker-thread checkpoint bridge. That avoids the concrete failure mode where the MCP request reaches the server successfully, but the client waits or tears down from the wrong event loop afterwards. In this pre-opened case the call is still tracked as adapter event metadata, but it is not persisted as its own per-call `mcp_call` checkpoint, so checkpoint-scoped args/result artifacts are not saved for that call.
- **Outside an explicit `@kitaru.flow`:** `KitaruAgent.run(...)` would normally auto-open a flow by moving the async agent body to a worker thread/event loop. If the MCP server is already open on your caller loop, Kitaru fails fast with `KitaruUsageError` instead of risking a hang. Wrap the call in an explicit flow while the MCP lifecycle is open, or do not pre-open the MCP server and let PydanticAI/Kitaru auto-connect it.

If you need per-call MCP checkpointing, let PydanticAI auto-connect the MCP server so the connection opens inside the checkpoint worker loop.

```python
from kitaru.adapters.pydantic_ai import KitaruAgent
from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio

server = MCPServerStdio('npx', args=['-y', '@modelcontextprotocol/server-filesystem', '/tmp'], cache_tools=True)
agent = Agent('openai:gpt-4o', name='researcher', toolsets=[server])
durable_agent = KitaruAgent(agent)
```

### 5. Active-stack sandbox commands

Import `sandbox_command_toolset` from this package when a PydanticAI agent should run commands in the active Kitaru stack sandbox:

```python
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent, sandbox_command_toolset

agent = Agent(
    'openai:gpt-4o',
    name='sandboxed_agent',
    toolsets=[sandbox_command_toolset(max_chars=20_000)],
)
durable_agent = KitaruAgent(
    agent,
    tool_checkpoint_config_by_name={
        'run_sandbox_command': {'cache': False},
    },
)
```

The factory returns a normal PydanticAI `FunctionToolset` with one tool named `run_sandbox_command`. Kitaru wraps it as a function toolset, so existing tracking and calls-strategy checkpoints still apply. The active stack must have exactly one sandbox component. Every tool call creates one temporary sandbox session, runs one command, returns `stdout`, `stderr`, `exit_code`, truncation flags, and cleanup status, then closes or destroys the temporary session. The adapter default is 20,000 characters per output stream; pass `max_chars=` when you need a different limit. Pass `cleanup="destroy"` or `cleanup="close"` to choose how the temporary session is cleaned up after the command: `"destroy"` is the default and removes the session when supported, while `"close"` only closes the session handle.

Non-zero process exits come back as normal `SandboxCommandToolResult` values. Missing sandbox setup or backend failures raise public Kitaru errors and are recorded as failed tool calls. The model-facing tool accepts only `command` and optional `cwd`; `env` is deliberately not exposed because captured tool arguments may be stored as artifacts.

## Checkpoint strategy

The adapter offers two strategies for how agent work maps onto Kitaru checkpoints. Pick per agent based on how you want to replay and retry.

| Strategy | How it maps | Replay/retry unit | Best for |
|---|---|---|---|
| `"calls"` (default) | No turn checkpoint; each model/tool/MCP call becomes its own checkpoint | Per call | Expensive model calls, flaky tools, long tool-call chains where one failure shouldn't rewind everything |
| `"turn"` | One checkpoint per agent run; model/tool/MCP calls are child events | The full turn | Agents where one aggregated checkpoint and checkpoint artifacts like `event_log` / `run_summary` are more useful than per-call boundaries |

**Replay semantics in one sentence.** If a flow crashes on the 8th LLM call of a turn, `"turn"` re-runs the whole turn; `"calls"` gives the earlier calls their own completed checkpoint boundaries. If you set `cache=True` on the per-call model/tool/MCP configs, repeated runs can also reuse completed per-call checkpoints when the logical inputs are the same.

For model checkpoints, the adapter builds the cache key from the prompt/messages, model settings, and model request parameters. Pydantic AI adds fresh per-run envelope fields such as `timestamp` and `run_id` to its internal message objects; Kitaru ignores those envelope labels for the cache key so the same prompt can hit cache on run 2. It does **not** strip user content: if your prompt text, tool args, or tool result contains words or fields named `timestamp` or `run_id`, those still count as part of the logical input. Changed prompts, message history, tool arguments, tool call IDs, model settings, or request parameters still produce a new cache key and should miss cache.

`checkpoint_strategy="calls"` is the default. It is shown here for clarity when setting per-call checkpoint configs:

```python
durable_agent = KitaruAgent(
    agent,
    checkpoint_strategy="calls",
    model_checkpoint_config={'retries': 3, 'cache': True},
    tool_checkpoint_config={'retries': 2, 'cache': True},
    tool_checkpoint_config_by_name={
        'lookup_price': {'retries': 5, 'cache': True},  # flaky external API
        'fetch_secret': False,                # never checkpoint this tool
    },
    mcp_checkpoint_config={'retries': 3},
)
```

Each config is a `CheckpointConfig` TypedDict accepting:

- `cache: bool | None` — passed through to `@kitaru.checkpoint(cache=...)`. Use `True` to opt adapter-created checkpoints into ZenML/Kitaru step caching, `False` to disable caching for that boundary, or omit it / use `None` to inherit the stack default.
- `runtime: 'inline'` — run in-process. `runtime='isolated'` is a planned follow-up and currently raises `KitaruUsageError`. Supporting it requires making every adapter wrapper (`KitaruModel`, `KitaruToolset`, `KitaruMCPServer`) reconstructible from serializable construction args on the far side of the process boundary and wiring `KitaruRunContext` through the granular dispatcher — the pydantic payloads already serialize via `TypeAdapter`, so most of the work is on the wrapper-identity side.
- `retries: int` — auto-retry the step on failure.
- `type: str` — dashboard grouping. Defaults to `'llm_call'`, `'tool_call'`, or `'mcp_call'` so adapter checkpoints group with native `kitaru.llm()` / `@kitaru.checkpoint(type='tool_call')` calls.

The turn checkpoint is configured via `turn_checkpoint_config=` with `checkpoint_strategy="turn"`. To opt into one checkpoint per agent run, pass:

```python
durable_agent = KitaruAgent(agent, checkpoint_strategy="turn")
```

**Looking for `granular_checkpoints`?** It still works as a backwards-compatible alias, not a removed feature. Prefer `checkpoint_strategy` in new code:

- `granular_checkpoints=True` → `checkpoint_strategy="calls"`
- `granular_checkpoints=False` → `checkpoint_strategy="turn"`

**Streaming exception.** `checkpoint_strategy="calls"` cannot apply to streamed turns — per-call checkpointing around an `@asynccontextmanager` would require draining and replaying the stream inside a sync ZenML step. When an `event_stream_handler` is supplied, `KitaruAgent` transparently falls back to opening a turn checkpoint for that call so tracking and durability still work. That fallback disables turn-checkpoint caching for the call, because serving the final result from cache would skip the handler's progress side effects. `run_stream()` and `iter()` always require an explicit `@kitaru.checkpoint` in both modes.

## Streaming

Two patterns supported:

- **`event_stream_handler`** — recommended. Pass a handler to `run()` / `run_sync()` for live progress updates; auto-flow and auto-checkpoint still work.
- **`run_stream()` / `iter()`** — the context-manager APIs cannot auto-open a checkpoint, so wrap them in an explicit `@kitaru.checkpoint` yourself.

Stream transcripts are persisted as artifacts when `CapturePolicy.save_stream_transcripts=True` (the default).

## Capture policy

`CapturePolicy` controls what the adapter stores per run. Defaults favor full observability. Wait records always keep minimal routing metadata (`adapter`, `tool_name`, `tool_call_id`), but tool args and exception payloads are only stored in wait metadata when `tool_capture='full'`.

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

With `persist_message_history=True` the adapter remembers `result.all_messages()` on the instance after each successful `run()` / `run_sync()` and auto-injects it as `message_history` on the next call when the caller doesn't pass one. This refresh also happens when an adapter-owned checkpoint returns a cached Pydantic AI result, because the adapter can read `all_messages()` from the returned result. **One `KitaruAgent` instance = one conversation** — create separate instances for separate conversations. An explicit `message_history=` on a single call overrides the remembered history for that call only.

**Limits to be aware of:**

- **In-memory only.** History lives on the Python instance. Adapter-owned cached turns can refresh it from their returned result, but a restart, new process, or replay path that skips the adapter call still starts with no instance history.
- **Do not hide the whole agent call inside a cached checkpoint if you rely on this.** If an outer `@kitaru.checkpoint` returns from cache, `KitaruAgent.run*()` never executes, so the adapter cannot restore `_last_messages`. The adapter warns once when `persist_message_history=True` is used inside an existing checkpoint.
- **For fully durable conversation state, persist it yourself.** Store `result.all_messages()` in your own durable storage (database, file, or `kitaru.save()` artifact) and pass it back with `message_history=`.
- **Serial use.** Concurrent `run` / `run_sync` calls on the same instance race on the stored history. Gate concurrency externally, or use one instance per conversation.
- **Unbounded.** The list grows monotonically — apply your own truncation or summarization for long-lived conversations.
- **Success-only.** The instance only updates its history after a successful run. A partial failure leaves the last-successful history in place.

## Requirements and constraints

- **Concrete model at construction time.** The wrapped agent must have a bound `Model` — late model binding and per-run `model=` overrides are not supported. If you need a different model, wrap a different agent.
- **Stable agent name.** A `name` is required; the adapter uses it for artifact keys and auto-created flow/checkpoint names. `KitaruAgent(name=...)` wins over the wrapped Pydantic AI `Agent(name=...)`; if the wrapper name is omitted, the wrapped agent name is used. Changing this stable name orphans existing executions.
- **No nested checkpoints.** Kitaru's MVP forbids opening a checkpoint inside another. `checkpoint_strategy="calls"` therefore cannot coexist with an enclosing turn checkpoint — the adapter runs the agent body inline at flow scope when per-call checkpoints are enabled.
- **Auto-flow is local-only.** When called outside any flow, `KitaruAgent` auto-opens a flow named `{stable_agent_name}_flow` using an in-process registry. If you call the agent inside your own explicit `@kitaru.flow`, that outer flow keeps its own name. Remote stacks (Kubernetes, Vertex, SageMaker, AzureML) cannot see the auto-flow registry — wrap the call in an explicit `@kitaru.flow` for those. Serializing an arbitrary agent closure isn't worth the machinery when a one-line decorator does the job.

## Advanced composition

Most users should only need `KitaruAgent`. For custom durable surfaces, the lower-level wrappers are exported:

- `KitaruModel` — wrap a Pydantic AI `Model` directly.
- `KitaruToolset` / `KitaruFunctionToolset` / `KitaruMCPServer` — wrap toolsets or MCP servers independently.
- `kitaruify_toolset(toolset, capture=..., ...)` — dispatch helper that picks the right wrapper class.
- `CheckpointStrategy` — public type alias for supported checkpoint strategy values.
- `validate_checkpoint_strategy(...)` — normalize and validate checkpoint strategy inputs.
- `KitaruRunContext` — `RunContext` subclass that survives isolated-runtime serialization boundaries.

## Troubleshooting

- **"KitaruAgent requires the wrapped agent to define a concrete model"** — pass `model=` to the `Agent()` constructor, not to `run()`.
- **"requires an explicit `@kitaru.checkpoint`"** — `run_stream()` and `iter()` return context managers; wrap them in a checkpoint yourself.
- **Auto-flow fails on a remote stack** — the in-process registry doesn't cross process boundaries. Use `@kitaru.flow` explicitly.
- **Too many per-call checkpoints** — pass `checkpoint_strategy="turn"` to group a whole agent run into one turn checkpoint. Existing `granular_checkpoints=False` code still works as a compatibility alias.
- **Replay cost control** — `checkpoint_strategy="calls"` gives per-call checkpoint boundaries, not a billing guarantee. Pair it with provider-side caching or idempotency for expensive calls.
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
    CheckpointStrategy, validate_checkpoint_strategy,
    hitl_tool,
    wait_for_input,
    kitaruify_toolset,
    sandbox_command_toolset,
    SandboxCommandToolResult,
    SANDBOX_COMMAND_TOOL_NAME,
    DEFAULT_SANDBOX_TOOL_MAX_CHARS,
)
```

## Further reading

- [Kitaru docs](https://kitaru.ai/docs) — flows, checkpoints, waits, stacks
- [Pydantic AI docs](https://ai.pydantic.dev) — agents, tools, MCP, deferred execution
- [Example](../../../../examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py) — runnable research agent with a HITL tool
