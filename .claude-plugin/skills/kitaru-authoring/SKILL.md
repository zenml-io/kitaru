---
name: kitaru-authoring
description: >
  Guide for writing Kitaru durable workflows and operational control paths. Use
  when creating or refactoring Kitaru flows, checkpoints, waits, logging,
  artifacts, tracked LLM calls, replay/resume/retry flows, KitaruClient usage,
  CLI commands, MCP operations, or PydanticAI adapter integrations. Triggers on
  mentions of kitaru, @flow, @checkpoint, kitaru.wait, kitaru.log,
  kitaru.save, kitaru.load, kitaru.llm, KitaruClient, replay, resume, retry,
  `kitaru run`, `kitaru executions ...`, MCP tools, `wrap(...)`, or
  `hitl_tool(...)`.
---

# Kitaru Authoring Skill

Use this guide when writing or refactoring Kitaru workflows and when choosing
which Kitaru surface to use for running, observing, replaying, or controlling
those workflows.

> **Before building**: If the workflow shape is still fuzzy, suggest the
> `kitaru-scoping` skill first. It helps the user decide whether Kitaru is a
> fit, where checkpoints and waits belong, and which replay anchors should be
> stable before code gets written.

## Mental model

Think of a Kitaru flow like a long trip with named save points.

- `@flow` is the durable outer boundary.
- `@checkpoint` is a replay boundary inside that flow.
- `wait()` pauses at the flow level and resumes later with input.
- Replay reruns from the top, but checkpoints before the selected replay point
  return cached outputs instead of doing the work again.
- Flows are executed with `.run(...)` or `.deploy(...)`, not by calling the
  decorated function directly.

```python
from kitaru import checkpoint, flow, wait

@checkpoint
def draft(topic: str) -> str:
    return f"Draft for {topic}"

@flow
def review_flow(topic: str) -> str:
    text = draft(topic)
    approved = wait(name="approve_draft", question="Approve draft?", schema=bool)
    if not approved:
        return "Rejected"
    return text

handle = review_flow.run("Durable agents")
print(handle.exec_id)
```

A `FlowHandle` is the object you use after submission:

- `handle.exec_id` -> execution ID
- `handle.status` -> current execution status
- `handle.wait()` -> block until terminal state and return the result
- `handle.get()` -> fetch final result (or raise on failure)

## Authoring guardrails

Enforce these rules when writing or reviewing Kitaru code:

1. Do not nest flows.
2. Do not call one checkpoint from inside another checkpoint.
3. Do not call `wait()` inside a checkpoint.
4. `save()` and `load()` require checkpoint scope.
5. `log()` works in flow scope and checkpoint scope, but it attaches metadata to
   different targets depending on where it runs.
6. Checkpoint outputs must be serializable.
7. `.submit()`, `.map()`, and `.product()` are for work launched from inside a
   running flow.
8. Use stable, unique names for checkpoints, waits, and artifacts so replay and
   artifact lookup stay unambiguous.

## Primitive reference

### `@flow`

Use `@flow` for the durable orchestration boundary.

- Supported decorator overrides: `stack`, `image`, `cache`, `retries`
- Main entrypoints:
  - `.run(...)`
  - `.deploy(...)`
  - `.replay(exec_id, from_=..., overrides=..., **flow_inputs)`
- `.deploy(...)` is still execution through the same flow model; it is not a
  separate deployment DSL.

### `@checkpoint`

Use `@checkpoint` for meaningful replayable units of work.

- Supported decorator args: `retries`, `type`
- Supported call styles:
  - direct call inside a flow
  - `.submit(...)`
  - `.map(...)`
  - `.product(...)`
- Keep checkpoints coarse enough to matter and small enough to serialize.

### `wait(...)`

`wait(*, schema=bool, name=None, question=None, timeout=None, metadata=None)`
pauses the flow until input arrives.

- Valid only in the flow body
- Invalid inside checkpoints
- Use simple schemas and stable `name` values
- Timeout defaults exist even if the caller omits one

### `log(...)`

`log(**kwargs)` records structured metadata.

- Inside a checkpoint: metadata is attached to the checkpoint
- Inside a flow but outside a checkpoint: metadata is attached to the execution
- Use this for breadcrumbs, decisions, IDs, and derived metrics

### `save(...)` / `load(...)`

Use explicit artifacts when a checkpoint should publish named outputs for later
inspection or reuse.

- `save(name, value, type="...")` requires checkpoint scope
- `load(exec_id, name)` requires checkpoint scope and an execution UUID string
- Allowed artifact kinds are: `prompt`, `response`, `context`, `input`,
  `output`, `blob`
- Keep artifact names unique within an execution to avoid ambiguous loads

### `llm(...)`

`llm(prompt, *, model=None, system=None, temperature=None, max_tokens=None,
name=None) -> str`

- Accepts a plain string or chat-style message list
- Uses local model alias resolution when `model` names an alias
- Credentials resolve env-first, then optional ZenML secret fallback from the
  alias config
- Inside a checkpoint: runs inline
- Inside a flow body outside a checkpoint: Kitaru wraps the call in a synthetic
  `llm_call` checkpoint so the call is still tracked and replayable

## Replay and control surfaces

Replay is one shared concept exposed through several surfaces.

### Replay entrypoints

- SDK: `flow.replay(...)`
- Client: `KitaruClient().executions.replay(...)`
- CLI: `kitaru executions replay ...`
- MCP: `kitaru_executions_replay`

### Replay selector rules

`from_` can point at a checkpoint selector or a wait selector. The exact replay
plan is built from stable names and IDs, so avoid vague naming.

Override keys must stay namespaced:

- `checkpoint.<selector>`
- `wait.<selector>`

Do not invent alternate replay APIs or made-up override keys.

## Operational surfaces: what exists where

Use the surface that matches the job instead of assuming everything is available
in every interface.

- **SDK**
  - author flows and checkpoints
  - use `wait`, `log`, `save`, `load`, `llm`
  - use `configure`, `connect`, `list_stacks`, `current_stack`, `use_stack`
  - use `KitaruClient` for programmatic execution and artifact control
- **KitaruClient**
  - `executions.get/list/latest/logs/input/retry/resume/replay/cancel`
  - `artifacts.list/get`
- **CLI**
  - `login`, `logout`, `run`, `status`, `info`
  - `log-store set/show/reset`
  - `stack list/current/use`
  - `model register/list`
  - `secrets set/show/list/delete`
  - `executions get/list/logs/input/replay/retry/resume/cancel`
- **MCP**
  - execution listing, lookup, latest, run, log retrieval, input, retry,
    replay, cancel
  - artifact listing/get
  - status and stack listing

Important asymmetries to remember:

- Artifact browsing exists in `KitaruClient` and MCP, not the CLI
- `resume` exists in `KitaruClient` and the CLI, not MCP
- Stack switching exists in SDK helpers and the CLI, not MCP
- `latest` exists in `KitaruClient` and MCP, not the CLI

## PydanticAI adapter

Public adapter surface:

- `kitaru.adapters.pydantic_ai.wrap(agent, *, name=None,
  tool_capture_config=None, tool_capture_config_by_name=None)`
- `kitaru.adapters.pydantic_ai.hitl_tool(*, question=None, name=None,
  schema=bool)`

What the adapter really does:

- Wrapped agents must already have a model bound at construction time
- Wraps agent/model/tool activity so it is tracked under Kitaru execution
  metadata
- Inside a checkpoint, child model/tool events become part of that checkpoint's
  trace
- At flow scope outside a checkpoint, `run()` and `run_sync()` use a synthetic
  `llm_call` checkpoint
- HITL tools bridge tool-time approvals back to flow-level `wait(...)`
- Tool capture modes are `full`, `metadata_only`, and `off`
- Per-tool capture overrides are supported
- MCP toolsets are supported and wrapped
- Deferred tool flows are not supported; prefer `hitl_tool(...)` or an explicit
  flow-level `wait(...)` instead

Safe default pattern: keep wrapped agent calls inside an explicit outer
checkpoint unless you have a reason not to. That gives you the clearest replay
boundary, especially when distributed execution is involved.

```python
from kitaru import checkpoint
import kitaru.adapters.pydantic_ai as kp

@checkpoint(type="agent_turn")
def ask_agent(agent, prompt: str) -> str:
    wrapped = kp.wrap(
        agent,
        tool_capture_config={"mode": "metadata_only", "enabled": True},
    )
    return wrapped.run_sync(prompt).output
```

## Connection and runtime context

Use Kitaru configuration helpers instead of inventing custom runtime wiring.

- `configure(...)` sets local execution defaults
- `connect(...)` / `kitaru login ...` connect to a server or managed workspace
- `list_stacks()`, `current_stack()`, `use_stack()` and `kitaru stack ...` help
  choose the active execution stack
- `model register/list` manage local model aliases used by `llm(...)`
- `secrets ...` manages secret values used by aliases and connected operations

## Common mistakes checklist

- Calling `my_flow(...)` directly instead of `my_flow.run(...)`
- Putting `wait()` inside a checkpoint
- Nesting checkpoint calls
- Returning non-serializable values from checkpoints
- Using vague or duplicate checkpoint / wait names that make replay selectors
  hard to target
- Reusing artifact names so `load()` becomes ambiguous
- Assuming CLI, client, and MCP expose the same operation set
- Reaching for deferred PydanticAI tools even though they are unsupported here
- Wrapping every tiny helper in a checkpoint instead of using meaningful replay
  boundaries
