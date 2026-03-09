---
name: kitaru-authoring
description: >
  Guide for writing Kitaru durable agent workflows. Use when creating or
  refactoring Kitaru flows, checkpoints, waits, logging, artifacts, LLM calls,
  or PydanticAI adapter usage. Triggers on mentions of kitaru, @kitaru.flow,
  @kitaru.checkpoint, kitaru.wait, kitaru.log, kitaru.save, kitaru.load,
  kitaru.llm, or durable execution patterns.
---

# Kitaru Authoring Skill

Use this guide when writing or refactoring Kitaru workflows.

> **Before building**: If the user hasn't validated their workflow design yet, suggest
> using the `kitaru-scoping` skill first to assess fit and define checkpoint/wait
> boundaries. Scoping produces a `flow_architecture.md` that feeds directly into
> authoring.

## Mental model

- `@kitaru.flow` is the **durable outer boundary**.
- `@kitaru.checkpoint` is a **replayable work unit** inside the flow.
- `kitaru.wait()` pauses at the flow level and resumes with user/system input.

```python
import kitaru

@kitaru.checkpoint
def draft(topic: str) -> str:
    kitaru.log(phase="draft")
    return f"Draft for {topic}"

@kitaru.flow
def review_flow(topic: str) -> str:
    text = draft(topic)
    approved = kitaru.wait(name="approve_draft", question="Approve draft?", schema=bool)
    if not approved:
        return "Rejected"
    return text
```

## Rules to enforce

1. Do not nest flows (`@kitaru.flow` inside another flow).
2. Do not call `kitaru.wait()` inside a checkpoint.
3. Checkpoint return values must be serializable (JSON-friendly or Pydantic models).
4. Wrap meaningful work in checkpoints so replay boundaries are explicit.

## Core primitives to use

- `kitaru.log()` for structured metadata (with key/value fields).
- `kitaru.save(name, value, type="...")` and `kitaru.load(exec_id, name)` for explicit artifacts.
- `kitaru.llm(...)` for tracked model calls.
- `.submit()` and `.result()` for concurrent checkpoint work.

## PydanticAI adapter pattern

```python
import kitaru
import kitaru.adapters.pydantic_ai as kp

@kitaru.checkpoint
def ask_agent(agent, prompt: str) -> str:
    wrapped = kp.wrap(agent)
    return wrapped.run_sync(prompt).output
```

Use `@kitaru.adapters.pydantic_ai.hitl_tool(...)` when agent tools should trigger flow-level waits.

## Connection and runtime context

- Use `kitaru.configure(...)` for defaults.
- Use `kitaru login ...` for connected mode.
- Use `kitaru stack list/current/use` to inspect/select default runtime stack.

## Common mistakes checklist

- Wait inside checkpoint? Move wait into flow body.
- Non-serializable checkpoint output? Convert to JSON-compatible structure.
- Missing checkpoints around expensive/tool work? Add replay boundaries.
- Nested flow decorators? Flatten to one flow boundary.
