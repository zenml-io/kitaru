# 11. Per-Flow and Per-Checkpoint Overrides

Overrides are runtime-level controls that let you tune how a flow or checkpoint executes.

Kitaru uses a [unified config object](04-connection-stacks-and-configuration.md) that gathers all settings into one structure. Within that object, overrides are the **runtime layer** — per-decorator or per-invocation choices that override the pre-execution defaults.

Overrides are distinct from:

- **connection settings** — resolved before any flow runs
- **pre-execution settings** — stack selection, image config, project defaults set before the run
- **replay overrides** — replacing historical outcomes during replay

This section is about **execution-time behavior** on flows, checkpoints, and individual calls.

## Why these overrides exist

Some choices belong close to the code that needs them.

For example:

- a flaky API checkpoint may need retries
- one flow may need to target `prod`
- one LLM call may need `fast` instead of the default model

These are not project-wide truths. They are execution-level decisions.

## Flow-level overrides

Flow-level overrides apply to the whole execution.

Typical examples:

- `stack`
- `image`
- `cache`
- `retries`

### Example

```python
@kitaru.flow(
    stack="prod",
    image=ImageSettings(
        base_image="python:3.12-slim",
        requirements=["pydantic"],
    ),
    cache=False,
    retries=2,
)
def heavy_flow(data: str) -> str:
    ...
```

### What they control

- which stack the execution should use
- what Docker image and environment to use for remote execution
- whether checkpoint outputs can be reused from previous executions
- whether the flow should retry after uncaught failure (same-execution retry, not replay)

### What they should not control

Flow-level overrides should not silently redefine:

- project model aliases
- unrelated checkpoint behavior
- global connection state

## Checkpoint-level overrides

Checkpoint-level overrides apply to that checkpoint boundary only.

Typical examples:

- `retries`
- visualization `type`

### Example

```python
@kitaru.checkpoint(
    retries=3,
    type="tool_call",
)
def fetch_external_data(query: str) -> dict:
    ...
```

### What they control

- how many times that checkpoint may be retried (same-execution, local retries)
- how it is labeled in the dashboard

### What they should not control

Checkpoint-level overrides should not secretly switch:

- the enclosing execution's stack
- unrelated flow defaults
- global model routing rules

## Call-level overrides

Some overrides belong at the call site rather than the decorator.

The best example is `kitaru.llm()`:

```python
outline = kitaru.llm("Create an outline", model="fast")
draft = kitaru.llm("Write the article", model="smart")
```

Here, `model=` is a call-level override.

It should resolve against the frozen execution spec, but the choice itself belongs to that individual call.

## Invocation-time overrides

A flow may also be started with explicit runtime overrides.

For example:

```python
handle = my_flow.start(task="demo", stack="local")
```

This lets the caller choose execution-time behavior without editing the decorator.

## Precedence

Execution-time overrides should follow a simple rule:

- more specific beats less specific

In practice:

- invocation-time flow overrides beat flow decorator defaults
- checkpoint decorator overrides beat flow-level execution defaults for that checkpoint
- explicit call-time arguments beat default app config

Examples:

### Flow stack precedence

1. `my_flow.start(..., stack="prod")` — invocation-time override
2. `@kitaru.flow(stack="prod")` — decorator default
3. selected active stack
4. implicit `local`

### LLM model precedence

1. `kitaru.llm(..., model="fast")`
2. configured `default_model`

### Retry precedence

1. explicit checkpoint decorator values
2. explicit flow decorator values where applicable
3. framework defaults

## Flow retries vs checkpoint retries

These are related, but not the same.

### Checkpoint retries

Checkpoint retries re-execute the checkpoint boundary before failure propagates to the flow.

They are **same-execution, local** retries that behave like ZenML step retries.

### Flow retries

Flow retries rerun the flow from the top, replaying earlier durable outcomes and re-executing from the failed point forward.

They are also **same-execution** — a flow retry does not create a new user-visible execution.

This means:

- checkpoint retries are narrow and local
- flow retries are broader, but still cheap when prior durable outcomes can be replayed
- neither creates a new execution (that would be replay)

## Stack override scope

For MVP, stack choice should be **flow-scoped**, not checkpoint-scoped.

That means:

- one flow runs on one resolved stack
- checkpoints inside it do not secretly jump to a different compute target

If per-checkpoint compute selection comes later, it should be introduced as a separate feature rather than implied now.

## Rules

- overrides should be execution-time controls, not hidden global config
- more specific settings should beat less specific settings
- flow-level infra selection should stay flow-scoped in MVP
- call-time overrides should resolve against the frozen execution spec
- checkpoint retries and flow retries are both same-execution operations, distinct from replay

## MVP notes

For March, the most useful override surface is:

- flow: `stack`, `image`, `cache`, `retries`
- checkpoint: `retries`, `type`
- `kitaru.llm()`: `model`, plus normal per-call LLM parameters

These are all part of the [unified config object](04-connection-stacks-and-configuration.md) — overrides are just the runtime layer that sits on top of pre-execution defaults.
