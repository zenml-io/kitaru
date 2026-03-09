# 5. `@flow` — The Outer Boundary

## What it does

Marks a function as a **durable execution** — the top-level unit Kitaru manages and tracks.

A flow is the boundary around a long-running piece of Python logic that may span multiple checkpointed steps, survive process restarts, suspend for input, and later resume or replay.

Under the hood, a Kitaru flow maps to a **dynamic ZenML pipeline**, but the user-facing model is simpler: a flow is just normal Python with durable boundaries inside it. The heavy lifting for durability, retry, and resume is implemented in the ZenML backend — Kitaru provides the simpler developer-facing model.

A flow is where Kitaru creates and owns the **execution record**: status, inputs, checkpoints, waits, artifacts, and replay history all live under that execution.

## Signature

```python
from kitaru import flow

@flow
def my_agent(prompt: str) -> str:
    ...

# Async may be supported, but sync is the primary path
@flow
async def my_async_agent(prompt: str) -> str:
    ...
```

## Behavior

- creates a tracked **execution** when invoked, with a unique execution ID
- runs plain Python code — `if`, `while`, `for`, `try/except`, helper functions, framework calls, and custom loops all work normally
- establishes the runtime context used by durable primitives inside it
- defines the boundary for **retry**, **resume**, and **replay**

## Execution model

A flow does **not** resume by restoring an in-memory Python frame or stack.

Instead, **retry**, **resume**, and **replay** all work by rerunning the flow function from the top, while durable calls inside it consult recorded execution history:

- checkpoints before the target point replay their recorded outcomes
- waits before the target point reuse their recorded inputs
- the target point and everything after it executes live again

## Invocation

There are three invocation patterns:

```python
# Synchronous — blocks until complete
# This is the ZenML-equivalent of calling my_pipeline() directly.
result = my_agent("Build a CLI tool")

# Start — returns a handle for a longer-running execution
# The @flow decorator returns an object with .start(), making this a
# simple extension. Kitaru needs this pattern for long-running executions
# where you want a handle to check status, provide wait input, etc.
handle = my_agent.start("Build a CLI tool")
print(handle.exec_id)
print(handle.status)
result = handle.wait()

# Deploy — starts an execution on a named stack
# Semantically identical to .start() with stack=, but communicates
# intent more clearly when targeting remote infrastructure.
handle = my_agent.deploy("Build a CLI tool", stack="aws-sandbox")
```

### Implementation note

The `my_flow.start()` and `my_flow.deploy()` patterns work because the `@flow` decorator returns a callable object that also exposes `.start()` and `.deploy()`. This is a small extension of the ZenML pattern (where you just call the function directly).

The alternative pattern `kitaru.start_flow(my_flow)` would require reworking how flows are resolved and imported across environments, so it should be avoided. Invocation should always go through the decorated function object itself.

### `.deploy()` vs `.start()` with `stack=`

`.deploy(...)` is sugar for `.start(..., stack=...)`. Both produce the same execution. `.deploy()` exists because it communicates intent more clearly in user-facing contexts — "deploy this agent on production infrastructure" reads better than "start with stack equals."

```python
# These are equivalent:
handle = my_agent.start("Build a CLI tool", stack="gcp-production")
handle = my_agent.deploy("Build a CLI tool", stack="gcp-production")
```

### Start with runtime overrides

```python
handle = my_agent.start("Build a CLI tool", stack="prod")
```

This lets the caller choose execution-time behavior (like stack selection) at invocation time without editing the decorator.

## What goes inside

Anything that should execute as one durable unit:

- raw SDK calls
- framework agent runs
- custom loops
- branching logic
- multiple checkpointed functions
- waits for human or external input

Kitaru does not impose a graph DSL. The flow body is plain Python. Durability comes from the boundaries inside it.

```python
from kitaru import flow

@flow
def content_pipeline(topic: str) -> str:
    research = research_step(topic)
    draft = write_step(research)
    review = kitaru.wait(schema=ReviewDecision, question="Review?")

    if not review.approved:
        draft = revise_step(draft, review.notes)

    return draft
```

## The flow as the main config surface

The `@flow` decorator is the primary place where configuration enters an execution. All execution-relevant settings — infrastructure, image, behavior — flow through it. Connection credentials (server URL, auth) are resolved separately before any flow runs; they do not belong in the decorator.

See [Chapter 4](04-connection-stacks-and-configuration.md) for the full unified configuration model.

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `stack` | `str` | `None` | Named stack to use for this execution |
| `image` | `ImageSettings` | `None` | Docker image and environment settings for remote execution |
| `cache` | `bool` | `True` | Whether checkpoint outputs can be reused from previous executions |
| `retries` | `int` | `0` | Number of automatic retries on unhandled failure |

## Flow retry semantics

Flow retries are **same-execution** recovery. They do **not** create a new execution.

When a flow retry triggers:

1. the failure is recorded on the existing execution timeline
2. the flow reruns from the top
3. previously completed checkpoints replay their recorded outcomes
4. the failed region and everything after it executes live again
5. the execution timeline shows both the failed attempt and the retry continuation

Flow retries are usually cheap because most work before the failure point is replayed from durable history.

Flow retries are **not** replay. They use fixed code, fixed config, and no user overrides. If the user wants to change code/config/inputs, that is a replay (which creates a new execution).

## Rules

- sync is the primary and recommended path for MVP
- must be the **outermost** Kitaru boundary — flows cannot be nested as one execution
- can contain checkpoints, waits, logs, LLM calls, and framework code
- can start other flows, but those are separate executions
- owns the execution journal and replay cursor for everything inside it

## Concurrency

Kitaru does not have a dedicated `parallel` primitive. Instead, concurrency uses the **`.submit()` + `.result()`** pattern (ZenML futures).

```python
from kitaru import flow, checkpoint

@checkpoint
def research(topic: str) -> str:
    ...

@checkpoint
def gather_data(topic: str) -> dict:
    ...

@flow
def parallel_research(topic: str) -> str:
    # Submit both checkpoints — they run concurrently
    research_future = research.submit(topic)
    data_future = gather_data.submit(topic)

    # Collect results — blocks until both complete
    notes = research_future.result()
    data = data_future.result()

    return combine(notes, data)
```

### How it works

- `.submit()` kicks off the checkpoint and returns a future immediately
- `.result()` blocks until that checkpoint completes and returns its output
- multiple `.submit()` calls run concurrently
- the order of `.result()` calls does not matter — both futures resolve independently

### What concurrency applies to

Concurrency applies to **checkpoints within a flow**. Flows themselves cannot run concurrently within a single execution — starting another flow creates a separate execution.

### Dashboard representation

When multiple checkpoints are submitted concurrently, the dashboard should show them as parallel branches in the execution timeline, with a join point where results are collected.

## Control flow stays normal Python

Because replay works by rerunning from the top and replaying recorded outcomes at durable boundaries, Python control flow behaves naturally across resume and replay.

That includes:

- `try/except`
- loops
- branching
- helper functions
- local variable recomputation between checkpoints

## ZenML defaults

Kitaru sets two underlying ZenML defaults that should always be active:

- **Dynamic pipeline mode is on by default.** Every Kitaru flow maps to a ZenML dynamic pipeline. This is not user-configurable — Kitaru requires dynamic mode for its execution model to work.
- **Cache is on by default.** Checkpoint outputs are reused from previous executions unless explicitly disabled with `cache=False`. This is the safe default — most agent workflows benefit from not re-executing expensive checkpoints unnecessarily.

## Notes for MVP

- direct composition via `other_flow.start()` is cleaner than pretending nested flows are a single execution
- background durability after process exit depends on connected or server-backed mode (Pro), not local inline execution alone
- automatic scheduler-driven retries (e.g. a background process that retries failed flows) are desirable but may be scoped as future work beyond manual retry for the MVP
- flow retry behavior is backed by ZenML step/pipeline retry machinery
