# 10. Replay and Overrides

Replay is one of the core reasons Kitaru exists.

It is the mechanism that lets you take a previous execution, reuse what already happened, change one part, and re-run the rest deterministically.

## Replay vs retry vs resume

These are different operations. This section covers **replay only**.

| Operation | Execution identity | Code/config changes | User overrides |
| --- | --- | --- | --- |
| **Retry** | Same execution | No | No |
| **Resume** | Same execution | No | Only the pending wait input |
| **Replay** | **New execution** | Yes, optionally | Yes |

**Only replay creates a new user-visible execution.** Retry and resume are covered in the execution model (section 2) and error handling (section 12).

## What replay means

Replay creates a **new execution** based on a previous one.

It does **not** mutate the old execution.

It does **not** resume the old execution in place.

Instead, replay means:

- start a new execution
- rerun the flow from the top
- reuse recorded outcomes before the replay point
- execute live at and after the replay point
- optionally inject overrides to change the downstream path

## Replay model

Given an original execution:

```python
@kitaru.flow
def content_pipeline(topic: str) -> str:
    notes = research(topic)
    draft = write_draft(notes)
    approved = kitaru.wait(schema=bool, name="approve")
    if not approved:
        draft = revise(draft)
    return publish(draft)
```

A replay from `write_draft` works conceptually like this:

1. rerun the flow from the top
2. `research(...)` replays its recorded outcome
3. `write_draft(...)` executes live again
4. everything after it executes normally, unless later durable outcomes are also reused by the replay plan

## What gets reused

Before the replay point, Kitaru should reuse recorded durable outcomes:

- checkpoint return values
- checkpoint exceptions
- wait inputs
- synthetic standalone LLM call outcomes, if treated as top-level durable calls

This is outcome replay, not just output caching.

## What gets re-executed

At and after the replay point, Kitaru may execute live again:

- checkpoints
- waits
- LLM calls inside those live checkpoints
- downstream side effects

This is why replay and side effects must be treated carefully.

## Why replay reruns from the top

Replay reruns from the top because Kitaru does not restore Python frames or stack state.

Instead:

- normal Python code reruns naturally
- durable boundaries consult history
- local variables recompute between those boundaries

That makes replay compatible with ordinary Python control flow without needing a custom interpreter.

## Replay point targeting

Users often think in terms of names:

- `write_draft`
- `wait:approve`
- `research`

That is useful for CLI and dashboard ergonomics.

But internally, replay should target a **specific durable call instance**, not just a label.

This matters for:

- loops
- repeated checkpoint names
- repeated waits
- branched executions

So the real model is:

- user-facing APIs may accept names
- the runtime resolves that to a specific recorded call instance ID

## Examples

### Replay from a checkpoint

```python
ex.replay(from_="write_draft")
```

Semantically:

- reuse everything before `write_draft`
- execute `write_draft` live
- execute everything after it live unless otherwise specified

### Replay with new flow inputs

Flow inputs can be passed directly as keyword arguments. This is the most common replay pattern — re-run with different inputs while reusing cached checkpoint outcomes.

```python
# Replay the same execution with a different topic
content_pipeline.replay(exec_id="kr-a8f3c2", topic="New topic")

# Replay a coding agent with a different issue
coding_agent.replay(exec_id="kr-b7e4d1", issue="Fix login bug")
```

When flow inputs are provided, they replace the original execution's inputs. Checkpoints before the replay point still return cached outcomes (unless explicitly overridden).

### Replay from a wait with new input

```python
ex.replay(from_="approve", override_input=True)
```

Semantically:

- reuse everything before the wait
- do not reuse the historical wait input
- require or inject a new input
- continue execution from there

## Local replay with overrides

A key MVP feature is **local replay with overrides**.

This means a developer can take a previous execution and replay it locally while changing selected values.

Typical use cases:

- edit a prompt
- swap out a checkpoint output
- provide a different review decision
- tweak a flow input without re-running everything from scratch

## Override targets

Overrides should be explicit and narrowly scoped.

The most useful override targets are:

- **flow input**
- **checkpoint outcome**
- **wait input**

Conceptually, a replay creates a new execution with:

- a base execution
- a replay start point
- an override map

Example conceptually:

```python
my_flow.replay(
    exec_id="kr-a8f3c2",
    from_="write_draft",
    overrides={
        "flow.input.topic": "New topic",
        "checkpoint.research": "Edited research notes",
        "wait.approve": False,
    },
)
```

The exact API may differ, but the model should be this precise.

## Override semantics

Overrides should behave like **synthetic replayed outcomes**.

That means:

- they do not mutate the original execution
- they replace the historical value used during replay
- downstream code sees them as if they were the recorded outcome
- lineage should show that the replay used an override rather than the original outcome

## Artifact overrides vs call overrides

The SDK often talks about "artifact overrides," but the runtime concept is usually a little broader.

An override may target:

- the input to a flow
- the outcome of a checkpoint
- the input returned by a wait
- possibly a named artifact produced by a checkpoint

For clarity, the runtime should treat overrides primarily as **durable call outcome overrides**, even if the UI exposes them through artifact names.

## Replay and divergence detection

Replay should only reuse historical outcomes when the durable call sequence still matches.

If code changes before the replay point alter the sequence of checkpoints or waits, Kitaru should raise a divergence error.

This prevents silent corruption such as:

- returning the wrong cached value to the wrong call site
- applying an old human input to a different wait
- skipping a newly inserted checkpoint unintentionally

Divergence detection should be treated as a hard rule, not an optional idea.

## Replay and code version

Replay and resume are different here too.

### Resume

Resume should use the original frozen execution spec of the waiting execution.

### Retry

Retry should use the original frozen execution spec of the failed execution.

### Replay

Replay creates a new execution and therefore uses a new execution spec unless explicitly told to inherit from the old one.

In practice, this means:

- local replay usually uses current local code and current local settings
- server resume uses the original execution's stored context
- retry uses the original execution's stored context

This distinction should be explicit so users understand why replay, resume, and retry may behave differently.

## Replay and side effects

Replay is powerful, but it can re-trigger side effects after the replay point.

Examples:

- creating a second PR
- sending a second email
- deploying again
- writing duplicate records to an external system

So replay should be used carefully with side-effecting checkpoints, and those checkpoints should be designed for idempotency.

## Rules

- replay creates a new execution
- replay reuses durable outcomes before the replay point
- replay executes live at and after the replay point
- overrides replace reused outcomes without mutating the source execution
- replay must validate durable call sequence compatibility before reusing history
- retry is not a replay subtype — they are fundamentally different operations

## MVP notes

For MVP, replay should stay focused on:

- local replay
- replay from a specific checkpoint or wait
- explicit overrides for the most useful targets
- deterministic reuse of prior outcomes

The core goal is not a giant replay surface. It is a reliable, inspectable rerun mechanism that developers can actually trust.
