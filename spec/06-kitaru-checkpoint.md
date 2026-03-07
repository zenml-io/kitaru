# 6. `@kitaru.checkpoint` — Durable Unit of Work

## What it does

Marks a function as a **durable checkpoint boundary** inside a flow.

Under the hood, a checkpoint maps to a **ZenML step**, but the contract is stronger than "a step whose output is cached."

A checkpoint is the unit Kitaru uses for:

- persistence
- replay
- retries
- dashboard timeline rendering
- artifact production

Each checkpoint invocation is recorded in execution history with its **outcome**, not just its output.

## Signature

```python
@kitaru.checkpoint
def my_step(input: str) -> str:
    ...

@kitaru.checkpoint(
    retries=3,
    type="llm_call",
)
def heavy_step(data: str) -> str:
    ...
```

## Behavior

- executes as a ZenML step under the hood
- on success, its return value is automatically persisted to the artifact store
- inside the same process, returns a normal Python value — users do not manually `.load()` artifacts
- on replay, prior checkpoints do **not** execute user code again; instead, they replay their recorded outcome
- appears as a checkpoint entry in the execution timeline on the dashboard

## Replay semantics: outcomes, not just outputs

This is the key rule.

On replay, a checkpoint before the replay point replays its **recorded outcome**:

- if it previously returned a value, return that value
- if it previously raised an exception, re-raise that exception

This preserves normal Python behavior across replay.

Example:

```python
@kitaru.checkpoint
def flaky() -> str:
    raise ValueError("bad input")

@kitaru.flow
def my_flow() -> str:
    try:
        flaky()
    except ValueError:
        return "recovered"
```

If replay only cached outputs, this flow could not replay correctly. Replaying the recorded exception preserves the same `try/except` path.

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `retries` | `int` | `0` | Number of automatic retries on failure |
| `type` | `str` | `None` | Visualization hint — `'llm_call'`, `'tool_call'`, `'human'`, or any custom string |

## Config hierarchy

Checkpoint-level execution settings override flow-level execution settings.

These are execution-time settings, not global config.

## Identity and replay matching

Checkpoint names are useful for humans, but they are **not enough** for replay identity.

In particular, loops can call the same checkpoint multiple times:

```python
for topic in topics:
    write_draft(topic)
```

So each checkpoint invocation should be treated as a distinct durable call with:

- a unique call ID
- a sequence position in the execution
- a human-facing name

Replay should match checkpoint calls by execution order and durable identity, not just by function name.

## Examples

**Simple checkpoint:**

```python
@kitaru.checkpoint
def summarize(text: str) -> str:
    client = openai.OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": f"Summarize: {text}"}],
    )
    return response.choices[0].message.content
```

**Typed checkpoint (PydanticAI agent):**

```python
@kitaru.checkpoint(type="llm_call")
def research(topic: str) -> str:
    agent = Agent("openai:gpt-4o", name="researcher", tools=[search_web])
    result = agent.run_sync(f"Research {topic} thoroughly")
    return result.output
```

**Checkpoint with retries:**

```python
@kitaru.checkpoint(retries=3)
def generate_embeddings(texts: list[str]) -> list[list[float]]:
    model = SentenceTransformer("all-MiniLM-L6-v2")
    return model.encode(texts).tolist()
```

## Retry semantics

Checkpoint retries are **same-execution, local retries** at the checkpoint boundary. They use ZenML step retry behavior underneath — Kitaru exposes and configures this, but the retry machinery itself is ZenML's.

If a checkpoint has `retries=3`:

- Kitaru may re-execute that checkpoint up to 3 additional times
- retries happen before the failure propagates back to the flow
- retries do **not** create a new user-visible execution
- the final recorded outcome is what replay uses later

For MVP, the most important thing is that failures are journaled, not discarded. Retry history can be exposed more richly in the UI later if needed.

## Restrictions

- **`wait()` inside a checkpoint is unsupported in the MVP.** Waits are valid only directly in flow execution.
- **Nested checkpoint-within-checkpoint semantics are unsupported in the MVP.** Each checkpoint is an independent durable boundary.
- **`kitaru.llm()` inside a checkpoint is a child event**, not a nested replay boundary.

## Artifacts

A successful checkpoint produces an implicit output artifact:

- named after the function by default
- persisted automatically
- linked to the checkpoint call that produced it
- shown in the execution timeline and artifacts view

A failed checkpoint still produces a recorded call outcome, even though it may not produce an output artifact.

That is an important distinction:

- **artifacts** are persisted values
- **call records** are durable execution history

Kitaru needs both.

## Parallel execution

Checkpoints support concurrent execution via `.submit()`:

```python
future = my_checkpoint.submit(arg1, arg2)
result = future.result()
```

This returns a future that resolves when the checkpoint completes. Multiple checkpoints can be submitted concurrently — see section 5 for the full concurrency pattern.

On replay, concurrently submitted checkpoints replay their recorded outcomes just like sequential ones. The replay model does not change for concurrent execution.

## Rules

- sync is the primary and recommended path for MVP
- must be called inside a `@kitaru.flow`
- should have a return type annotation
- the return value becomes an artifact on success
- exceptions are also recorded and replayed
- repeated calls inside loops are distinct durable calls
- on replay, checkpoints before the replay point do not execute again

## Side effects and determinism

A checkpoint is a durable boundary, not a magical idempotency wrapper.

That means:

- if a checkpoint is replayed from history, its side effects do **not** happen again
- if a checkpoint is re-executed live at or after the replay point, its side effects may happen again

So checkpoints that call external systems should either be naturally idempotent or designed with replay in mind.

## Notes for MVP

- exact exception rehydration may be best-effort, but replay should preserve control-flow-relevant failure behavior
