# 13. Client API — `KitaruClient`

`KitaruClient` is the programmatic interface for inspecting and managing executions outside the flow body itself.

It is used by:

- dashboards
- internal tools
- tests
- CI/CD pipelines
- other services that need to inspect, replay, retry, or resume executions

The client API is not where durable orchestration happens. It is where **execution management** happens.

## What it does

The client should provide access to the following, listed in **priority order** (most-needed functions first):

1. **input / resume for waiting executions** — provide input to a paused flow
2. **replay from a durable call** — replay a step or checkpoint with optional overrides
3. **retry for failed executions** — same-execution recovery
4. **execution listing and lookup** — find and inspect executions
5. **status inspection** — current execution state
6. **pending wait inspection** — what the execution is waiting for
7. **latest-execution lookup** — find the most recent execution of a flow
8. **artifact browsing** — inspect artifacts (lower priority for MVP)

## Basic usage

```python
from kitaru import KitaruClient

client = KitaruClient()
```

The client should use the current resolved connection settings unless explicitly overridden.

## Executions API

### List executions

```python
execs = client.executions.list()
waiting = client.executions.list(status="waiting")
completed = client.executions.list(flow="content_pipeline", status="completed")
```

Typical filters may include:

- `status`
- `flow`
- `stack`
- time range
- tags or metadata later

### Get one execution

```python
ex = client.executions.get("kr-a8f3c2")
print(ex.status)
print(ex.flow_name)
print(ex.exec_id)
```

A retrieved execution should expose at least:

- execution ID
- flow name
- status
- start and end timestamps
- stack used
- summary metadata
- checkpoints or durable calls
- pending wait state, if any

## Execution status model

The public execution status model should be simple:

- `running`
- `waiting`
- `completed`
- `failed`
- `cancelled`

The execution **timeline** can contain richer detail underneath (failed attempts, retry segments, wait lifecycle events), but the top-level status should stay clean.

This gives the Temporal-like "one execution, visible gap/red segment" story without overcomplicating the public status taxonomy.

## Inspecting waits

If an execution is waiting, the client should expose the pending wait information.

```python
ex = client.executions.get("kr-a8f3c2")
print(ex.status)         # "waiting"
print(ex.pending_wait)   # schema/question/metadata for the active wait
```

Useful fields include:

- wait display name
- wait call ID
- question
- schema
- metadata
- active timeout status if set
- when the execution entered waiting

## Providing input to a waiting execution (resume)

The client should allow validated input to be supplied to a specific waiting execution. This is a **resume** operation — it continues the same execution.

```python
client.executions.input(
    "kr-a8f3c2",
    wait="approve_deploy",
    value=True,
)
```

For exact targeting, the runtime should resolve or accept a stable wait call instance ID internally, even if the public API accepts a name for convenience.

### Structured input example

```python
client.executions.input(
    "kr-a8f3c2",
    wait="review_draft",
    value={"approved": False, "notes": "Revise the intro"},
)
```

## Retry

Retry should be exposed as a client operation on a failed execution. This is a **same-execution** operation — it does not create a new execution.

```python
ex = client.executions.retry("kr-a8f3c2")
```

Or on the execution object:

```python
ex = client.executions.get("kr-a8f3c2")
ex.retry()
```

Retry semantics:

- same logical execution
- fixed code, fixed config, no user overrides
- reruns from the top, replaying prior durable outcomes
- re-executes from the failure point forward

## Replay

Replay should be exposed as a client operation that creates a **new execution**.

```python
ex = client.executions.get("kr-a8f3c2")
replayed = ex.replay(from_="write_draft")
```

Or equivalently:

```python
replayed = client.executions.replay(
    "kr-a8f3c2",
    from_="write_draft",
)
```

This should create a **new execution**, not mutate the old one.

### Replay with overrides

```python
replayed = client.executions.replay(
    "kr-a8f3c2",
    from_="approve",
    overrides={
        "wait.approve": False,
    },
)
```

Or with new flow inputs passed directly:

```python
replayed = ex.replay(
    from_="write_draft",
    topic="New topic",
    overrides={
        "checkpoint.research": "Edited notes",
    },
)
```

The exact shape may evolve, but the semantics should stay precise:

- replay creates a new execution
- overrides replace selected reused outcomes
- the original execution remains unchanged

## Conceptual distinction

```python
# Resume: same execution, provide wait input
client.executions.input(exec_id, wait="review", value=...)

# Retry: same execution, recover from failure
client.executions.retry(exec_id)

# Replay: new execution, optionally with new inputs or overrides
client.executions.replay(exec_id, from_="write_draft", topic="New topic")
```

These three operations map directly to the execution model's three distinct concepts.

## Cancel

A client may also cancel an execution if supported.

```python
ex.cancel()
```

Or:

```python
client.executions.cancel("kr-a8f3c2")
```

Cancellation semantics should be explicit in implementation, especially for:

- currently running executions
- waiting executions
- already completed executions

## Latest execution lookup

A common pattern is to start from the latest completed execution of a flow.

```python
prev = client.executions.latest(flow="content_pipeline", status="completed")
print(prev.exec_id)
```

This is useful for:

- building follow-up flows
- comparing runs
- starting replay from a recent baseline

## Artifact API

The client should allow browsing artifacts for an execution.

### List artifacts

```python
artifacts = client.artifacts.list("kr-a8f3c2")
```

Typical filters may include:

- artifact name
- artifact type
- producing call
- tags

### Get one artifact

```python
artifact = client.artifacts.get("art_abc123")
```

### Load an artifact value

```python
value = artifact.load()
```

This is the programmatic equivalent of inspecting an artifact in the dashboard.

## Example workflow

```python
from kitaru import KitaruClient

client = KitaruClient()

# Find the latest successful run
prev = client.executions.latest(flow="content_pipeline", status="completed")

# Replay from the draft step with a new input
new_ex = client.executions.replay(
    prev.exec_id,
    from_="write_draft",
    topic="AI observability",
)

# Inspect the new execution
current = client.executions.get(new_ex.exec_id)
print(current.status)
```

## Design notes

A good client API should be:

- explicit
- targetable
- scriptable
- stable around execution identity

That means public APIs can be name-friendly, but the implementation must still be exact about:

- execution IDs
- durable call instance IDs
- artifact IDs

## MVP notes

For the MVP, the client surface should be prioritized as follows:

### Tier 1 — core operations (build first)

- `executions.input(...)` — provide input to a waiting execution (resume)
- `executions.retry(...)` — retry a failed execution (same execution)
- `executions.replay(...)` — create a new execution from a previous one
- `executions.get(exec_id)` — inspect an execution

### Tier 2 — browsing and lookup

- `executions.list(...)` — list and filter executions
- `executions.latest(...)` — find the most recent execution of a flow

### Tier 3 — artifacts (later)

- `artifacts.list(exec_id)` — list artifacts for an execution
- `artifacts.get(artifact_id)` — get a specific artifact

The SDK should be built before the CLI (except `kitaru login`). The client API is the foundation that the CLI wraps.
