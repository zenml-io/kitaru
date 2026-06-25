---
description: Understand Kitaru exception types and failure journaling
icon: triangle-exclamation
---

# Error Handling

Because every model call and tool call in a flow is recorded as a durable
checkpoint, failures are journaled, not lost — you get the failing checkpoint,
its retry attempts, and a typed exception you can branch on. Kitaru's exception
hierarchy distinguishes usage, context, state, runtime, backend, and execution
failures so you can react precisely and replay a failed run from the last good
checkpoint.

## Core exception types

```python
import kitaru

try:
    result = my_flow.run(...).wait()
except kitaru.KitaruUserCodeError as exc:
    # user checkpoint/flow code raised
    print(exc.exec_id, exc.status, exc.failure_origin)
except kitaru.KitaruDivergenceError:
    # replay divergence surfaced from backend contract
    ...
except kitaru.KitaruExecutionError:
    # other execution-level failure
    ...
```

## Wait-input validation failures

`client.executions.input(...)` raises `kitaru.KitaruWaitValidationError`
when supplied input does not satisfy the wait schema.

```python
try:
    client.executions.input(exec_id, wait="approve_deploy", value="yes")
except kitaru.KitaruWaitValidationError as exc:
    print(exc)
```

When validation fails, the execution remains in `waiting`.

## Failure journaling in the client

`KitaruClient` surfaces structured failure details:

- `execution.failure`: failure summary for failed executions
- `checkpoint.failure`: final checkpoint failure (if terminal attempt failed)
- `checkpoint.attempts`: full retry attempt history, including failed attempts

```python
client = kitaru.KitaruClient()
execution = client.executions.get(exec_id)

if execution.failure:
    print(execution.failure.origin, execution.failure.message)

for checkpoint in execution.checkpoints:
    for attempt in checkpoint.attempts:
        print(attempt.attempt_id, attempt.status)
        if attempt.failure:
            print("  ", attempt.failure.exception_type, attempt.failure.message)
```

## Replay divergence behavior

Replay re-executes a recorded run from a checkpoint. If the new run can't follow
the recorded durable call sequence — for example, the code changed in a way that
breaks the checkpoint contract — Kitaru raises `kitaru.KitaruDivergenceError`
instead of silently producing an unfaithful baseline.

`client.executions.replay(...)` may fail immediately with this error when the
backend detects an incompatible call sequence at submission time. Even when
submission succeeds, divergence can surface later on the replayed execution as
normal failure metadata:

```python
replayed = client.executions.replay(exec_id, from_="write_draft")
latest = replayed.refresh()

if latest.failure and latest.failure.origin == kitaru.FailureOrigin.DIVERGENCE:
    print("Replay divergence:", latest.failure.message)
```
