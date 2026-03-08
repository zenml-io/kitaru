# 12. Error Handling

Error handling in Kitaru should preserve normal Python behavior while remaining compatible with durable replay.

The key principle is:

- failures at durable boundaries are recorded
- retries happen at well-defined levels
- replay reuses recorded failure outcomes when appropriate
- ordinary `try/except` should continue to work naturally

## Failure model

A durable call can end in different kinds of outcomes, including:

- success with a value
- failure with an exception
- waiting for input
- cancellation

For the MVP, the most important thing is that **successes and failures are both journaled**.

That is what makes replay deterministic.

## Checkpoint failure

When a checkpoint raises an unhandled exception:

1. Kitaru records the failure outcome for that checkpoint call
2. if checkpoint retries are configured, Kitaru may retry it (same-execution, local retry)
3. if retries are exhausted, the exception propagates back to the flow
4. the flow may catch it with normal Python `try/except`
5. if uncaught, the flow may fail and flow-level retry logic may apply

## Example: checkpoint failure handled in flow

```python
@kitaru.checkpoint
def risky_step(data: str) -> str:
    raise ValueError("bad input")

@kitaru.flow
def my_agent(task: str) -> str:
    try:
        return risky_step(task)
    except ValueError:
        return "recovered"
```

This should behave normally on first run and on replay, because the failure outcome is recorded and can be replayed.

## Checkpoint retries

Checkpoint retries are the narrowest retry scope. They are **same-execution, local** retries that use ZenML step retry behavior underneath.

```python
@kitaru.checkpoint(retries=3)
def flaky_api_call(query: str) -> dict:
    return external_api.search(query)
```

Semantics:

- Kitaru may re-execute the checkpoint up to the configured retry count
- retries happen before the failure propagates to the flow
- retries do not create a new user-visible execution
- the final outcome of the checkpoint is what later replay uses
- failed attempts should be recorded in the execution timeline

For MVP, the most important guarantee is that failures are not silently discarded.

## Flow failure

If a failure escapes the flow body:

- the execution is marked failed
- optional flow retry may apply (same-execution retry, not a new execution)
- the execution timeline shows the failure and any retry attempts

Because flow retries rerun from the top, previously completed checkpoints are replayed from history and the failed area can run again.

## Flow retries

```python
@kitaru.flow(retries=2)
def my_flow(task: str) -> str:
    data = fetch_data(task)
    return process_data(data)
```

Flow retry semantics:

- same logical execution continues
- rerun the flow from the top
- replay earlier durable outcomes where possible
- re-execute from the failure point forward
- the timeline shows both the failed attempt and the retry continuation

Flow retries are **not** replay. They use fixed code, fixed config, and no user overrides. The user sees one execution with a gap or red segment, not two separate executions.

## Replay of failures

Replay must preserve failure outcomes, not just values.

That means if a prior checkpoint failed and the flow took a fallback path, replay should re-raise that recorded exception before the replay point rather than quietly re-running the checkpoint and possibly taking a different path.

This is essential for:

- deterministic replay
- correct `try/except` behavior
- trust in historical debugging

## Wait validation failure

If input provided to a wait does not satisfy the declared schema:

- the input should be rejected
- the execution should remain in `waiting`
- validation errors should be returned to the caller

This is not a runtime failure of the execution — it is rejected input while the execution remains waiting. The execution does not fail or transition to an error state because of bad input.

This is especially important for API, CLI, and webhook resume paths.

## Wait timeout behavior

Wait timeout means **active resource retention**, not total expiration.

When an active wait timeout is reached:

- compute/resources are released
- the execution remains in `waiting` state
- the wait does **not** expire or fail
- the execution can still be resumed later with valid input

There is no default "wait timed out and the execution fails" behavior. If a product later needs business-level expiration ("approval window closed after 24h"), that should be a separate concept, not overloading the active wait timeout.

## Divergence errors

Replay divergence is its own class of failure. Divergence detection is implemented in the ZenML backend — Kitaru exposes the user-visible error.

If the durable call sequence no longer matches the historical execution before the replay point, replay fails with a clear divergence error rather than silently reusing the wrong outcomes.

Typical causes:

- inserting a checkpoint before the replay point
- removing a wait before the replay point
- changing loop structure so durable call ordering changes

This should be treated as a first-class runtime error.

## Serialization and replay errors

Some failures are about the runtime itself rather than user code.

Examples:

- checkpoint return value cannot be serialized
- stored exception cannot be fully reconstructed
- artifact cannot be loaded
- override value is invalid for the replay target

These should surface as clear runtime errors rather than being confused with ordinary user-code exceptions.

## Logging errors

Because `kitaru.log()` is context-sensitive, it can be used both:

- inside a checkpoint to annotate the failing durable call
- inside the flow to annotate execution-level recovery or fallback paths

Example:

```python
@kitaru.flow
def resilient_agent(task: str) -> str:
    try:
        return primary_model(task)
    except Exception as e:
        kitaru.log(error=str(e), recovery="fallback")
        return fallback_model(task)
```

## Rules

- failures at durable boundaries should be recorded
- replay should preserve recorded failure outcomes
- checkpoint retries and flow retries are both same-execution operations, distinct from replay
- invalid wait input should not break waiting executions
- wait timeout releases resources but does not fail the execution
- divergence should be explicit and fail loudly
- runtime/serialization errors should be separated from user-code failures where possible

## MVP notes

For March, the essential error-handling contract is:

- journal both success and failure
- make replay preserve control-flow-relevant failures
- keep wait validation explicit
- keep wait timeout as resource retention only

**Retry status (March 2026):** A ZenML CLI command to retry failed runs exists on the `feature/pause-pipeline-runs` branch, but **does not work yet**. Kitaru should stub `client.executions.retry(...)` until the upstream fix lands. Do not attempt to reimplement retry independently.
