# 2. Execution Model

This is the most important section in the SDK.

Everything else in Kitaru depends on this model.

## Durable execution by rerun, not frame restore

Kitaru does **not** snapshot and restore Python stack frames in the MVP, and that is not the product direction.

Instead, Kitaru records the outcomes of durable calls and continues by **rerunning the flow from the top**, reusing recorded outcomes until the point that should execute live again.

This is the model behind:

- retry
- resume after `wait()`
- replay with optional overrides
- local debugging with overrides

## Retry, resume, and replay

These are three distinct operations. They should never be conflated.

| Operation | Execution identity | Code/config changes | User overrides | Typical trigger |
| --- | --- | --- | --- | --- |
| **Retry** | Same execution | No | No | Automatic or manual recovery after failure |
| **Resume** | Same execution | No | Only the pending wait input | Wait input arrives |
| **Replay** | **New execution** | Yes, optionally | Yes | Explicit developer action |

**Only replay creates a new user-visible execution.**

Retry and resume both continue the **same logical execution**, even if the backend may create new internal run attempts underneath. The user should see one execution timeline — potentially with failed-attempt segments, retry gaps, and resumed continuations — rather than separate execution records.

### Retry

Retry is same-execution recovery after failure.

- Fixed code, fixed config, no user overrides
- The flow reruns from the top
- Prior durable outcomes are reused where valid
- The failed region executes again
- The execution timeline shows both the failed attempt and the retry continuation under the same execution
- May happen automatically (configured retries) or be triggered manually

Retry must **not** allow user-injected code/config/input changes. If the user wants to change those, that is a replay.

### Resume

Resume is same-execution continuation after `wait()` input arrives.

- Wait input is recorded on the existing execution
- The flow reruns from the top
- Prior durable outcomes are replayed
- `wait()` returns the recorded input at the wait site
- Execution continues from there

Resume does **not** create a new execution. The wait input binds to the existing execution and the pending wait call instance.

### Replay

Replay creates a **new execution** derived from an earlier one.

- The old execution remains unchanged
- The new execution may use new local code, config, inputs, or overrides
- Before the replay point, historical outcomes may be reused unless overridden
- At and after the replay point, execution runs live again
- Lineage records the replay source and any overrides applied

Because replay may involve changed code and config, it **must** be a new execution.

## Durable calls replay outcomes, not only outputs

Before the replay point, durable calls replay their prior **outcome**:

- a checkpoint that returned a value returns that value
- a checkpoint that raised an exception re-raises that exception
- a wait that received input returns that input

This preserves normal Python control flow across replay, including:

- `try/except`
- loops
- branching
- local recomputation between checkpoints

## Durable boundaries

The core durable boundaries are:

- `@kitaru.flow` — execution boundary
- `@kitaru.checkpoint` — replayable work boundary
- `kitaru.wait()` — suspension boundary

`kitaru.llm()` is a convenience wrapper whose behavior depends on where it is called:

- inside a flow, it may synthesize a checkpoint-like durable call
- inside an existing checkpoint, it behaves as a child event and artifact producer rather than a nested replay boundary

## MVP durable-boundary restrictions

For the MVP, these restrictions apply:

- **Flows are the outermost durable execution boundary.** Nested flows are separate executions, not nested same-execution structures.
- **Checkpoints are durable work boundaries inside flows.** Nested checkpoint-within-checkpoint semantics are unsupported.
- **`wait()` is valid only directly in flow execution, not inside a checkpoint.**
- **`kitaru.llm()` inside a checkpoint is a child event**, not a nested replay boundary.
- **Adapters do not bypass these restrictions.** Framework adapters must not create alternate nested durable semantics.

## Durable identity

Every durable call in an execution has:

- a **display name** for humans
- a **stable call instance ID** for APIs and exact targeting
- a **sequence position** in execution history for replay matching

Names are for dashboards and developer ergonomics. IDs and sequence position are what make replay reliable, especially in loops and branches.

## Divergence detection [TBD]

On replay, Kitaru should verify that the durable call sequence still matches the original execution up to the replay point.

If code changes insert, remove, or reorder durable calls before the replay point, replay should fail with a clear divergence error rather than silently returning the wrong historical values.

This matters especially for:

- loops
- conditionals
- changed helper structure
- reordered checkpoints or waits

## Snapshots

Snapshots are **backend machinery** used to implement pause, resume, and retry.

They are not a primary user-facing MVP feature. Users should not need to manually trigger or manage snapshots.

The snapshot mechanism enables:

- suspending execution state when compute is released after a `wait()` timeout
- restoring execution context when retry or resume triggers a rerun
- preserving the frozen execution spec across process boundaries

For the MVP, snapshots are an implementation detail. Dashboard-triggered snapshot management may come later.

## Side effects and idempotency

Kitaru makes execution durable. It does **not** automatically make side effects safe.

A checkpoint may be:

- retried
- replayed
- re-executed after a replay point

So external side effects such as:

- creating PRs
- sending emails
- deploying services
- writing to external systems

must be either:

- naturally idempotent
- guarded by external idempotency keys
- split into plan + commit stages

If a checkpoint is replayed from history, its side effects do not happen again. If it is re-executed live, its side effects may happen again.

## What should live inside a durable boundary

Kitaru gives you plain Python orchestration, but not every line of Python is automatically durable.

Anything that is:

- expensive
- non-deterministic
- side-effecting
- important for audit or replay

should live inside a durable boundary such as a checkpoint.

For example, this is risky:

```python
@kitaru.flow
def bad():
    branch = random.choice(["a", "b"])
    return step(branch)
```

This is safer:

```python
@kitaru.checkpoint
def choose_branch() -> str:
    return random.choice(["a", "b"])

@kitaru.flow
def good():
    branch = choose_branch()
    return step(branch)
```

The same principle applies to reading time, UUIDs, external state, and environment-dependent values that influence control flow.
