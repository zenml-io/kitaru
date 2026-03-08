# Appendix: Glossary

This glossary defines the core terms used throughout the Kitaru SDK reference.

## Execution

An **execution** is a single run of a `@kitaru.flow`.

It is the top-level durable object Kitaru tracks and owns.

An execution includes:

- execution ID
- flow name
- status
- durable call history
- artifacts
- metadata
- replay and wait state

Examples of execution statuses include:

- `running`
- `waiting`
- `completed`
- `failed`
- `cancelled`

## Flow

A **flow** is the outer orchestration function decorated with `@kitaru.flow`.

It defines the boundary of a durable execution.

A flow:

- runs plain Python orchestration logic
- contains checkpoints, waits, and other runtime calls
- is rerun from the top during replay, resume, and retry

## Checkpoint

A **checkpoint** is a durable work boundary inside a flow, created with `@kitaru.checkpoint`.

A checkpoint:

- executes as a durable unit of work
- persists a successful output as an artifact
- records either a success or failure outcome
- acts as a replay boundary

## Wait

A **wait** is a durable suspension boundary created with `kitaru.wait()`.

A wait:

- records that execution is waiting for input
- suspends the current run
- later returns validated input when the execution reruns
- participates in replay just like other durable calls
- is valid only directly in flow execution, not inside a checkpoint (MVP restriction)

## Durable call

A **durable call** is any runtime boundary whose outcome is recorded and can later be replayed.

In the MVP, the main durable calls are:

- checkpoints
- waits
- standalone `kitaru.llm()` calls in a flow

Durable calls are what make replay possible.

## Outcome

An **outcome** is the recorded result of a durable call.

Examples include:

- returned value
- raised exception
- wait input
- cancellation

Replay works by reusing prior outcomes, not only successful outputs.

## Call record

A **call record** is the durable history entry for one durable call instance.

A call record should identify:

- which execution it belongs to
- what kind of call it was
- its display name
- its stable call instance ID
- its sequence position
- its outcome

A call record is not the same thing as an artifact.

## Artifact

An **artifact** is a persisted value produced by a durable call.

Examples include:

- a checkpoint return value
- an LLM prompt
- an LLM response
- a wait input
- an explicitly saved blob or context object

Artifacts are used for:

- replay support
- dashboard rendering
- debugging
- lineage
- local inspection

## Metadata

**Metadata** is structured key-value information attached to a checkpoint or execution.

Examples include:

- token counts
- cost
- latency
- quality scores
- debug flags
- custom business fields

Metadata is attached with `kitaru.log()`.

## Retry

**Retry** is same-execution recovery after failure.

Retry means:

- the same logical execution continues
- fixed code, fixed config, no user overrides
- rerun from the top, replaying prior durable outcomes
- re-execute from the failure point forward
- the execution timeline shows failed attempts and retry continuations under one execution

Retry does **not** create a new execution. If the user wants to change code/config/inputs, that is a replay.

Checkpoint retries are narrow and local (re-execute one checkpoint boundary). Flow retries are broader (rerun from the top).

## Resume

**Resume** continues an existing waiting execution after `wait()` input arrives.

Resume means:

- record validated input for the current wait
- rerun the flow from the top (same execution)
- replay prior durable outcomes
- return the recorded wait input at the wait site
- continue execution from there

Resume is different from replay because it continues the **same execution** instead of creating a new one.

## Replay

**Replay** creates a **new execution** based on a previous one.

Replay means:

- rerun the flow from the top
- reuse prior durable outcomes before the replay point
- execute live at and after the replay point
- optionally apply overrides
- may use changed code, config, or inputs

Replay does not mutate the original execution. Because it may involve changes, it **must** be a new execution.

## Replay point

The **replay point** is the durable call where a replay stops reusing historical outcomes and starts executing live again.

Examples:

- replay from `write_draft`
- replay from `approve_review`

Before the replay point, outcomes are reused unless overridden.

At and after the replay point, execution may happen live again.

## Override

An **override** is a value injected into a replay so that a historical outcome is replaced in the new execution.

Useful override targets include:

- flow input
- checkpoint outcome
- wait input

Overrides act like synthetic replayed outcomes in the new execution and do not mutate the original run.

## Divergence

**Divergence** happens when the durable call sequence in replay no longer matches the historical execution before the replay point. Divergence detection is implemented in the ZenML backend — Kitaru exposes the user-visible error.

Examples:

- inserting a checkpoint before the replay point
- removing a wait before the replay point
- changing loop structure so durable call ordering changes

Divergence should fail loudly rather than silently reusing the wrong history.

## Stack

A **stack** is a named execution target or infrastructure profile.

A stack determines things like:

- local vs remote execution
- where artifacts and execution journal data live
- what runtime capabilities are available

Stacks include components for runner, artifact store, and container registry. A stack is not the same thing as app config.

**Note:** LLM model configuration (aliases, credentials) is managed through the local model registry, not through stacks. See **Model registry** and **Model alias** below.

## Model registry

The **model registry** is user-local configuration that stores LLM model aliases and optional references to ZenML secrets for provider credentials. It is managed via `kitaru model register` and is independent of stack selection.

The registry allows `kitaru.llm()` to resolve aliases like `fast` or `smart` to concrete LiteLLM model identifiers. Alias definitions are stored on the user's machine, not in a stack or on the server. Credentials for remote execution are stored in ZenML secrets and referenced by name from the alias.

## Model alias

A **model alias** is a short name (e.g. `fast`, `smart`) that maps to a concrete LiteLLM model identifier (e.g. `openai/gpt-4o-mini`). Aliases are defined in the local model registry and let user code reference models by role rather than specific provider/model name.

## Secret

A **secret** is a named bag of key-value pairs stored in ZenML's centralized secret store. Secrets are managed via `kitaru secrets set/show/list/delete`.

Kitaru uses secrets primarily for:

- LLM provider credentials (referenced from model aliases via `--secret`)
- Infrastructure credentials in image environment settings

Secrets are private by default (only the creating user can access them). Secret keys should use actual environment variable names (e.g. `OPENAI_API_KEY`) for compatibility with LiteLLM and ZenML's runtime env injection.

Under the hood, secret metadata lives in the ZenML server database, and secret values live in the configured secrets-store backend.

## App config

**App config** is project-level runtime configuration set through `kitaru.configure()` or `pyproject.toml` under `[tool.kitaru]`. There is no separate `kitaru.toml` file.

Typical app config includes:

- local runtime directory
- project-level defaults

It should not be confused with connection state or stack selection. Rich project-level configuration is likely not in the MVP scope.

## Connection

**Connection** is how the SDK talks to a server.

Under the hood, the Kitaru server **is** the ZenML server. All server URLs are ZenML server URLs. Connection includes:

- server URL (ZenML server)
- auth token or API key

It is separate from both stack selection and app config.

## Resolved execution spec

The **resolved execution spec** is the frozen configuration snapshot attached to a started execution.

It should capture the execution's resolved view of:

- stack
- app config
- relevant flow defaults
- connection context if needed
- code or source version info if available

This prevents resume and retry behavior from drifting when ambient config changes later.

Resume and retry use the original frozen spec. Replay creates a new spec.

## Snapshot

A **snapshot** is ZenML backend machinery used to implement pause, resume, and retry. Kitaru neither owns nor exposes snapshot internals.

Snapshots are **not** a user-facing MVP feature. Users do not manually trigger or manage snapshots.

Snapshots enable:

- suspending execution state when compute is released
- restoring execution context for retry or resume
- preserving the frozen execution spec across process boundaries

Dashboard-triggered snapshot management may come later as a Pro feature.

## Wait timeout

**Wait timeout** on `kitaru.wait()` means **active wait / resource retention timeout**.

- It controls how long compute/resources stay alive while waiting for input
- After the timeout, resources are released but the execution remains in `waiting` state
- The wait does **not** expire or fail after the timeout
- The execution can still be resumed later

This is distinct from any potential future business-level expiration concept.

## Lineage

**Lineage** is the record of how artifacts and durable calls depend on one another.

It helps answer questions like:

- which checkpoint produced this artifact?
- which earlier artifact was used to create this output?
- which previous execution did this checkpoint load from?

Lineage is useful for replay inspection, auditing, and debugging.

## Synthetic checkpoint

A **synthetic checkpoint** is a lightweight durable call boundary created by the runtime for convenience rather than by an explicit `@kitaru.checkpoint` decorator.

In the MVP, a standalone `kitaru.llm()` call in a flow may behave this way.

This is different from a child event inside a checkpoint.

## Child event

A **child event** is a tracked sub-activity inside an enclosing durable boundary.

Examples include:

- an `llm()` call inside a checkpoint
- adapter-emitted model/tool activity inside an outer checkpoint

Child events are useful for visibility, artifacts, and metadata, but they are not independent replay boundaries in the MVP.

## Sandbox

A **sandbox** is a stack component that provides isolated compute for agent execution.

It is relevant for use cases where agents run arbitrary code (e.g., coding agents), and you need:

- isolated execution environment
- resource limits
- protection from unsafe tool calls

The sandbox is an MVP deliverable. Its exact shape is still being defined.

## Concurrency / Futures

**Concurrency** in Kitaru uses the `.submit()` + `.result()` pattern (ZenML futures), not a dedicated primitive.

- `.submit()` kicks off a checkpoint and returns a future immediately
- `.result()` blocks until that checkpoint completes
- multiple `.submit()` calls run concurrently

On replay, concurrently submitted checkpoints replay their recorded outcomes just like sequential ones.

## Log Store

The **log store** is the global backend where runtime logs (stdout/stderr, structured events) are stored. By default, logs go to the artifact store. Users can optionally switch the global log backend to an OTel-compatible provider (e.g. Datadog) via `kitaru log-store set`. There is no explicit local logger stack component — this is a global configuration. See chapter 9.

## Idempotency

**Idempotency** means an operation can be safely repeated without causing duplicate side effects.

This matters in Kitaru because checkpoints may be:

- retried
- replayed
- re-executed after a replay point

External side effects should therefore be idempotent or protected by external idempotency keys.
