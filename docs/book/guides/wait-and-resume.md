---
description: Suspend a flow for external input and continue the same execution
icon: hourglass-half
---

# Wait, Input, and Resume

`kitaru.wait(...)` is the durable suspension primitive for human-in-the-loop
and external-input workflows.

When your flow reaches `wait(...)`, what happens next depends on the environment:

- **Local interactive runs** (terminal with stdin/stdout): the runtime prompts
  for input directly in the same terminal and the flow continues in-process.
- **Non-interactive runs** (remote orchestrators, CI, piped output, MCP): the
  execution moves to `waiting` status and input must be supplied later via the
  client, CLI, or MCP.

## Basic pattern

```python
from kitaru import flow
import kitaru

@flow
def publish_flow(topic: str) -> str:
    approved = kitaru.wait(
        schema=bool,
        name="approve_publish",
        question=f"Approve publishing {topic}?",
        metadata={"topic": topic},
    )
    if not approved:
        return f"REJECTED: {topic}"
    return f"PUBLISHED: {topic}"
```

## Get the current execution ID inside a flow

If code is already running inside a Kitaru flow or checkpoint, use
`kitaru.current_execution_id()` to read the active execution ID:

```python
exec_id = kitaru.current_execution_id()
print(f"kitaru executions input {exec_id} --value true")
```

This is useful when a flow needs to print follow-up CLI commands, name
per-execution side resources, or attach the execution ID to application logs.
Outside a running Kitaru flow or checkpoint, it returns `None`.

`kitaru.wait()` itself must run at flow scope, before or after checkpoint calls,
not inside a `@checkpoint`. For why this rule exists and how to split work
around a wait, see
[Where wait() can be called](../concepts/wait-and-input.md#where-wait-can-be-called).

## Timeout behavior

By default, `kitaru.wait(...)` gives the runner up to 600 seconds to receive
inline input before pausing the execution:

```python
approved = kitaru.wait(
    schema=bool,
    name="approve_publish",
    question="Approve publishing?",
    timeout=300,  # runner waits up to 300s for inline resolution
)
```

`timeout` controls how long the runner keeps polling — it is **not** an
expiration on the wait record itself. If the timeout elapses without input, the
execution moves to `waiting` status and can still be resolved externally at any
time.

## Resolve input externally (non-interactive runs)

When the flow is running in a non-interactive context (or after the runner has
timed out and paused), resolve the pending wait from Python or the CLI.

### From Python

```python
client = kitaru.KitaruClient()
execution = client.executions.input(
    exec_id,
    wait="approve_publish",
    value=True,
)
```

If the execution does not continue automatically after input (e.g. the original
runner already exited), call `resume(...)`:

```python
execution = client.executions.resume(exec_id)
```

To abort a wait instead of continuing:

```python
execution = client.executions.abort_wait(exec_id, wait="approve_publish")
```

### From CLI

```bash
kitaru executions input <exec_id> --value true
```

The CLI auto-detects the single pending wait. For interactive review
(with JSON schema display, continue/abort choices, and multi-execution sweep):

```bash
kitaru executions input <exec_id> --interactive
kitaru executions input --interactive  # sweep all waiting executions
```

To abort a wait instead of continuing:

```bash
kitaru executions input <exec_id> --abort
```

Resume is only needed when the execution did not continue automatically:

```bash
kitaru executions resume <exec_id>
```

## Validation behavior

Input is validated against the wait schema:

- invalid input raises `kitaru.KitaruWaitValidationError`
- execution stays in `waiting` until valid input is supplied

## Example in this repository

```bash
uv sync --extra local
uv run python examples/features/execution_management/wait_and_resume.py
```

1. Run the command above — it starts the flow and blocks.
2. If you are in an interactive terminal, the runtime prompts for input
   directly. Type your answer and the flow continues.
3. If the run is non-interactive (or you want to resolve from elsewhere), the
   script prints fallback `kitaru executions input ...` and
   `kitaru executions resume ...` commands you can run in a second terminal.

Automated CI coverage for the same flow lives in
`tests/test_phase15_wait_example.py`.

For the broader catalog, see [Examples](../getting-started/examples.md).

## Related blog posts

- [Anatomy of a production agent](https://kitaru.ai/blog/anatomy-of-a-production-agent)
- [Why agents need durable execution](https://kitaru.ai/blog/why-agents-need-durable-execution)
