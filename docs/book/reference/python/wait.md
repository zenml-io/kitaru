---
description: "Wait primitive for durable suspension."
---


# `kitaru.wait`

Wait primitive for durable suspension.

``kitaru.wait()`` suspends a running flow until input is provided.  On local
runs with an interactive terminal, the runtime prompts for input directly in
the same terminal and the flow continues in-process.  In non-interactive
contexts (remote orchestrators, CI, piped output, etc.), the execution moves
to ``waiting`` status and input must be supplied later via the client API, CLI,
or MCP.

Waits belong at flow scope. They are rejected inside ``@checkpoint`` bodies
because a timeout pause signal cannot pass cleanly through a checkpoint step:
the run may pause, but the enclosing step can still be recorded as failed.

## `wait`

```python
wait(*, schema=None, name=None, question=None, timeout=None, metadata=None) -> Any
```

Suspend the current flow until input is provided.

On local interactive runs the runtime prompts for input in the same
terminal and the flow continues automatically.  In non-interactive
contexts the execution pauses until input is supplied externally via
``KitaruClient``, the CLI, or MCP.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `schema` | `Any` | `None` | Expected type of the input value. When ``None`` (the default), the wait acts as a pure continue/abort gate and returns ``None`` on continuation. Pass an explicit type (e.g. ``bool``, ``str``, a Pydantic model) when the caller needs structured input from the human. |
| `name` | `str | None` | `None` | Display name for this wait point. |
| `question` | `str | None` | `None` | Human-readable prompt describing what input is needed. |
| `timeout` | `int | None` | `None` | Maximum seconds the runner keeps polling before it pauses the execution and exits. Not an expiration on the wait record itself. Defaults to 600. |
| `metadata` | `dict[str, Any] | None` | `None` | Additional metadata to attach to the wait record. |

**Returns:** The validated input value, provided either inline via the terminal
