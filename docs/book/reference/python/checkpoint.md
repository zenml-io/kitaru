---
description: "Checkpoint decorator for durable work boundaries."
---


# `kitaru.checkpoint`

Checkpoint decorator for durable work boundaries.

A checkpoint is a unit of work inside a flow whose outcome is persisted.
Successful outputs become artifacts; failures are recorded for retry.

## `checkpoint`

```python
checkpoint(func=None, *, retries=0, type=None, runtime=None, cache=None) -> _CheckpointDefinition | Callable[[Callable[..., Any]], _CheckpointDefinition]
```

Mark a function as a durable checkpoint.

Can be used as a bare decorator or with arguments:

```python
from kitaru import checkpoint

@checkpoint
def my_step(): ...

@checkpoint(retries=3, type="llm_call")
def my_step(): ...

@checkpoint(runtime="isolated")
def heavy_step(): ...
```

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `func` | `Callable[..., Any] | None` | `None` | Optional function for bare decorator use. |
| `retries` | `int` | `0` | Number of checkpoint-level retries on failure. |
| `type` | `str | None` | `None` | Checkpoint type for dashboard visualization. |
| `runtime` | `str | None` | `None` | Execution runtime for this checkpoint. Accepts ``"inline"`` or ``"isolated"`` (case-insensitive). When set to ``"isolated"``, the checkpoint runs in its own container on remote orchestrators that support it. ``None`` (the default) lets the orchestrator decide. |
| `cache` | `bool | None` | `None` | Optional per-checkpoint cache override. ``True`` enables cache for this checkpoint, ``False`` disables it, and ``None`` defers to higher-level flow/default cache behavior. Useful for forcing one expensive step to re-run while the rest of the flow stays cached. |
