---
description: "Structured metadata logging."
---


# `kitaru.logging`

Structured metadata logging.

``kitaru.log()`` attaches structured key-value metadata to the current
checkpoint or execution. It is context-sensitive: inside a checkpoint
it attaches to that checkpoint; inside a flow but outside a checkpoint
it attaches to the execution.

## `log`

```python
log(**kwargs) -> None
```

Attach structured metadata to the current checkpoint or execution.

Standard keys include ``cost``, ``tokens``, ``latency``, but arbitrary
user-defined keys are accepted.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `kwargs` | `Any` | `{}` |  |
