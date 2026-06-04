---
description: "Artifact helpers for explicit named artifacts."
---


# `kitaru.artifacts`

Artifact helpers for explicit named artifacts.

``kitaru.save()`` persists a named artifact inside a checkpoint.
``kitaru.load()`` retrieves a named artifact from a previous execution.

Both are valid only inside a checkpoint.

## `save`

```python
save(name, value, *, type='output', tags=None) -> None
```

Persist a named artifact inside the current checkpoint.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | `str` |  | Artifact name (unique within the checkpoint). |
| `value` | `Any` |  | The value to persist. Must be serializable. |
| `type` | `str` | `'output'` | Artifact type for categorization (one of ``"prompt"``, ``"response"``, ``"context"``, ``"input"``, ``"output"``, ``"blob"``). |
| `tags` | `list[str] | None` | `None` | Optional tags for filtering and discovery. |

## `load`

```python
load(exec_id, name) -> Any
```

Load a named artifact from a previous execution.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `exec_id` | `str` |  | The execution ID to load from. |
| `name` | `str` |  | The artifact name to retrieve. |

**Returns:** The materialized artifact value.
