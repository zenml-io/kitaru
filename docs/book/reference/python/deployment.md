---
description: "SDK-facing deployment facade with convenience operations."
---

# `Deployment`

SDK-facing deployment facade with convenience operations.

**Attributes**

| Name | Type | Description |
| --- | --- | --- |
| `deployment_id` | `str` | Backend snapshot/deployment identifier. |
| `flow` | `str` | Flow name. |
| `version` | `int` | Deployment version. |
| `tags` | `dict[str, bool]` | Public deployment tags mapped to exclusivity flags. |
| `commit_sha` | `str | None` | Best-effort source commit SHA. |
| `commit_dirty` | `bool | None` | Best-effort source dirty flag. |
| `image_digest` | `str | None` | Best-effort image digest. |
| `created_at` | `datetime | None` | Creation timestamp when provided by the backend. |
| `schema` | `dict[str, Any] | None` | Best-effort deployment input schema. |
| `stack` | `str | None` | Stack name or ID associated with the deployment snapshot. |

**Constructor**

```python
Deployment(_record) -> None
```

### `Deployment.invoke`

```python
Deployment.invoke(**flow_inputs) -> FlowHandle
```

Invoke this pinned deployment version.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `flow_inputs` | `Any` | `{}` |  |

### `Deployment.add_tag`

```python
Deployment.add_tag(tag, *, exclusive=False) -> Deployment
```

Attach a public tag to this deployment version.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `tag` | `str` |  |  |
| `exclusive` | `bool` | `False` |  |

### `Deployment.remove_tag`

```python
Deployment.remove_tag(tag) -> Deployment
```

Remove a public tag from this deployment version.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `tag` | `str` |  |  |

### `Deployment.delete`

```python
Deployment.delete() -> None
```

Delete this deployment version if no exclusive tag protects it.
