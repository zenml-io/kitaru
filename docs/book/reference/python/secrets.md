---
description: "Public helpers for reading and managing Kitaru-managed secrets."
---


# `kitaru.secrets`

Public helpers for reading and managing Kitaru-managed secrets.

## `get_secret`

```python
get_secret(name_or_id) -> Secret
```

Read a stored secret by exact name or ID.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `name_or_id` | `str` |  | Secret name or ID. Partial name and partial ID matches are disabled so the lookup resolves exactly one intended secret. |

**Returns:** A Kitaru-native ``Secret`` model with normalized string values.

## `create_secret`

```python
create_secret(name, values, *, private=False) -> SecretSummary
```

Create a secret and return metadata without raw secret values.

New secrets are public by default. Pass ``private=True`` to create a
private backend secret.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | `str` |  |  |
| `values` | `Mapping[str, Any]` |  |  |
| `private` | `bool` | `False` |  |

## `delete_secret`

```python
delete_secret(name_or_id) -> SecretSummary
```

Delete a secret by exact name or ID and return deleted metadata.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `name_or_id` | `str` |  |  |

## `Secret`

Kitaru-native view of a stored secret.

The model intentionally exposes only stable Kitaru SDK fields and hides the
underlying backend response object.

**Attributes**

| Name | Type | Description |
| --- | --- | --- |
| `name` | `str` | Secret name. |
| `id` | `str` | Backend secret ID, normalized to a string. |
| `values` | `dict[str, str]` | Readable secret key/value pairs, normalized to strings. |
| `private` | `bool | None` | Whether the backend marks the secret as private, when known. |
| `model_config` |  |  |

### `Secret.get`

```python
Secret.get(key, default=None) -> str | None
```

Return a secret value by key, or a default when the key is absent.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `key` | `str` |  |  |
| `default` | `str | None` | `None` |  |

## `SecretSummary`

Metadata-only view of a stored secret.

This model is safe to return from write/delete operations because it lists
key names but never includes raw secret values.

**Attributes**

| Name | Type | Description |
| --- | --- | --- |
| `name` | `str` |  |
| `id` | `str` |  |
| `private` | `bool` |  |
| `keys` | `list[str]` |  |
| `has_missing_values` | `bool` |  |
| `model_config` |  |  |
