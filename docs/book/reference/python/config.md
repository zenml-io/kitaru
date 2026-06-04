---
description: "Configuration and connection management."
---


# `kitaru.config`

Configuration and connection management.

This module contains:

- global config helpers for runtime log-store settings
- stack-selection helpers
- runtime configuration via ``kitaru.configure(...)``
- config precedence resolution for execution and connection settings

## `current_stack`

```python
current_stack() -> StackInfo
```

Return the currently active stack.

The active stack is managed by the underlying runtime and persisted in the
runtime's global user configuration.

## `list_stacks`

```python
list_stacks() -> list[StackInfo]
```

List stacks visible to the current user and mark the active one.

## `create_stack`

```python
create_stack(name, *, activate=True, labels=None) -> StackInfo
```

Create a new local stack and optionally activate it.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | `str` |  |  |
| `activate` | `bool` | `True` |  |
| `labels` | `dict[str, str] | None` | `None` |  |

## `delete_stack`

```python
delete_stack(name_or_id, *, recursive=False, force=False) -> None
```

Delete a stack and optionally its components.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `name_or_id` | `str` |  |  |
| `recursive` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

## `use_stack`

```python
use_stack(name_or_id) -> StackInfo
```

Set the active stack and return the resulting active stack info.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `name_or_id` | `str` |  | Stack name or stack ID. |

**Returns:** Information about the newly active stack.

## `configure`

```python
configure(*, stack=_UNSET, image=_UNSET, cache=_UNSET, retries=_UNSET, project=_UNSET) -> KitaruConfig
```

Set process-local runtime defaults.

Execution-level fields (``stack``, ``image``, ``cache``, ``retries``)
update the execution precedence chain. The ``project`` field updates
the connection precedence chain and is intended as an internal /
testing escape hatch — it is not a normal user-facing setting.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `stack` | `str | None | object` | `_UNSET` | Default stack name/ID override. |
| `image` | `ImageInput | None | object` | `_UNSET` | Default image settings override. |
| `cache` | `bool | None | object` | `_UNSET` | Default cache behavior override. |
| `retries` | `int | None | object` | `_UNSET` | Default retry-count override. |
| `project` | `str | None | object` | `_UNSET` | Project override (internal/testing). Set to ``None`` to clear. |

**Returns:** The current runtime override layer after applying updates.

## `connect`

```python
connect(server_url, *, api_key=None, refresh=False, project=None, no_verify_ssl=False, ssl_ca_cert=None, cloud_api_url=None, timeout=None) -> None
```

Connect to a Kitaru server.

Under the hood, this connects to a ZenML server and stores the resolved
connection/auth state in ZenML's global user configuration.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `server_url` | `str` |  | URL of the Kitaru server. |
| `api_key` | `str | None` | `None` | API key used to authenticate with the server. |
| `refresh` | `bool` | `False` | Force a fresh authentication flow. |
| `project` | `str | None` | `None` | Project name or ID to activate after connecting. |
| `no_verify_ssl` | `bool` | `False` | Disable TLS certificate verification. |
| `ssl_ca_cert` | `str | None` | `None` | Path to a CA bundle used to verify the server. |
| `cloud_api_url` | `str | None` | `None` | Optional managed-cloud API URL used when the server URL points at a managed Kitaru deployment or staging environment. |
| `timeout` | `int | None` | `None` | Optional connection timeout forwarded when supported by the underlying runtime. |
