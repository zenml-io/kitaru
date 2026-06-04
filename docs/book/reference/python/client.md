---
description: "Kitaru client for execution and artifact management."
---


# `kitaru.client`

Kitaru client for execution and artifact management.

`KitaruClient` provides a programmatic API for inspecting and managing
executions and artifacts outside flow bodies.

## `KitaruClient`

Client for Kitaru executions, artifacts, deployments, and auth.

**Attributes**

| Name | Type | Description |
| --- | --- | --- |
| `auth` |  |  |
| `executions` |  |  |
| `artifacts` |  |  |
| `deployments` |  |  |

**Constructor**

```python
KitaruClient(*, server_url=None, auth_token=None, project=None) -> None
```

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `server_url` | `str | None` | `None` | Optional per-client server override (not yet supported). |
| `auth_token` | `str | None` | `None` | Optional per-client auth token override (not yet supported). |
| `project` | `str | None` | `None` | Optional per-client project override (not yet supported). |

### `KitaruClient.for_auth_management`

```python
KitaruClient.for_auth_management() -> KitaruClient
```

Create a client for server-level auth management.

Normal ``KitaruClient()`` construction remains strict and requires a
project for env-driven remote connections. Auth management is
server-level, so this constructor validates server/auth pairing while
intentionally skipping project validation.
