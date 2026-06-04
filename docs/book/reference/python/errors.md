---
description: "Kitaru exception hierarchy and shared failure helpers."
---


# `kitaru.errors`

Kitaru exception hierarchy and shared failure helpers.

## `FailureOrigin`

High-level origin categories for execution failures.

**Attributes**

| Name | Type | Description |
| --- | --- | --- |
| `USER_CODE` |  |  |
| `RUNTIME` |  |  |
| `BACKEND` |  |  |
| `DIVERGENCE` |  |  |
| `UNKNOWN` |  |  |

## `KitaruError`

Base class for all Kitaru-specific exceptions.

## `KitaruUsageError`

Raised when API inputs are invalid.

## `KitaruContextError`

Raised when APIs are called outside their valid runtime context.

## `KitaruStateError`

Raised when execution state does not allow the requested operation.

## `KitaruRuntimeError`

Raised for runtime/serialization/materialization failures.

## `KitaruAmbiguousFlowResultError`

Raised when ``flow.run(...).wait()`` cannot pick a single return value.

Common in agent-style flows where each model/tool call produces its own
checkpoint with no DAG sink. The per-checkpoint artifacts are still
persisted and visible in the Kitaru UI / via ``KitaruClient`` — the
exception message points at them.

## `KitaruExecutionError`

Raised when a flow execution finishes unsuccessfully.

**Attributes**

| Name | Type | Description |
| --- | --- | --- |
| `exec_id` | `str | None` |  |
| `status` | `ExecutionStatus | None` |  |
| `failure_origin` | `FailureOrigin | None` |  |

**Constructor**

```python
KitaruExecutionError(message, *, exec_id=None, status=None, failure_origin=None) -> None
```

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `message` | `str` |  |  |
| `exec_id` | `str | None` | `None` |  |
| `status` | `ExecutionStatus | str | None` | `None` |  |
| `failure_origin` | `FailureOrigin | None` | `None` |  |

## `KitaruUserCodeError`

Raised when user checkpoint/flow code fails.

## `KitaruBackendError`

Raised when Kitaru cannot communicate with the backend.

## `KitaruLogRetrievalError`

Raised when runtime logs cannot be retrieved from the backend.

## `KitaruDivergenceError`

Raised when replay divergence is detected by the backend.

## `KitaruWaitValidationError`

Raised when wait-resume input fails schema validation.

## `KitaruStackIntegrationDependencyError`

Raised when the active stack is missing local integration dependencies.

## `KitaruFeatureNotAvailableError`

Raised when a documented API is intentionally not implemented yet.
