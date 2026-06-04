---
description: "Flow decorator for defining durable executions."
---


# `kitaru.flow`

Flow decorator for defining durable executions.

A flow is the outer orchestration boundary in Kitaru. It marks the top-level
function whose execution becomes durable, replayable, and observable.

## `flow`

```python
flow(func=None, *, stack=None, image=None, cache=None, retries=None) -> _FlowDefinition | Callable[[Callable[..., Any]], _FlowDefinition]
```

Mark a function as a durable flow.

Can be used as a bare decorator or with arguments:

```python
@flow
def my_flow(...):
    ...

@flow(stack="prod", retries=2)
def my_other_flow(...):
    ...
```

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `func` | `Callable[..., Any] | None` | `None` | Optional function for bare decorator use. |
| `stack` | `str | None` | `None` | Default execution stack. |
| `image` | `ImageSetting | None` | `None` | Default image settings. |
| `cache` | `bool | None` | `None` | Optional cache override (when omitted, lower-precedence config sources apply and eventually default to ``True``). |
| `retries` | `int | None` | `None` | Optional retry override (when omitted, lower-precedence config sources apply and eventually default to ``0``). Retries rerun the whole flow body, including any side effects that happened before a post-return internal result-artifact save failure. |

**Returns:** The wrapped flow object or a decorator that returns it.

## `FlowHandle`

Handle for a running or finished flow execution.

**Attributes**

| Name | Type | Description |
| --- | --- | --- |
| `exec_id` | `str` | Execution identifier for this flow run. |
| `status` | `ExecutionStatus` | Current execution status. |

**Constructor**

```python
FlowHandle(run, *, observed_started_at=None, analytics_metadata=None, track_terminal_if_finished=False) -> None
```

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `run` | `PipelineRunResponse` |  | Initial pipeline run response. |
| `observed_started_at` | `float | None` | `None` | SDK-observed start time from ``time.perf_counter``. |
| `analytics_metadata` | `dict[str, Any] | None` | `None` | Privacy-safe metadata captured at submission time. |
| `track_terminal_if_finished` | `bool` | `False` | Emit terminal analytics immediately when the initial run is already terminal. |

### `FlowHandle.wait`

```python
FlowHandle.wait() -> Any
```

Block until execution finishes and return its result.

**Returns:** The flow return value.

### `FlowHandle.get`

```python
FlowHandle.get() -> Any
```

Get the flow result without waiting.

**Returns:** The flow return value.
