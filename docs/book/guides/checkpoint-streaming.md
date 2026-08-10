---
description: Publish and watch best-effort live progress and custom events from running checkpoints
icon: tower-broadcast
---

# Checkpoint Live Events

Kitaru checkpoints can publish **live events** while their function body runs, so
a dashboard, log tail, or custom monitor can watch work in progress. Live events
are best-effort observability, not the durable record of a run.

The durable record is still the checkpoint result, artifacts, logs, and metadata
— the same record that makes a run replayable. Live events are not part of it and
may be dropped, replayed, or skipped (see [Replay and cache behavior](#replay-and-cache-behavior)).

That distinction is also the privacy policy. By default, adapter live events say
**what kind of thing is happening**, not the user's prompt, raw tool arguments,
raw tool results, full SDK event objects, or the final model answer. If you need
content-bearing stream payloads, use the adapter-specific explicit opt-in.

## Quick start

Use `kitaru.progress(...)` for human-readable progress updates:

```python
import kitaru
from kitaru import checkpoint, flow


@checkpoint
def build_report(topic: str) -> str:
    kitaru.progress("Collecting notes", percent=0.25, topic=topic)
    notes = f"Notes about {topic}"

    kitaru.progress("Writing draft", percent=0.75)
    return f"Report: {notes}"


@flow
def report_flow(topic: str) -> str:
    return build_report(topic)
```

Use `kitaru.events.publish(...)` when you want your own event kind:

```python
@checkpoint
def train_model(rows: int) -> str:
    kitaru.events.publish(
        "training.batch.completed",
        {"batch": 1, "rows": rows},
        message="First batch finished",
    )
    return "model-ready"
```

Each event is sent with Kitaru metadata under `payload["kitaru"]`, including the
checkpoint name and a `correlation_id` shared by all live events from that one
checkpoint execution.

## Event ordering inside one checkpoint

For one checkpoint execution, Kitaru publishes function-body lifecycle events
automatically:

1. `kitaru.checkpoint.started`
2. your `kitaru.progress(...)` or `kitaru.events.publish(...)` calls
3. `kitaru.checkpoint.returned` if the Python function body returns, or
   `kitaru.checkpoint.failed` if the Python function body raises

Automatic failed lifecycle events include the exception type, but intentionally
use a generic message instead of publishing the raw exception text. That way a
mistake like `RuntimeError("api_key=...")` still fails the checkpoint without
broadcasting the secret through the live-event feed.

`kitaru.checkpoint.returned` is intentionally named narrowly: it means the
checkpoint function returned a value. It does **not** mean the returned value has
already been durably stored as a checkpoint output. Use the
execution status, `handle.wait()`, or `KitaruClient().executions.get(...)` when
you need the durable final checkpoint outcome.

Those events share one correlation ID and one increasing `index` counter. A
simple run might look like this:

```text
index 0 → kitaru.checkpoint.started
index 1 → kitaru.checkpoint.progress      # your call
index 2 → kitaru.checkpoint.returned
```

If the same checkpoint is fanned out with `.map()` or `.product()`, each actual
checkpoint execution gets its own correlation ID. That way a UI can draw one
ordered event lane per checkpoint execution rather than mixing all fan-out items
together.

## Watch events from Python

Use `KitaruClient().executions.events(...)` to watch live events for one
execution. The first argument must be the exact execution ID; name or ID prefixes
are not accepted for live event watching:

```python
from kitaru import KitaruClient

client = KitaruClient()

for event in client.executions.events(
    "execution-id",
    kinds=["kitaru.checkpoint.progress"],
    checkpoint="build_report",
):
    print(event.kind, event.checkpoint_name, event.payload.get("message"))
```

The event object has four identity/order fields that do different jobs:

- `kind` is what happened, for example `kitaru.checkpoint.progress`.
- `correlation_id` groups events from one checkpoint execution.
- `index` orders events inside that correlation group.
- `cursor` is the backend stream position used for reconnecting.

That last point matters. If the network drops, Kitaru reconnects with the SSE
`cursor`. It does **not** use `index` or `correlation_id` as a resume marker.
Those fields are useful for your UI; the cursor is for the wire.

You can filter by event kinds, checkpoint name, correlation IDs, or an existing
cursor:

```python
client.executions.events(
    "execution-id",
    kinds=["kitaru.checkpoint.started", "kitaru.checkpoint.returned"],
    checkpoint="train_model",
    correlation_ids=["kitaru.checkpoint:train_model:..."],
    since="last-seen-cursor",
    reconnect=True,
)
```

Checkpoint filtering is guaranteed client-side. For completed executions,
Kitaru may also send known step names to the backend to reduce traffic. For live
executions, Kitaru deliberately avoids that server-side step-name filter because
the matching checkpoint step may not exist yet when the watcher starts.

There is no CLI watcher yet. Use the Python watcher for now.

## Adapter live events

Some agent adapters publish their own live events while an adapter-owned
checkpoint is running. For example, the PydanticAI adapter emits
`pydantic_ai.stream.event` when you use PydanticAI streaming hooks, the Claude
Agent SDK adapter emits `claude_agent_sdk.stream.event` when you call
`KitaruClaudeRunner.run_stream(...)` or `run_stream_sync(...)`, and the
LangGraph adapter emits `langgraph.stream.*` events when you call
`KitaruGraphRunner.stream(...)` or `astream(...)` in `graph_call` mode. These
events may come from inside synthetic checkpoints created by the adapter, but
the mechanism is the same as custom checkpoint live events: watch them with
`KitaruClient().executions.events(...)`, and treat them as best-effort progress
rather than saved state.

The separately packaged OpenAI Agents v2 adapter (`kitaru-openai-agents`) does not support `run_streamed` or publish adapter streaming events.

See [PydanticAI Adapter](../adapters/pydantic-ai.md#streaming), [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md#live-streaming-with-kitaru-durability), and [LangGraph Adapter](../adapters/langgraph.md#graph-call-streaming) for adapter-specific details and runnable examples.

## Backend requirements

Live-event publishing and watching use the active Kitaru backend's streaming
support.

If streaming is unavailable while publishing, publishing degrades safely: the
checkpoint keeps running, and event delivery is skipped. That is deliberate. A
broken live-event lane should not break your training job, tool call, or agent
step.

Watching is stricter. If you ask Kitaru to watch events while using local
database mode, or while connected to a server where streaming is disabled,
Kitaru raises `KitaruFeatureNotAvailableError` with a clear explanation.

## Replay and cache behavior

Live events describe what happened while code was running **this time**.

That has two important consequences:

- **Replay may publish events again.** If a checkpoint body re-executes during
  replay, its progress postcards can be sent again too.
- **Cache hits may publish no events.** If Kitaru reuses a cached checkpoint
  result, the checkpoint body does not run, so there may be no started/progress/
  returned events for that checkpoint.

So a replay's live feed is not a reliable diff signal. To compare a baseline run
against a replay, use checkpoint outputs and artifacts, not live events.

## API reference

```python
KitaruClient().executions.events(
    exec_id: str,
    *,
    kinds: list[str] | None = None,
    checkpoint: str | None = None,
    correlation_ids: list[str] | None = None,
    since: str | None = None,
    reconnect: bool = True,
) -> Iterator[ExecutionEvent]
```

Yields live `ExecutionEvent` objects with `exec_id`, `kind`, `payload`,
`correlation_id`, `index`, `cursor`, `checkpoint_id`, `checkpoint_name`, and
`step_name`.

```python
kitaru.progress(
    message: str,
    *,
    percent: float | None = None,
    correlation_id: str | None = None,
    flush: bool = False,
    **fields,
)
```

`percent` is a number from `0` to `1`. Extra keyword fields are placed under the
event payload's `data` key.

```python
kitaru.events.publish(
    kind: str,
    payload: Mapping[str, Any] | None = None,
    *,
    message: str | None = None,
    correlation_id: str | None = None,
    index: int | None = None,
    flush: bool = False,
)
```

`kind` is your event type. It cannot use server control names such as `cursor`,
`end`, `gap`, `error`, or `system`.

```python
kitaru.events.flush(timeout=2.0) -> bool
```

`flush(...)` asks the process-local publisher to drain queued events. It does
not require checkpoint scope and returns `False` if flushing fails or times out.

## Related pages

- [Checkpoints](../concepts/checkpoints.md)
- [Execution Runtime Logs](execution-logs.md)
- [Logging and Metadata](../concepts/logging.md)
