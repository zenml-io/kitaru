# Checkpoint live-event publishing

This example shows how to publish best-effort live events from inside a Kitaru
checkpoint.

The important idea: the checkpoint output is still the durable record. Live
events are progress postcards for anything watching the backend stream.

## Run it

```bash
cd examples/features/checkpoint_streaming
kitaru init
uv run checkpoint_streaming.py
```

If your active backend has live streaming enabled, the `kitaru.progress(...)`
and `kitaru.events.publish(...)` calls are sent while the checkpoints run. If
streaming is not enabled, the flow still succeeds and the events are skipped.

## What to look for in the code

- `kitaru.progress("Choosing sections", percent=0.25, topic=topic)` publishes a
  standard checkpoint progress event.
- `kitaru.events.publish("report.outline.ready", {...})` publishes a custom
  event kind.
- Kitaru automatically publishes checkpoint lifecycle events around the function
  body: started, completed, or failed.

See the docs guide for the event envelope and replay/cache behavior:
<https://kitaru.ai/docs/guides/checkpoint-streaming/>.
