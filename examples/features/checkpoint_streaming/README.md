# Checkpoint streaming example

This example shows checkpoint-level live events in the terminal. It is designed
for a short video: one terminal runs work, and a second terminal watches the
postcards sent from inside the checkpoint while that work is still happening.

The demo does not call OpenAI or any external API. It sleeps between steps so
you can see the event stream move.

## What you will see

- `kitaru.progress(...)` sends ordinary progress updates from inside a
  checkpoint.
- `kitaru.events.publish(...)` sends a custom event when a named piece of work
  is ready.
- `KitaruClient().executions.events(...)` watches those events from another
  process.

There are two useful ways to run it:

1. **Local flow demo:** `kitaru init` is enough to run the flow on your machine.
   In this mode, the progress calls are safe best-effort no-ops if live
   streaming is not available.
2. **Live watching demo:** Terminal B needs a REST-backed Kitaru/ZenML server
   with stream-event support, plus a logged-in client. If you are in local
   database mode or connected to an older server, the watcher will kindly tell
   you live execution events are not available.

## Terminal B — start the watcher first

For the live watching demo, start Terminal B before Terminal A. The watcher can
wait for the next running demo execution, so you do not need to race to copy an
execution ID while recording.

First connect to a streaming-enabled REST-backed server if you have not already:

```bash
kitaru login <server-url>
```

Then start the watcher from this example directory:

```bash
cd examples/features/checkpoint_streaming
uv run python watch_checkpoint_events.py
```

It will print:

```text
Waiting for the next running `streaming_brief` execution...
```

## Terminal A — run the flow

Now run the flow in Terminal A:

```bash
cd examples/features/checkpoint_streaming
kitaru init
uv run python checkpoint_streaming_flow.py "garden robot"
```

That command is enough for the local flow demo. For a slower recording, increase
the delay between postcards:

```bash
CHECKPOINT_STREAMING_DELAY_SECONDS=8 uv run python checkpoint_streaming_flow.py "garden robot"
```

If you already have an execution ID, Terminal B can still watch it directly:

```bash
uv run python watch_checkpoint_events.py <execution-id>
```

You should see output shaped like:

```text
12:00:01  draft_brief        started
12:00:02  draft_brief         20.0%  Collecting source notes
12:00:04  draft_brief         45.0%  Comparing the strongest claims
12:00:06  draft_brief         75.0%  Writing the first version
12:00:08  draft_brief                Recommendation section is ready
12:00:10  draft_brief        100.0%  Brief complete
12:00:10  draft_brief        completed
```

## Why this is different from logs

Think of the checkpoint as a room where work is happening. Progress events are
postcards pushed out through the door while the work is still happening. Logs
and artifacts are the durable record you inspect after, or alongside, the work.

Use streaming for live UX and demos. Use `kitaru.log(...)` or artifacts when the
information must be queryable later.
