# LangGraph adapter example (local, deterministic)

This example teaches a real Kitaru flow that calls a LangGraph graph twice:

1. **start call** → graph returns `interrupted`
2. **resume call** → graph returns `completed`

No model API, no credentials, and no network calls are required.

## What the graph does

The graph has two nodes:

- **`request_decision`** pauses with `interrupt(...)` and asks:
  `Approve ticket escalation?`
- **`finalize`** runs after resume and sets `status` to `approved` or `rejected`

So the graph story is:

- begin,
- pause for a decision,
- resume with a decision payload,
- finish.

## What Kitaru adds in this script

The script runs an actual `@kitaru.flow` named `run_demo_flow`.
Inside that flow, it calls `runner.invoke(...)` twice (start then resume).

That means Kitaru records two outer graph-call checkpoints in one flow execution.

After the second call, the flow saves a user-facing summary artifact:

- artifact name: `summary__langgraph_demo`

The script then loads that artifact via `KitaruClient` and prints it.

## Why `thread_id` matters

The example uses a fixed thread id:

```text
langgraph-local-demo-thread
```

Think of this as the graph conversation key.

- The first call creates/advances that thread.
- The resume call must use the same key to continue the paused state.

If the key changes, LangGraph treats it as a different thread and your paused run is not resumed.

## What is checkpointed (and what is not)

### Checkpointed in this example

- Kitaru flow execution metadata
- two adapter graph-call checkpoints (start and resume)
- summary artifact (`summary__langgraph_demo`)
- adapter metadata such as status and latest checkpoint id

### Not checkpointed by this example

- full sandbox filesystem snapshots
- arbitrary Python process memory
- every external side effect inside arbitrary graph-node code

Also: this example uses LangGraph `InMemorySaver`, which is ideal for local learning, not durable storage across process/container restarts.

## Run it

From this directory:

```bash
cd examples/integrations/langgraph_agent
uv pip install 'kitaru[local,langgraph]'
kitaru init
uv run langgraph_adapter.py
```

Or from the repository root:

```bash
uv run examples/integrations/langgraph_agent/langgraph_adapter.py
```

## What you should see

The script prints:

- execution id,
- first status (`interrupted`),
- resume status (`completed`),
- interrupt payload,
- final graph output,
- latest checkpoint metadata.

For the conceptual walkthrough, see the guide:
[LangGraph Adapter](https://kitaru.ai/docs/guides/langgraph-adapter).

For the full catalog, see [../../README.md](../../README.md).
