# LangGraph adapter example (local, deterministic)

This example shows the two LangGraph checkpoint strategies side by side:

- `--strategy graph_call` — one Kitaru checkpoint around each outer LangGraph invocation.
- `--strategy calls` — Kitaru checkpoints around synchronous LangChain model/tool handler calls, using `KitaruLangGraphMiddleware`.

No provider API key, credentials, or network calls are required. The `calls` demo uses LangChain's fake local chat model.

## Run it

From the repository root:

```bash
uv sync --extra local --extra langgraph
uv run kitaru init
uv run kitaru login
uv run examples/integrations/langgraph_agent/langgraph_adapter.py --strategy graph_call
uv run examples/integrations/langgraph_agent/langgraph_adapter.py --strategy calls
```

From this directory, the script path is shorter:

```bash
cd examples/integrations/langgraph_agent
uv run langgraph_adapter.py --strategy graph_call
uv run langgraph_adapter.py --strategy calls
```

## `graph_call` mode: outer graph-call checkpoints

The graph-call demo runs a real `@kitaru.flow` named `run_demo_flow`.
Inside that flow, it calls `runner.invoke(...)` twice:

1. **start call** → graph returns `interrupted`
2. **resume call** → graph returns `completed`

The graph has two nodes:

- **`request_decision`** pauses with `interrupt(...)` and asks:
  `Approve ticket escalation?`
- **`finalize`** runs after resume and sets `status` to `approved` or `rejected`.

What to expect in Kitaru:

- one flow execution;
- two adapter graph-call checkpoints, with names like
  `langgraph_local_interrupt_demo_langgraph_call...`;
- event/run-summary artifacts for each graph call, with names like
  `event_log__langgraph_local_interrupt_demo_...` and
  `run_summary__langgraph_local_interrupt_demo_...`;
- one user-facing summary artifact: `summary__langgraph_demo`.

This is the safe universal mode. Kitaru treats the whole graph call as the replay boundary. LangGraph still owns its internal node replay, checkpointer, stores, and interrupts.

## `calls` mode: LangChain model/tool checkpoints

The calls demo builds a deterministic LangChain agent with:

- a fake local chat model;
- one local `approve_ticket(...)` tool;
- `KitaruLangGraphMiddleware` installed as LangChain middleware;
- `KitaruGraphRunner(..., checkpoint_strategy="calls")`.

The fake model is scripted to do this:

1. receive the user message;
2. request the `approve_ticket` tool;
3. receive the tool result;
4. return a final approval message.

What to expect in Kitaru:

- one flow execution;
- model-call checkpoints with names beginning `model_call__...`;
- one tool-call checkpoint with a name beginning `tool_call__approve_ticket_...`;
- one calls-mode summary checkpoint with a name beginning `langgraph_summary__langgraph_local_calls_demo_...`;
- event/run-summary artifacts, with names like
  `event_log__langgraph_local_calls_demo_...` and
  `run_summary__langgraph_local_calls_demo_...`;
- one user-facing summary artifact: `summary__langgraph_demo`.

This is the granular mode. Kitaru is not splitting open arbitrary LangGraph nodes. It can create true call checkpoints because the middleware is physically wrapped around the synchronous LangChain model/tool handlers.

## Why `thread_id` matters

Both strategies use a fixed thread id:

```text
langgraph-local-demo-thread
```

Think of this as the graph conversation key.

- The first call creates or advances that thread.
- A resume call must use the same key to continue paused state.
- LangGraph checkpointers use this key to find graph-owned state.

If the key changes, LangGraph treats it as a different thread and your paused run is not resumed.

## What is not checkpointed

Neither strategy means "Kitaru owns all LangGraph internals."

Not checkpointed by this example:

- LangGraph's own checkpointer state — this stays LangGraph-owned;
- arbitrary Python process memory;
- full sandbox filesystem snapshots;
- every side effect inside arbitrary graph-node code;
- true async model/tool handler checkpoints.

Also: this example uses LangGraph `InMemorySaver`, which is ideal for local learning, not durable storage across process/container restarts.

## What you should see

For graph-call mode, the script prints:

- execution id;
- first status (`interrupted`);
- resume status (`completed`);
- interrupt payload;
- final graph output;
- latest checkpoint metadata.

For calls mode, the script prints:

- execution id;
- status (`completed`);
- a JSON-safe summary of the fake LangChain messages;
- the final fake-model message;
- expected call checkpoint name prefixes.

For the conceptual walkthrough, see the guide:
[LangGraph Adapter](https://kitaru.ai/docs/guides/langgraph-adapter).

For the full catalog, see [../../README.md](../../README.md).
