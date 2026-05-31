# LangGraph adapter example

This example shows how to run LangGraph and LangChain work inside a Kitaru flow.

The basic story is simple:

1. Your Python script starts a normal Kitaru flow.
2. Inside that flow, the code runs a LangGraph graph or a LangChain agent.
3. Kitaru records what happened as checkpoints and artifacts, so you can inspect the run later in the Kitaru dashboard or through the SDK.

That matters because LangGraph already has its own idea of state: threads, checkpointers, interrupts, and resumes. Kitaru should not barge in and pretend to own all of that. Instead, the adapter gives you two safe ways to draw the boundary between the Kitaru world and the LangGraph world.

Think of the two modes as two different camera positions:

- `graph_call` films the whole LangGraph box from the outside. Kitaru records, "I called this graph with this thread id; it interrupted or completed; here is the latest graph checkpoint metadata." This is the safest first mode and needs no model provider API key.
- `calls` puts a smaller camera at the LangChain model/tool doorway. Kitaru records the actual model call and local tool calls as their own checkpoints. This mode uses a real OpenAI-backed LangChain agent, while the tools stay deterministic local Python functions.

In other words: start with `graph_call` if you want to see Kitaru wrap a LangGraph run. Try `calls` when you want to see Kitaru checkpoint the model/tool calls inside a LangChain agent.

This README is enough to run the example and know what to look for. For the fuller design explanation, see the [LangGraph Adapter guide](https://kitaru.ai/docs/guides/langgraph-adapter).

## The runs in this example

- `--strategy graph_call` — local interrupt/resume graph with one Kitaru checkpoint around each outer LangGraph invocation. No provider API key is needed.
- `langgraph_streaming.py` — local graph-call streaming demo. It emits LangGraph `custom` and `updates` stream events while returning a durable `LangGraphRunResult`. No provider API key is needed.
- `--strategy calls` — real OpenAI-backed LangChain support agent with Kitaru checkpoints around synchronous model/tool handler calls. Requires `OPENAI_API_KEY`.

## Run it

From the repository root, install the dependencies for the strategy you want.

For the local `graph_call` demo:

```bash
uv sync --extra local --extra langgraph
uv run kitaru init
uv run kitaru login
uv run examples/integrations/langgraph_agent/langgraph_adapter.py --strategy graph_call
```

For the local streaming demo:

```bash
uv sync --extra local --extra langgraph
uv run kitaru init
uv run kitaru login
uv run examples/integrations/langgraph_agent/langgraph_streaming.py
```

For the OpenAI-backed `calls` demo:

```bash
uv sync --extra local --extra langgraph-openai
uv run kitaru init
uv run kitaru login
export OPENAI_API_KEY='sk-...'
# Optional: override the default gpt-5-nano model.
export LANGGRAPH_AGENT_MODEL='gpt-5-nano'
uv run examples/integrations/langgraph_agent/langgraph_adapter.py --strategy calls
```

From this directory, the script path is shorter.

After the local `graph_call` setup above, run:

```bash
cd examples/integrations/langgraph_agent
uv run langgraph_adapter.py --strategy graph_call
```

For the local streaming setup above, run:

```bash
cd examples/integrations/langgraph_agent
uv run langgraph_streaming.py
```

After the OpenAI-backed `calls` setup above, including `OPENAI_API_KEY`, run:

```bash
cd examples/integrations/langgraph_agent
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

## Streaming demo: live LangGraph progress, durable result

The streaming demo runs a local `StateGraph` through `runner.stream(...)` inside a Kitaru flow. It does not call an LLM and does not need `OPENAI_API_KEY`.

The story is intentionally small:

1. The graph looks up `ticket-42` from a local Python dictionary.
2. The first node emits custom progress with LangGraph's stream writer.
3. The second node drafts a handoff note and emits another custom progress event.
4. Kitaru publishes safe `langgraph.stream.*` live events while the outer graph-call checkpoint is running.
5. After the stream is drained, the flow returns the durable `LangGraphRunResult`.

What to expect in the terminal:

- the submitted Kitaru execution id;
- a `live LangGraph stream events` section when the connected backend supports event watching;
- a graceful "live event watching is unavailable" note if the backend cannot stream events;
- a final `durable LangGraphRunResult` section with `status: completed`, the `thread_id`, latest LangGraph checkpoint metadata, and the final graph state.

The important boundary is the same as normal `graph_call`: live events are progress postcards, not replay checkpoints. Kitaru saves the final `LangGraphRunResult` as the durable value. If the graph-call checkpoint is replayed, live events may be emitted again. If a cached checkpoint result is reused, fresh live events may not appear because the graph body did not run.

## `calls` mode: LangChain model/tool checkpoints

The calls demo builds a LangChain agent with:

- a real OpenAI chat model through `langchain-openai`;
- the default model `gpt-5-nano`, overridable with `LANGGRAPH_AGENT_MODEL`;
- local deterministic `lookup_ticket(...)` and `approve_ticket(...)` tools;
- `KitaruLangGraphMiddleware` installed as LangChain middleware;
- `KitaruGraphRunner(..., checkpoint_strategy="calls")`.

The story is deliberately small and inspectable:

1. The user asks the agent to handle `ticket-42`.
2. The model is instructed to call `lookup_ticket` first.
3. The local tool returns stable ticket details: status, priority, customer, and issue.
4. If the status is `needs_escalation`, the model is instructed to call `approve_ticket`.
5. The final answer summarizes the ticket status, approval result, and next step.

The model is real, so its exact wording and exact tool sequence may vary. The tools are local and deterministic, so the side-effect story is still easy to understand.

What to expect in Kitaru:

- one flow execution;
- model-call checkpoints with names beginning `model_call__...`;
- a ticket lookup checkpoint with a name beginning `tool_call__lookup_ticket_...` when the model follows the lookup instruction;
- an approval checkpoint with a name beginning `tool_call__approve_ticket_...` when the model follows the escalation instruction for a ticket that needs escalation;
- one calls-mode summary checkpoint with a name beginning `langgraph_summary__langgraph_local_calls_demo_...`;
- event/run-summary artifacts, with names like
  `event_log__langgraph_local_calls_demo_...` and
  `run_summary__langgraph_local_calls_demo_...`;
- one user-facing summary artifact: `summary__langgraph_demo`.

This is `checkpoint_strategy="calls"`. Kitaru is not splitting open arbitrary LangGraph nodes. It can create true call checkpoints because the middleware is physically wrapped around the synchronous LangChain model/tool handlers.

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

For streaming mode, the script prints:

- execution id;
- best-effort `langgraph.stream.*` live events, if the backend supports watching them;
- status (`completed`);
- thread id;
- latest checkpoint metadata;
- final graph output.

For calls mode, the script prints:

- execution id;
- model name;
- status (`completed`);
- a JSON-safe summary of the LangChain messages;
- the final model message;
- typical and model-dependent call checkpoint name prefixes.

For the conceptual walkthrough, see the guide:
[LangGraph Adapter](https://kitaru.ai/docs/guides/langgraph-adapter).

For the full catalog, see [../../README.md](../../README.md).
