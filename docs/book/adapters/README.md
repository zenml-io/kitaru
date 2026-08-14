---
description: One wrapper, no rewrite — adapters record your existing agent's runs as replayable sessions.
icon: puzzle-piece
---

# Adapters

Adapters are the first of two ways into Kitaru: wrap the agent you already have, and every run is recorded natively. (The second — [importing the traces you already collect](../getting-started/import-your-traces.md) — needs no adapter at all.)

An adapter leaves your framework in charge of the agent loop while recording the model and tool activity that the integration exposes. The same adapter makes [replay](../concepts/replay.md) work: it applies supported overrides at the model boundary and answers tool calls per the [tool policy](../guides/tool-policies.md). Capabilities differ by integration. The current TypeScript adapters record non-streaming calls only; the Vercel adapter also preserves Agent `stream()` as a native, recording-free passthrough. Each adapter page states its exact boundary.

## Available adapters

Each adapter ships as its own distribution, installed alongside Kitaru in the agent's environment.

### Python

| Framework | Install | Entry point | Records | Replays |
| --- | --- | --- | --- | --- |
| [PydanticAI](pydantic-ai.md) | `kitaru-pydantic-ai` | `kitaru_pydantic_ai.KitaruAgent` | Yes | Yes |
| [LangGraph](langgraph.md) | `kitaru-langgraph` | `kitaru_langgraph.KitaruGraphRunner` | Yes | Depends on construction |
| [OpenAI Agents SDK](openai-agents.md) | `kitaru-openai-agents` | `kitaru_openai_agents.KitaruRunner` | Yes | Yes |

LangChain agents and Deep Agents use the LangGraph adapter — their public factories return LangGraph runnables. What the LangGraph adapter can replay depends on how the graph was constructed; its [capability matrix](langgraph.md#capability-matrix) is the reference.

### TypeScript

| Framework | Install | Entry point |
|---|---|---|
| [Vercel AI SDK](vercel-ai.md) | `@zenml-io/kitaru-vercel-ai` | `createKitaruToolLoopAgent`, `createKitaruGenerateText` |
| [Mastra](mastra.md) | `@zenml-io/kitaru-mastra` | `KitaruAgent` |

Both build on `@zenml-io/kitaru`, the framework-neutral TypeScript client and adapter foundation. Its public client uses methods such as `createSession(...)` and `upsertSessionNodes(...)`; it deliberately does not provide a framework-neutral agent or streaming abstraction.

If your framework isn't covered, see [No adapter for your framework](custom.md) for three options available today:

- **Import** — your framework already emits traces to Langfuse, LangSmith, or Braintrust? [Import them](../getting-started/import-your-traces.md); sessions from imports can be replayed and evaluated like any other. Convert any other format to [Kitaru JSONL](../guides/importing-sessions.md).
- **Record directly** — create a session and ingest its nodes with the Python or TypeScript client. The `kitaru-adapter-builder` [agent skill](../agent-native/skills.md) will write that integration with you.
- **Hand the run back to your own system** — register the agent version as a function instead of a command, and Kitaru calls you to run it. See [Let Kitaru call your agent](custom.md#let-kitaru-call-your-agent).

## Why the wrapper is enough

"One wrapper, no rewrite" means you keep the framework's agent loop and native result types. The Python adapters expose framework-shaped runner or agent objects. Mastra adds a `KitaruAgent(existingAgent, options)` with `generate(...)`; the Vercel AI SDK adapter returns either a public AI SDK Agent or a native-signature `generateText(...)` function. Under a [worker](../concepts/workers.md), the same entrypoint reads the replay task environment, substitutes the recorded inputs, and applies the supported replay configuration. Your application does not need a separate replay branch.
