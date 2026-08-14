---
description: One wrapper, no rewrite — adapters record your existing agent's runs as replayable sessions.
icon: puzzle-piece
---

# Adapters

Adapters are the first of two ways into Kitaru: wrap the agent you already have, and every run is recorded natively. (The second — [importing the traces you already collect](../getting-started/import-your-traces.md) — needs no adapter at all.)

An adapter changes nothing about how your agent thinks. Your framework still runs it — decides how it calls tools, streams, retries — while the adapter observes the seams and records each model request and tool call as a node on a [session](../concepts/agents-and-sessions.md). The same adapter is what makes [replay](../concepts/replay.md) work: under a replay it applies the override at the model boundary and answers tool calls per the [tool policy](../guides/tool-policies.md), so recording and replaying are two modes of one wrapper.

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

<!-- TODO(v2-launch): TypeScript support lands with #679 (feat/ts-support),
     open but unmerged at the time of writing, at 0.1.0-rc.1. Confirm the
     published package names and entry points before publish. -->

| Framework | Install | Entry point |
|---|---|---|
| Vercel AI SDK | `@zenml-io/kitaru-vercel-ai` | `createKitaruGenerateText` |
| Mastra | `@zenml-io/kitaru-mastra` | `KitaruAgent` |

Both build on `@zenml-io/kitaru`, the TypeScript SDK and adapter
foundation, which you can also record against directly the same way the
Python SDK is used below.

<!-- TODO(v2-launch): the Claude Agent SDK, Gemini and Google ADK adapters
     from the v1 line are still unported to the v2 recording API — their
     pages exist on develop but describe the v1 surface. Confirm whether
     any of them make v2.0 before publish. -->

More adapters are on the way; the v1 line of Kitaru shipped six, and the rest are being ported to the v2 recording API. If your framework isn't covered yet you have three options today, none of which involve waiting for us — see [No adapter for your framework](custom.md) for the full comparison:

- **Import** — your framework already emits traces to Langfuse, LangSmith, Braintrust, or OpenTelemetry? [Import them](../getting-started/import-your-traces.md); sessions from imports can be replayed and evaluated like any other. Any other format converts to [Kitaru JSONL or OTLP](../guides/importing-sessions.md).
- **Record directly** — the recording API is small: create a session, ingest nodes. `client.sessions.create(...)` and `client.sessions.ingest_nodes(...)` are all an adapter does, and they're yours to call from any Python agent. The `kitaru-adapter-builder` [agent skill](../agent-native/skills.md) will write that wrapper for you.
- **Hand the run back to your own system** — register the agent version as a function instead of a command, and Kitaru calls you to run it. See [Let Kitaru call your agent](custom.md#let-kitaru-call-your-agent).

## Why the wrapper is enough

The claim "one wrapper, no rewrite" has a precise meaning: the adapter is a transparent wrapper around your agent object. Same constructor, same `run` / `run_sync`, same output types — your tests and callers don't know it's there. What you gain is that every run leaves a recording, and that the same script doubles as the replay target: under a [worker](../concepts/workers.md), the adapter reads the task environment, substitutes the recorded inputs, and applies the replay configuration automatically. Your entrypoint doesn't branch on "am I replaying"; the adapter does.
