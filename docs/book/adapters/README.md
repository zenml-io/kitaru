---
description: One wrapper, no rewrite — adapters record your existing agent's runs as replayable sessions.
icon: puzzle-piece
---

# Adapters

Adapters are the first of two ways into Kitaru: wrap the agent you
already have, and every run is recorded natively. (The second —
[importing the traces you already collect](../getting-started/import-your-traces.md) —
needs no adapter at all.)

An adapter changes nothing about how your agent thinks. Your framework
still runs it — decides how it calls tools, streams, retries — while the
adapter observes the seams and records each model request and tool call
as a node on a [session](../concepts/agents-and-sessions.md). The same
adapter is what makes [replay](../concepts/replay.md) work: under a
replay it applies the override at the model boundary and answers tool
calls per the [tool policy](../guides/tool-policies.md), so recording and
replaying are two modes of one wrapper.

## Available adapters

| Framework | Adapter | Status |
|---|---|---|
| [PydanticAI](pydantic-ai.md) | `kitaru.adapters.pydantic_ai.KitaruAgent` | Shipped |

<!-- TODO(v2-launch): confirm the adapter list shipping in v2.0. The v1
     adapters (OpenAI Agents SDK, Claude Agent SDK, LangGraph, Gemini,
     Google ADK) have not been ported to the v2 recording API yet. -->

More adapters are on the way; the v1 line of Kitaru shipped six, and they
are being ported to the v2 recording API. If your framework isn't covered
yet, you have two options today:

* **Import** — your framework already emits traces to Langfuse or
  OpenTelemetry? [Import them](../getting-started/import-your-traces.md);
  sessions from imports are replayable and scorable like any other.
* **Record directly** — the recording API is small: create a session,
  ingest nodes. `client.sessions.create(...)` and
  `client.sessions.ingest_nodes(...)` are all an adapter does, and they're
  yours to call from any Python agent.

## Why the wrapper is enough

The claim "one wrapper, no rewrite" has a precise meaning: the adapter is
a transparent wrapper around your agent object. Same constructor, same
`run` / `run_sync`, same output types — your tests and callers don't know
it's there. What you gain is that every run leaves a recording, and that
the same script doubles as the replay target: under a
[worker](../concepts/workers.md), the adapter reads the task environment,
substitutes the recorded inputs, and applies the replay configuration
automatically. Your entrypoint doesn't branch on "am I replaying"; the
adapter does.
