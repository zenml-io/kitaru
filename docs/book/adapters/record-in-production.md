---
description: "Take an adapter-wrapped agent to production: register it, give the service its own credential, and know exactly what recording does to your request path."
icon: tower-broadcast
---

# Record in production

An adapter in production means every real request lands in Kitaru as a [session](../concepts/agents-and-sessions.md) at the moment it happens. There is no export cron, no nightly reconciliation, and no format conversion, because the recording is not a translation of a trace: it is the run itself, written by the same wrapper that will later execute [replays](../concepts/replay.md) of it.

That last point is the one worth internalizing. The wrapper you install today is the thing that answers tool calls and applies overrides when a [worker](../concepts/workers.md) replays a session tomorrow. Recording and replay are one integration, not two.

You do not have to choose between the two entry paths. [Import the backlog you already have](../getting-started/import-your-traces.md) to get a population worth evaluating this week, and add the adapter with your next deploy so the population keeps growing on its own.

## Three wiring steps

### 1. Register the agent

An agent is the identity your sessions hang off. Register it once:

```bash
kitaru agent register support-agent --command "python support.py"
```

Registration creates the agent and its first version. A version pins the run specification Kitaru needs to execute your code later: the `--command`, `--working-dir`, `--env KEY=VALUE` pairs, `--secret-id` references, and `--timeout-seconds`. Register a new version whenever that specification changes:

```bash
kitaru agent version register support-agent \
  --command "python support.py" \
  --working-dir /srv/support \
  --env KITARU_AGENT_ID="$KITARU_AGENT_ID"
```

Recording itself only needs the agent ID. The run specification matters because a replay re-runs your real code, and the version is where Kitaru learns how.

{% hint style="info" %}
Pass `agent_version_id` instead of `agent_id` when you want sessions attributed to one specific version, which is what makes "did the change help?" answerable later. Retrieve either with `kitaru --output json agent get support-agent | jq -r '.item.id'`.
{% endhint %}

### 2. Give the service its own credential

The adapter does not carry a connection of its own. It builds a `KitaruAPIClient`, which resolves the server from `KITARU_API_URL` (falling back to the URL stored by `kitaru login`) and the credential in this order: the task token a worker injects (`KITARU_API_TOKEN`), then `KITARU_API_KEY`, then stored login credentials.

In production, set both explicitly:

```bash
export KITARU_API_URL="https://kitaru.internal.example.com"
export KITARU_API_KEY="KITKEY_..."
export KITARU_AGENT_ID="..."
```

Issue a dedicated key per service so it can be rotated and revoked without touching anything else. Never copy a developer's stored `kitaru login` credential into a container image or CI secret. See [Authentication & API keys](../deploy/authentication.md) for issuing, rotating with a grace window, and deactivating keys.

Because the credential resolution order puts the worker's task token first, the same process image works unmodified on a laptop, in production, and under a worker executing a replay.

### 3. Wrap the agent

```python
import os
import uuid

from pydantic_ai import Agent
from kitaru_pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5.4", name="support-agent",
              system_prompt="You resolve support tickets.")

support = KitaruAgent(agent, agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]))

result = support.run_sync("Refund order #4821.")
```

The wrapper returns your framework's native result type, so nothing downstream changes. Each adapter page documents its exact boundary and constructor: [PydanticAI](pydantic-ai.md), [LangGraph](langgraph.md), [OpenAI Agents SDK](openai-agents.md), [Mastra](mastra.md), [Vercel AI SDK](vercel-ai.md).

## What recording costs in the request path

This is the part to read carefully before a production rollout. Recording is **in-band**: every adapter creates the session with an awaited HTTP call before your agent runs, and none of them degrade to an unrecorded run.

{% hint style="warning" %}
If the Kitaru server is unreachable when a request starts, the run raises before the agent executes. Not one provider call is made, and nothing is silently dropped. Treat Kitaru server availability as a dependency of the agent path, the same way you treat your model provider.
{% endhint %}

Mid-run behavior is where the adapters genuinely differ, and the difference decides whether a Kitaru outage costs you a request or costs you only a recording.

| Adapter | Node writes during the run | Recording failure after the agent produced a result |
| --- | --- | --- |
| [LangGraph](langgraph.md) | Buffered, flushed at `batch_size` (default 20) | Contained. The graph result or exception is preserved and a single structured warning is logged |
| [OpenAI Agents SDK](openai-agents.md) | None during the run; observations are collected in memory and written after the SDK returns | Raises `KitaruRecordingError`, whose `result` field carries the native `RunResult` |
| [PydanticAI](pydantic-ai.md) | Buffered, but a flush at `batch_size` is awaited inline inside the run | The result is lost. A failing final flush or session update propagates out of the run |
| [Mastra](mastra.md) | One awaited write per completed step | The result is lost. Completion failure discards the successful Mastra result and rethrows |
| [Vercel AI SDK](vercel-ai.md) | One awaited write per step, no batching | The result is lost. Completion failure discards the native result and rethrows |

Read that table as three tiers of exposure:

- **LangGraph is the only adapter that fails open after the graph starts.** Once graph delegation begins, adapter-owned recording failures are latched, all later writes short-circuit, and finalization is time-bounded. Your caller gets the graph's real answer. One caveat: under a worker, a task whose result session cannot be completed still fails, because the worker requires a completed result session.
- **The OpenAI Agents adapter loses the request but not the answer.** `KitaruRecordingError` preserves the native `RunResult` on its `result` attribute, along with `session_id` and the `phase` that failed. It also sets `retry_safe=False` and `side_effects_possible=True`, so catch it and read `err.result` rather than re-running the agent, which would duplicate tool side effects.
- **PydanticAI, Mastra, and the Vercel AI SDK adapter propagate recording failures to your caller.** A successful agent result is discarded if the final write fails. In the Mastra and Vercel adapters a step write is awaited inline, so each step adds a round trip to the Kitaru server to your request latency, and a mid-run write failure aborts the generation loop.

If you are running one of those three behind a user-facing request, put the wrapped call behind your own retry-and-fallback boundary, and keep the Kitaru server close to the agent on the network.

## Redaction before payloads leave the process

Recorded prompts, tool arguments, and tool results are your application's real data. Kitaru is self-hosted, so it never leaves your infrastructure, but the session store still inherits whatever the agent handled.

Only the **LangGraph** adapter exposes a configurable policy. `CapturePolicy` transforms the copies sent to Kitaru and never touches the values passed to or returned by LangGraph:

```python
from kitaru_langgraph import CapturePolicy, KitaruGraphRunner

runner = KitaruGraphRunner(
    graph,
    agent_id=agent_id,
    capture_policy=CapturePolicy(redactor=strip_customer_pii),
)
```

Alongside the custom `redactor`, `CapturePolicy` carries the per-invocation bounds (`max_child_nodes`, `max_field_bytes`, `max_buffer_bytes`, `max_depth`, `max_collection_items`) and a built-in recursive key redactor for common credential field names. Hitting a bound marks the recording lossy and truncates only the stored copy; the graph outcome is preserved.

The other adapters have no user-supplied redaction hook:

- **OpenAI Agents SDK** applies fixed size, depth, and collection limits with truncation metadata, and excludes caller context, clients, credentials, callbacks, and private SDK fields.
- **Mastra** and the **Vercel AI SDK** adapter replace credential-shaped keys (`authorization`, `token`, `secret`, `password`, `api_key`, `apikey`, `cookie`) with a redaction marker and bound oversized values.
- **PydanticAI** applies no redaction and no size bounds to recorded payloads. Prompts and tool payloads are serialized as-is.

A key-name redactor is a safety net, not a data classifier. Sensitive values under names it cannot recognize, and free text inside prompts, still reach the server. Where the data is regulated, redact in your own tool and prompt construction, and apply the same access and retention rules to Kitaru sessions that you apply to the original payloads.

## What you do not need in production

- **No worker.** Workers execute replays, imports, and evaluations. Recording is a direct authenticated HTTP call from your process to the server API. Run workers where you run offline analysis, not in the request path.
- **No second observability system to replace.** The adapter composes with your existing tracing; PydanticAI's OpenTelemetry instrumentation keeps working while Kitaru records the same run.
- **No data leaving your infrastructure.** The Kitaru server is self-hosted, so sessions live where you deploy it.

## Verify it is recording

Send one real request, then look for the session:

```bash
kitaru session list --agent support-agent --origin recorded --size 5
```

A live-recorded session has `origin: recorded`. Filter further with `--status`, `--started-after`, or `--tag`. A run whose process died mid-way stays `in_progress` with everything written so far, which is exactly the evidence you want from a crash.

## Next

- [Agents and sessions](../concepts/agents-and-sessions.md) explains what a session contains and how versions relate to it.
- [Write an evaluator](../guides/write-an-evaluator.md) turns the sessions you are now recording into a quality signal.
- [Build a regression suite from production](../guides/regression-suite.md) freezes the ones that matter into a cohort you can replay against every change.
