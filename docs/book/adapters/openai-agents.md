---
description: Record and replay non-streaming OpenAI Agents SDK runs with the separately packaged Kitaru v2 adapter
icon: robot
---

# OpenAI Agents SDK

The Kitaru v2 OpenAI Agents adapter records a native OpenAI Agents SDK run as one Kitaru session. Model calls, tools, hosted tools, and handoffs appear as child nodes inside that session. The OpenAI SDK still executes the agent and returns its own result object.

{% hint style="warning" %}
This adapter ships as its own separately versioned distribution, `kitaru-openai-agents` (`uv add kitaru-openai-agents`); it is not exported by the installed `kitaru` package. Do not import it from `kitaru.adapters`.
{% endhint %}

## Run an agent

Install the adapter package:

```bash
uv add kitaru-openai-agents
```

In a Kitaru repository checkout, sync the plugin workspace instead:

```bash
uv sync --project plugins --all-packages
```

Create the OpenAI agent as usual, then pass it to `KitaruRunner.run(...)` or `KitaruRunner.run_sync(...)`:

```python
import uuid

from agents import Agent

from kitaru_openai_agents import KitaruRunner

agent = Agent(
    name="support_agent",
    instructions="Answer briefly and accurately.",
    model="gpt-5-nano",
)
runner = KitaruRunner(agent_id=uuid.UUID("00000000-0000-0000-0000-000000000001"))

result = runner.run_sync(agent, "Where is my order?")
print(result.final_output)
```

Use `await runner.run(...)` in asynchronous code. `run_sync(...)` must not be called while an event loop is already running.

Both methods return the exact native OpenAI `RunResult`. Kitaru does not replace it with a custom result type, and it preserves caller-owned agents, hooks, context, OpenAI SDK sessions, run configuration, and response or conversation identifiers unless a worker-managed replay overrides the corresponding supported value.

## Kitaru identity

A standalone run must set either `agent_id` or `agent_version_id` on `KitaruRunner`. You can also set `session_name` and `batch_size`:

```python
runner = KitaruRunner(
    agent_version_id=agent_version_id,
    session_name="support-request",
    batch_size=20,
)
```

A Kitaru worker provides `KITARU_TASK_ID` for task-bound runs. In that case, the server links the result session to the task and infers the agent identity, so the runner does not need either identity argument.

## Correlate the native result with its session

Pass a sync or async `session_observer` when constructing `KitaruRunner`. Kitaru calls it with the new `SessionResponse` after the session and its root node exist. The callback can retain the session ID, then the caller can associate the exact native result returned later with that Kitaru session:

```python
session_ids: list[uuid.UUID] = []


def remember_session(session) -> None:
    session_ids.append(session.id)


runner = KitaruRunner(
    agent_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
    session_observer=remember_session,
)
result = runner.run_sync(agent, "Where is my order?")
print(result.final_output, session_ids[0])
```

Define `remember_session` with `async def` when it needs to await other work. The observer runs before OpenAI executes the agent, so it identifies the session even if the native run later fails.

## What Kitaru records

Before OpenAI executes the agent, Kitaru creates one session and an in-progress root node. As the run proceeds, it records structured observations for supported model calls, direct function-tool calls, provider-hosted tools, handoffs, token usage, final output, and failures. It completes the session only after all observations have been persisted.

These nodes are observations of what the OpenAI SDK run did. They are not independently replayable units, and they do not change how the SDK runs the agent.

## Replay behavior

Replay selection is worker-managed through `KITARU_REPLAY_ID`. `KitaruRunner.run(...)` and `run_sync(...)` have no per-run replay argument. Use the Kitaru replay and task flow, which starts the agent task with the selected replay, rather than mutating the process environment around concurrent standalone calls. Environment variables are process-wide, so one concurrent call could otherwise read another call's replay ID.

For the selected replay, the adapter can replace the root input, starting-agent instructions, run-level model, and model settings without mutating the caller's objects.

Direct `FunctionTool` replay supports two policies:

- **Passthrough:** call the original tool. This is the default.
- **Static:** return the recorded or configured static value without calling the original tool.

Static substitution must match one ordinary, direct, enabled, non-approval `FunctionTool` on the starting agent. Unsupported, ambiguous, duplicate, or unmatched tool policies fail before Kitaru creates a session or OpenAI calls the model. History, LLM, hosted, MCP, programmatic, agent-as-tool, handoff-target, and unknown tool substitutions are rejected.

## Deliberate exclusions

This first v2 adapter release does not provide:

- `run_streamed` or token and event streaming
- per-call or mid-run replay from recorded observation nodes
- a Kitaru sandbox helper for OpenAI tools
- an adapter-specific request or result envelope
- `RunState` input or durable approval interruption and resume
- adapter-specific CLI, MCP, or server support

Approval interruptions fail closed instead of completing the Kitaru session. `RunState` input is rejected because Kitaru v2 does not yet have a durable interrupted-session state to resume.

## Exceptions after OpenAI starts

The adapter exposes two public exceptions in `kitaru_openai_agents` for failures after OpenAI starts:

- `KitaruRecordingError` means OpenAI produced a native result but Kitaru failed while reconciling observations, finalizing the session, or closing the client. Its `result` field preserves that `RunResult`; `session_id` identifies the Kitaru session when available; and `phase` names the failed recording phase. It sets `retry_safe=False` and `side_effects_possible=True`, because automatically running the model or tools again could duplicate work or side effects.
- `UnsupportedInterruptionError` means OpenAI returned an approval interruption that this adapter cannot durably resume. Its `result` field preserves the interrupted native `RunResult`.

## Data safety

Kitaru recording is independent of OpenAI tracing. Disabling OpenAI tracing does not disable Kitaru session and node recording.

The adapter excludes caller context, clients, credentials, environment state, callbacks, OpenAI SDK session objects, private SDK fields, provider-internal reasoning, and unknown-object serialization. Recorded values use deterministic size, depth, and collection limits with truncation metadata.

Effective prompts, tool arguments, tool results, and exception summaries can still contain sensitive application data. Review what your application sends to models and tools, and apply the same access controls and retention policy to Kitaru data that you apply to the original application payloads.

## Runnable example

The repository example has a no-cost help check and an opt-in real run:

```bash
uv run --project plugins python -m examples.python.openai_agents_v2.agent --help

export OPENAI_API_KEY="..."
export KITARU_AGENT_ID="..."
uv run --project plugins python -m examples.python.openai_agents_v2.agent \
  "Use the tool to check order ORD-1007"
```

See [`examples/python/openai_agents_v2/README.md`](https://github.com/zenml-io/kitaru/tree/develop/examples/python/openai_agents_v2) for setup details.
