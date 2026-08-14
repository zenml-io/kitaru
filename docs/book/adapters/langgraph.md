---
description: Record LangGraph, LangChain, and Deep Agents invocations with the kitaru-langgraph adapter package
icon: diagram-project
---

# LangGraph Adapter

`KitaruGraphRunner` records supported `invoke()` and `ainvoke()` calls as Kitaru v2 sessions. It wraps an already compiled LangGraph runnable without recompiling it, replacing its checkpointer, or changing the graph result. LangChain agents and Deep Agents use the same adapter because their public factories return LangGraph runnables.

The adapter ships as the installable `kitaru-langgraph` distribution with the `kitaru_langgraph` import package. Install it directly in the agent environment:

```bash
uv add kitaru-langgraph
```

Deep Agents support is an optional extra; the `langchain.agents.create_agent` factory and direct graph wrapping work without it, and constructing a runner through `deepagents.create_deep_agent` requires it:

```bash
uv add "kitaru-langgraph[deepagents]"
```

Model-provider packages are not bundled. An `init_chat_model` string like `"openai:gpt-5-nano"` needs the matching LangChain provider package, for example `langchain-openai`, installed by you.

## Capability matrix

Check the construction path before requesting replay behavior. Unsupported operations fail before the graph runs.

| Construction | Invocation recording | Whole-input replacement | Model-request overrides | Tool-result substitution | Nested coverage |
| --- | --: | --: | --: | --: | --- |
| Direct compiled graph wrapper | Yes | Yes | No | No | Public callbacks observed by the outer run |
| `langchain.agents.create_agent` factory | Yes | Yes | Yes, with one live model call | Yes, for supported static or history results | Main agent and observable descendants |
| `deepagents.create_deep_agent` factory | Yes | Yes | Yes, with one live model call | Yes, for supported static or history results | Main agent and explicit Kitaru-built local subagents |
| Opaque compiled or remote subagent | Included in the outer result | No separate capability | No | No | Reported as opaque when it has a stable public category |

Use `runner.capabilities` to inspect the immutable view produced by the adapter. A direct wrapper reports only recording and whole-input replacement. Factory construction reports only capabilities attached to middleware that Kitaru actually injected.

## Record a compiled graph

Import the runner from the installed package:

```python
from typing import NotRequired, TypedDict

from langgraph.graph import END, START, StateGraph

from kitaru_langgraph import KitaruGraphRunner


class SupportState(TypedDict):
    request: str
    normalized_request: NotRequired[str]


def normalize(state: SupportState) -> dict[str, str]:
    return {"normalized_request": " ".join(state["request"].split())}


builder = StateGraph(SupportState)
builder.add_node("normalize", normalize)
builder.add_edge(START, "normalize")
builder.add_edge("normalize", END)

runner = KitaruGraphRunner(builder.compile(), agent_id=agent_id)
result = runner.invoke({"request": "  Reset   my password  "})
```

Kitaru creates the session and its root node before the graph starts. The session records bounded copies of the effective input and final output or error, plus public chain, graph, model, and tool callbacks that LangGraph exposes during the call. Ordinary Python calls and provider SDK calls that emit no public callback do not get invented child nodes.

The wrapper returns the exact graph value or raises the exact graph exception. Caller config, callbacks, tags, metadata, configurable values, thread ID, store, and checkpointer behavior remain with LangGraph. If a Kitaru task supplies task inputs, those replace the whole graph input; a caller `Command`, including `Command(resume=...)`, always takes precedence.

Run the complete provider-free example from the repository root:

```bash
uv sync --project plugins --all-packages
uv run --project plugins python -m examples.integrations.langgraph_v2
```

The local command needs an existing `KITARU_AGENT_ID` or `KITARU_AGENT_VERSION_ID` and a configured Kitaru v2 connection. It does not need a model-provider key or replay setup. See the example [README](https://github.com/zenml-io/kitaru/tree/develop/examples/integrations/langgraph_v2) for the full setup.

## Apply live model-request overrides

Model, prompt, system-prompt, and model-parameter overrides require construction through one of the two accepted public factories:

```python
from langchain.agents import create_agent

from kitaru_langgraph import KitaruGraphRunner

runner = KitaruGraphRunner.from_agent_factory(
    create_agent,
    factory_kwargs={
        "model": "openai:gpt-5.4-mini",
        "tools": [lookup_order],
    },
    agent_id=agent_id,
)
```

The factory path inserts Kitaru middleware before the agent is compiled. During a Kitaru replay, the middleware builds the effective model request from the replay override, then calls that live model exactly once. Changing the model, prompt, system prompt, or model parameters never reuses a stored model response and never reduces the model-call count to zero. A mapped model override requires the original `factory_kwargs["model"]` to be a string identifier because that exact string selects the replacement. If the factory receives an already constructed model object, use a direct replacement instead of a mapping. Prompt replacement targets the factory-built agent's message state; direct compiled graph wrappers reject prompt overrides instead of guessing a state field.

`from_agent_factory()` accepts the exact public `langchain.agents.create_agent` or `deepagents.create_deep_agent` factory objects. The `factory` in each `LocalSubagentFactorySpec` has the same exact-object restriction; wrappers and other compatible callables are rejected because Kitaru cannot prove which middleware they install. For Deep Agents, the spec lets Kitaru build named local subagents before the parent and report each one separately. Caller-supplied compiled, remote, or framework-created children remain opaque and do not gain model or tool substitution capabilities from the outer wrapper.

## Substitute supported tool results

Factory construction also installs public tool middleware. During a Kitaru replay, a matching static result or valid recorded-history result becomes a framework-valid `ToolMessage` or `Command` with the current tool-call identity. That hit is the only adapter path that skips a live dependency: the live tool is called zero times.

Misses follow the replay policy without silent fallback:

- `fail` raises before a live tool call.
- `error_result` returns a tool error result without a live tool call.
- `passthrough` calls the live tool exactly once.

Malformed, unsupported, or lossy recorded results count as misses. The adapter rejects the `llm` tool policy. It does not substitute stored model responses.

## Interrupts and unsupported invocation modes

For a direct call outside a Kitaru worker, a public LangGraph interrupt result is returned unchanged and the session is recorded as completed with interruption metadata. Resume the same LangGraph thread with your existing checkpointer and `Command(resume=...)`; the resume call becomes a second Kitaru session. Kitaru never reads or replaces private checkpointer state.

Worker-managed interrupt scheduling and resume are not supported. If a worker invocation returns an interrupt, the adapter records the partial session as interrupted and failed, then raises `UnsupportedWorkerInterruptError` so the task cannot appear complete.

The v2 adapter is non-streaming. `stream()`, `astream()`, `astream_events()`, `astream_log()`, `batch()`, `abatch()`, `batch_as_completed()`, and `abatch_as_completed()` raise `UnsupportedInvocationError` before session creation or graph execution. Use `invoke()` or `ainvoke()`.

## Capture safety and limits

`CapturePolicy` transforms copies sent to Kitaru. It never changes values passed to or returned by LangGraph. The built-in recursive key redactor matches common credential fields case-insensitively, including authorization headers, API keys, passwords, secrets, access and refresh tokens, and cookies. You can supply a final custom `redactor` for application fields.

Prompts, graph state, tool arguments, tool results, outputs, errors, and arbitrary free text can still contain sensitive data under names the key redactor cannot recognize. Apply a custom redactor where needed and use Kitaru access and retention controls for stored sessions.

The default per-invocation bounds are:

| Limit                | Default |
| -------------------- | ------: |
| Child nodes          |  10,000 |
| One UTF-8 JSON field | 256 KiB |
| Buffered node data   |  16 MiB |
| Capture depth        |      20 |
| Items per collection |   1,000 |

Limit hits or serialization failures mark capture as lossy, truncate or drop only the stored copy, and preserve the graph outcome. Lossy tool arguments or results are not eligible for history substitution.

## Recording failures

Session and root-node setup must succeed before the graph starts. After graph delegation begins, adapter-owned recording failures are contained in that invocation. The direct runner preserves the graph result or exception and attempts one private, structured local warning with the failed stage and exception class, without captured payloads or exception text. A worker task can still fail if its linked result session cannot be completed.

## Migrate from the v1 adapter

The v2 adapter is a smaller recording and replay boundary, not a port of the v1 execution system.

| v1 capability | v2 status |
| --- | --- |
| Record one graph invocation | Use `KitaruGraphRunner.invoke()` or `ainvoke()`; each invocation is one Kitaru session |
| Middleware-observed model and tool calls | Use `from_agent_factory()` with LangChain or Deep Agents |
| Graph-call versus calls checkpoint strategies | Removed; construction determines the declared capability set |
| Adapter streaming and live stream events | Deferred; streaming entry points fail before execution |
| Synthetic checkpoints and stored model-response substitution | Removed or deferred; model overrides always make one live model call |
| Native checkpoint reconstruction, time travel, and node-boundary replay | Deferred; LangGraph keeps its checkpointer and thread state |
| Worker-managed interrupt resume | Deferred; direct interrupts work, worker interrupts fail explicitly |
| ZenML pipeline, flow, stack, and sandbox helpers | Not part of the v2 LangGraph adapter |
| Separate adapter distribution | Available; install `kitaru-langgraph` and import from `kitaru_langgraph` |

If your integration depends on v1 streaming, checkpoint strategies, synthetic checkpoints, or worker-managed resume, keep it on v1 until the required capability has an explicit v2 contract. Do not translate those options into the v2 runner.

## Next steps

- Run the [provider-free recording example](https://github.com/zenml-io/kitaru/tree/develop/examples/integrations/langgraph_v2).
- Compare other integration boundaries in the [adapters overview](README.md).
- Read the [LangGraph overview](https://docs.langchain.com/oss/python/langgraph/overview) and [interrupt documentation](https://docs.langchain.com/oss/python/langgraph/interrupts).
