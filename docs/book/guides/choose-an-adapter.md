---
description: Pick the Kitaru integration path for your existing agent harness
icon: shuffle
---

# Choose an Adapter

An adapter wraps an existing agent harness so each model call and tool call lands
as a Kitaru checkpoint, with no rewrite of your agent. The boundary you pick
decides how fine-grained your checkpoints are, which in turn sets how precisely
you can replay: per-call checkpoints let you reproduce a run and replay it from
the exact call you changed, while a single coarse checkpoint only lets you replay
the whole turn. Pick the boundary that matches the code you already have, then go
as granular as your replay needs require.

## Decision table

The **replay boundary** column is the unit you can replay from: a finer boundary
means `flow.replay(exec_id, at="<checkpoint>", ...)` can re-execute from a
single model or tool call instead of re-running the entire turn.

| You have... | Use this | Replay boundary (finest available) | First page |
|---|---|---|---|
| Plain Python functions | `@flow` + `@checkpoint` | Your function boundaries — you choose them | [Quickstart](../getting-started/quickstart.md) |
| PydanticAI agent | `KitaruAgent` | **Per model/tool/MCP call** by default, or one turn checkpoint | [PydanticAI Adapter](../adapters/pydantic-ai.md) |
| OpenAI Agents SDK agent | `KitaruRunner` | **Per call**, or one runner-call checkpoint | [OpenAI Agents Adapter](../adapters/openai-agents.md) |
| Claude Agent SDK invocation | `KitaruClaudeRunner` | One completed Claude invocation | [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md) |
| LangGraph, LangChain, or Deep Agents graph | `KitaruGraphRunner` from `kitaru-langgraph` | One invocation session; factory construction adds observable model/tool nodes and replay controls, not node-boundary replay | [LangGraph Adapter](../adapters/langgraph.md) |

{% hint style="info" %}
**Per-call checkpointing is fullest in the PydanticAI (`KitaruAgent`) and OpenAI Agents SDK (`KitaruRunner`) adapters.** The LangGraph v2 adapter records each invocation as one session. Factory-built LangChain and Deep Agents expose model/tool nodes and can apply live request overrides or supported tool substitution, but native LangGraph checkpoint reconstruction and node-boundary replay are deferred.
{% endhint %}

## Pick by goal

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>I want the simplest durable boundary</strong></td><td>Wrap phases of work in @checkpoint and keep your existing Python control flow</td><td><a href="../concepts/checkpoints.md">../concepts/checkpoints.md</a></td></tr><tr><td><strong>I want granular agent observability</strong></td><td>Use PydanticAI or OpenAI calls mode so model and tool calls become visible replay units</td><td><a href="../adapters/pydantic-ai.md">../adapters/pydantic-ai.md</a></td></tr><tr><td><strong>I want a clean return value from flow.wait()</strong></td><td>Use a coarse runner-call or turn checkpoint around the whole agent turn</td><td><a href="../adapters/openai-agents.md">../adapters/openai-agents.md</a></td></tr><tr><td><strong>I need human approval</strong></td><td>Keep wait/approval bridges at flow scope so the execution can suspend safely</td><td><a href="wait-and-resume.md">wait-and-resume.md</a></td></tr></tbody></table>

## Strategy notes

| Strategy | Best for | Trade-off |
|---|---|---|
| Coarse checkpoint | Fast migration, clean `.wait()` return value, low adapter complexity | Replay can only re-run the whole turn — you cannot replay from a single call |
| Granular (per-call) checkpoints | Expensive LLM/tool chains where you want to replay from one call with one input changed | More checkpoint rows and stricter rules around waits/nesting |
| Explicit raw checkpoints | Maximum control and framework independence | You decide every durable boundary yourself |

## Human-in-the-loop rule

`kitaru.wait()` belongs at flow scope, not inside a normal checkpoint body. If
your harness adapter creates granular tool checkpoints, configure wait-bearing
tools so the adapter keeps that wait outside the synthetic checkpoint wrapper.

Start with:

- [Wait, Input, and Resume](wait-and-resume.md)
- [PydanticAI human-in-the-loop tools](../adapters/pydantic-ai.md#ask-the-human-from-a-tool-body)
- [OpenAI approval interruptions](../adapters/openai-agents.md#approval-interruptions)
- [LangGraph interrupts and unsupported invocation modes](../adapters/langgraph.md#interrupts-and-unsupported-invocation-modes)

## Next

Run a small adapter example from [Examples](../getting-started/examples.md) before
porting a production agent. The fastest useful proof is one completed execution
where you can see the expected checkpoint shape.
