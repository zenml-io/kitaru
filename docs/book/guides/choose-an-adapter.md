---
description: Pick the Kitaru integration path for your existing agent harness
icon: shuffle
---

# Choose an Adapter

An adapter connects an existing agent harness to Kitaru without rewriting the agent. The source-only OpenAI Agents v2 adapter records one result session with observed activity nodes and applies a bounded set of replay overrides before a new run. Its observation nodes are not independently replayable units.

## Decision table

The **recorded boundary** column states what Kitaru persists for that integration. Some integrations let you replay from one recorded call. A session-node adapter can record finer observations without making each node independently replayable.

| You have... | Use this | Recorded boundary | First page |
|---|---|---|---|
| Plain Python functions | `@flow` + `@checkpoint` | The function boundaries you choose | [Quickstart](../getting-started/quickstart.md) |
| PydanticAI agent | `KitaruAgent` | **Per model/tool/MCP call** by default, or one turn checkpoint | [PydanticAI Adapter](../adapters/pydantic-ai.md) |
| OpenAI Agents SDK agent | `KitaruRunner` | One result session with observed model, tool, hosted-tool, and handoff nodes | [OpenAI Agents Adapter](../adapters/openai-agents.md) |
| Claude Agent SDK invocation | `KitaruClaudeRunner` | One completed Claude invocation | [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md) |
| LangGraph graph | `KitaruGraphRunner` | One graph call, or middleware-wrapped model/tool calls | [LangGraph Adapter](../adapters/langgraph.md) |

{% hint style="info" %}
The source-only OpenAI Agents v2 adapter records observations as session nodes that are not independently replayable. A worker-managed replay can override the prompt, instructions, run-level model, model settings, and a static value for a directly attached `FunctionTool`. The adapter has no per-run replay argument. Use the Kitaru replay and task flow to select a replay through `KITARU_REPLAY_ID`, and use PydanticAI when you need its per-call replay behavior.
{% endhint %}

## Pick by goal

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>I want the simplest durable boundary</strong></td><td>Wrap phases of work in @checkpoint and keep your existing Python control flow</td><td><a href="../concepts/checkpoints.md">../concepts/checkpoints.md</a></td></tr><tr><td><strong>I want granular agent observability</strong></td><td>Use PydanticAI for per-call replay, or the source-only OpenAI v2 adapter for session-node observations that are not independently replayable</td><td><a href="../adapters/pydantic-ai.md">../adapters/pydantic-ai.md</a></td></tr><tr><td><strong>I use the OpenAI Agents SDK</strong></td><td>Keep the native RunResult while Kitaru records one session and its observed activity</td><td><a href="../adapters/openai-agents.md">../adapters/openai-agents.md</a></td></tr><tr><td><strong>I need human approval</strong></td><td>Keep wait/approval bridges at flow scope so the execution can suspend safely</td><td><a href="wait-and-resume.md">wait-and-resume.md</a></td></tr></tbody></table>

## Strategy notes

| Strategy | Best for | Trade-off |
|---|---|---|
| Coarse checkpoint | Fast migration, clean `.wait()` return value, low adapter complexity | Replay can only re-run the whole turn; you cannot replay from a single call |
| Granular (per-call) checkpoints | Expensive LLM/tool chains where you want to replay from one call with one input changed | More checkpoint rows and stricter rules around waits/nesting |
| Explicit raw checkpoints | Maximum control and framework independence | You decide every durable boundary yourself |

## Human-in-the-loop rule

`kitaru.wait()` belongs at flow scope, not inside a normal checkpoint body. If your harness adapter creates granular tool checkpoints, configure wait-bearing tools so the adapter keeps that wait outside the synthetic checkpoint wrapper.

The OpenAI Agents v2 adapter does not provide this bridge. If the OpenAI SDK returns an approval interruption, the adapter fails the Kitaru session instead of exposing durable resume.

Start with:

- [Wait, Input, and Resume](wait-and-resume.md)
- [PydanticAI human-in-the-loop tools](../adapters/pydantic-ai.md#ask-the-human-from-a-tool-body)
- [OpenAI adapter exclusions](../adapters/openai-agents.md#deliberate-exclusions)
- [LangGraph interrupt and resume](../adapters/langgraph.md#interrupt-and-resume)

## Next

Run a small adapter example from [Examples](../getting-started/examples.md) before porting a production agent. The fastest useful proof is one completed run where you can see the expected checkpoints or session nodes.
