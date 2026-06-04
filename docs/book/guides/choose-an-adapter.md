---
description: Pick the Kitaru integration path for your existing agent harness
icon: shuffle
---

# Choose an Adapter

Kitaru does not require one agent framework. Pick the boundary that matches the
code you already have.

## Decision table

| You have... | Use this | Replay boundary | First page |
|---|---|---|---|
| Plain Python functions | `@flow` + `@checkpoint` | Your function boundaries | [Quickstart](../getting-started/quickstart.md) |
| PydanticAI agent | `KitaruAgent` | model/tool/MCP calls by default, or one turn checkpoint | [PydanticAI Adapter](../adapters/pydantic-ai.md) |
| OpenAI Agents SDK agent | `KitaruRunner` | per-call checkpoints or one runner-call checkpoint | [OpenAI Agents Adapter](../adapters/openai-agents.md) |
| Claude Agent SDK invocation | `KitaruClaudeRunner` | one completed Claude invocation | [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md) |
| LangGraph graph | `KitaruGraphRunner` | one graph call, or middleware-wrapped model/tool calls | [LangGraph Adapter](../adapters/langgraph.md) |

## Pick by goal

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>I want the simplest durable boundary</strong></td><td>Wrap phases of work in @checkpoint and keep your existing Python control flow</td><td><a href="../concepts/checkpoints.md">../concepts/checkpoints.md</a></td></tr><tr><td><strong>I want granular agent observability</strong></td><td>Use PydanticAI or OpenAI calls mode so model and tool calls become visible replay units</td><td><a href="../adapters/pydantic-ai.md">../adapters/pydantic-ai.md</a></td></tr><tr><td><strong>I want a clean return value from flow.wait()</strong></td><td>Use a coarse runner-call or turn checkpoint around the whole agent turn</td><td><a href="../adapters/openai-agents.md">../adapters/openai-agents.md</a></td></tr><tr><td><strong>I need human approval</strong></td><td>Keep wait/approval bridges at flow scope so the execution can suspend safely</td><td><a href="wait-and-resume.md">wait-and-resume.md</a></td></tr></tbody></table>

## Strategy notes

| Strategy | Best for | Trade-off |
|---|---|---|
| Coarse checkpoint | Fast migration, easy `.wait()` return value, low adapter complexity | Replay reruns the whole agent turn if it fails inside the turn |
| Granular checkpoints | Expensive LLM/tool chains where partial replay matters | More checkpoint rows and stricter rules around waits/nesting |
| Explicit raw checkpoints | Maximum control and framework independence | You decide every durable boundary yourself |

## Human-in-the-loop rule

`kitaru.wait()` belongs at flow scope, not inside a normal checkpoint body. If
your harness adapter creates granular tool checkpoints, configure wait-bearing
tools so the adapter keeps that wait outside the synthetic checkpoint wrapper.

Start with:

- [Wait, Input, and Resume](wait-and-resume.md)
- [PydanticAI human-in-the-loop tools](../adapters/pydantic-ai.md#ask-the-human-from-a-tool-body)
- [OpenAI approval interruptions](../adapters/openai-agents.md#approval-interruptions)
- [LangGraph interrupt and resume](../adapters/langgraph.md#interrupt-and-resume)

## Next

Run a small adapter example from [Examples](../getting-started/examples.md) before
porting a production agent. The fastest useful proof is one completed execution
where you can see the expected checkpoint shape.
