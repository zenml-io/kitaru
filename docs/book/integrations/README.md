---
description: Choose how Kitaru wraps your existing agent harness.
icon: plug
---

# Integrations

Kitaru should meet your agent where it already is. Use this section when the main
question is "which boundary should I wrap?"

## Choose an integration

| Existing code | Kitaru integration | Replay boundary | Start here |
|---|---|---|---|
| Plain Python functions | `@flow` + `@checkpoint` | Your own function boundaries | [Quickstart](../getting-started/quickstart.md) |
| PydanticAI | `KitaruAgent` | model/tool/MCP calls by default, or one turn checkpoint | [PydanticAI Adapter](../adapters/pydantic-ai.md) |
| OpenAI Agents SDK | `KitaruRunner` | per-call or whole runner-call checkpoint | [OpenAI Agents Adapter](../adapters/openai-agents.md) |
| Claude Agent SDK | `KitaruClaudeRunner` | one completed Claude invocation | [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md) |
| LangGraph | `KitaruGraphRunner` | graph call, or middleware-wrapped model/tool calls | [LangGraph Adapter](../adapters/langgraph.md) |

## Pick by constraint

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>I want the fastest migration</strong></td><td>Use one coarse checkpoint around an existing agent turn.</td><td><a href="../concepts/checkpoints.md">../concepts/checkpoints.md</a></td></tr><tr><td><strong>I want model/tool-level replay</strong></td><td>Use a granular adapter strategy when supported.</td><td><a href="../adapters/pydantic-ai.md">../adapters/pydantic-ai.md</a></td></tr><tr><td><strong>I need approval or HITL</strong></td><td>Keep waits at flow scope and use adapter-specific HITL bridges.</td><td><a href="../guides/wait-and-resume.md">../guides/wait-and-resume.md</a></td></tr><tr><td><strong>I want examples first</strong></td><td>Browse runnable harness examples by proof and prerequisite.</td><td><a href="../getting-started/examples.md">../getting-started/examples.md</a></td></tr></tbody></table>

## Human-in-the-loop rule

`kitaru.wait()` belongs at flow scope, not inside a normal checkpoint body. If an
adapter creates granular tool checkpoints, wait-bearing tools need
adapter-specific handling so the flow can suspend safely.

Relevant pages:

* [PydanticAI Adapter](../adapters/pydantic-ai.md)
* [OpenAI Agents Adapter](../adapters/openai-agents.md)
* [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md)
* [LangGraph Adapter](../adapters/langgraph.md)
* [MCP Server](../agent-native/mcp-server.md)
