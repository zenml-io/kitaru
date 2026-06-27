---
description: Use Kitaru with PydanticAI, OpenAI Agents, Claude Agent SDK, Gemini Interactions, Google ADK, and LangGraph.
icon: puzzle-piece
---

# Adapters

Adapters let Kitaru record and replay an agent you built with another framework, without rewriting it. Your framework still runs the agent — it decides how the agent thinks, calls tools, streams, pauses, and resumes — while the adapter wraps the durable seams so each model call, tool call, or graph invocation lands as a checkpoint you can replay later.

That boundary is deliberate. Kitaru records what passes through the seam the framework exposes safely; it does not claim to replay work it never saw. That honesty is what makes a replay faithful: a rerun with no change reproduces the original run, so when you replay again with one input changed, the diff is your change and not replay noise.

## Choose an adapter

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>PydanticAI</strong></td><td>Wrap a pydantic_ai.Agent and record model, tool, MCP, and wait boundaries.</td><td><a href="pydantic-ai.md">pydantic-ai.md</a></td></tr><tr><td><strong>OpenAI Agents</strong></td><td>Run OpenAI Agents SDK workflows with call-level or runner-call durability.</td><td><a href="openai-agents.md">openai-agents.md</a></td></tr><tr><td><strong>Claude Agent SDK</strong></td><td>Record one Claude Agent SDK invocation as one Kitaru checkpoint with usage and audit artifacts.</td><td><a href="claude-agent-sdk.md">claude-agent-sdk.md</a></td></tr><tr><td><strong>Gemini Interactions</strong></td><td>Record stable Gemini Interactions and Antigravity managed-agent responses as Kitaru checkpoints.</td><td><a href="gemini-interactions.md">gemini-interactions.md</a></td></tr><tr><td><strong>Google ADK</strong></td><td>Experimentally wrap Google ADK runner turns, or explicit ADK model/tool objects you control.</td><td><a href="google-adk.md">google-adk.md</a></td></tr><tr><td><strong>LangGraph</strong></td><td>Checkpoint LangGraph graph calls, interrupts, resumes, and LangChain model/tool calls.</td><td><a href="langgraph.md">langgraph.md</a></td></tr></tbody></table>

## How to pick the right page

Use the adapter that matches the framework object you already call:

- **PydanticAI** — your code has a `pydantic_ai.Agent` and you want Kitaru to record its model, tool, MCP, and wait boundaries.
- **OpenAI Agents** — your agent is built on the OpenAI Agents SDK and you want either detailed call checkpoints or one checkpoint around a whole runner call.
- **Claude Agent SDK** — your code invokes Claude through the Claude Agent SDK or Claude Code-style sessions and you want the full invocation saved as one durable step.
- **Gemini Interactions** — your code calls Gemini Interactions, including Antigravity managed-agent calls, and you want the response captured as replayable output.
- **Google ADK** — your code uses Google ADK, and you want experimental whole-runner checkpointing or explicit ADK model/tool checkpoints in an isolated no-dev ADK environment.
- **LangGraph** — your agent runs as a LangGraph graph (or LangChain agents on top of it) and you want graph, model, and tool boundaries recorded.

## Migrating existing agent code

If you already have framework-specific agent code, the [`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills) package includes migration skills that walk your coding agent through the conservative adapter path.

In Claude Code, invoke the skill that matches your current framework:

- `/kitaru:kitaru-pydantic-ai-migration`
- `/kitaru:kitaru-openai-agents-migration`
- `/kitaru:kitaru-langgraph-migration`
- `/kitaru:kitaru-claude-agent-sdk-migration`
- `/kitaru:kitaru-gemini-interactions-migration`
- Google ADK support is experimental; use the [Google ADK adapter page](google-adk.md) and runnable example first.

For install instructions and the full skill list, see [Agent Skills](../agent-native/claude-code-skill.md).

## What adapters do not promise

Adapters record work that passes through the seam, not work the framework hides inside itself. If a framework makes an internal model call, shell command, browser step, or tool call without exposing it, Kitaru cannot replay that hidden step — it can only save the result that comes back out. Record at the boundary you control, and what you record replays faithfully.

## Try one end to end

The examples include small adapter scripts you can run locally, plus larger flows that show how adapters fit into a real workflow.

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Runnable examples</strong></td><td>Browse adapter examples and end-to-end agent workflows.</td><td><a href="../getting-started/examples.md">../getting-started/examples.md</a></td></tr></tbody></table>
