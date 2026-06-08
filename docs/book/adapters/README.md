---
description: Use Kitaru with PydanticAI, OpenAI Agents, Claude Agent SDK, Gemini Interactions, and LangGraph.
icon: puzzle-piece
---

# Adapters

Adapters let you keep the agent framework you already chose while Kitaru records the parts of the run that need to survive failure.

Your framework still runs the agent. It decides how the agent thinks, calls tools, streams output, pauses, or resumes. Kitaru records the durable seams around that work: a model call, a tool call, a graph invocation, or one full SDK interaction, depending on what the framework exposes safely.

That boundary matters. If a run crashes halfway through, Kitaru should not pretend it can replay work it never saw. An adapter is the layer that says, "this part can be saved and reused honestly," while leaving the framework's own internals alone.

## Choose an adapter

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>PydanticAI</strong></td><td>Wrap a pydantic_ai.Agent and record model, tool, MCP, and wait boundaries.</td><td><a href="pydantic-ai.md">pydantic-ai.md</a></td></tr><tr><td><strong>OpenAI Agents</strong></td><td>Run OpenAI Agents SDK workflows with call-level or runner-call durability.</td><td><a href="openai-agents.md">openai-agents.md</a></td></tr><tr><td><strong>Claude Agent SDK</strong></td><td>Record one Claude Agent SDK invocation as one Kitaru checkpoint with usage and audit artifacts.</td><td><a href="claude-agent-sdk.md">claude-agent-sdk.md</a></td></tr><tr><td><strong>Gemini Interactions</strong></td><td>Record stable Gemini Interactions and Antigravity managed-agent responses as Kitaru checkpoints.</td><td><a href="gemini-interactions.md">gemini-interactions.md</a></td></tr><tr><td><strong>LangGraph</strong></td><td>Checkpoint LangGraph graph calls, interrupts, resumes, and LangChain model/tool calls.</td><td><a href="langgraph.md">langgraph.md</a></td></tr></tbody></table>

## How to pick the right page

Use the adapter that matches the framework object you are already calling:

- Use **PydanticAI** when your code already has a `pydantic_ai.Agent` and you want Kitaru to record the agent's model, tool, MCP, and wait boundaries.
- Use **OpenAI Agents** when your agent is built on the OpenAI Agents SDK and you want either detailed call checkpoints or one checkpoint around a whole runner call.
- Use **Claude Agent SDK** when your code invokes Claude through the Claude Agent SDK or Claude Code-style sessions and you want the full invocation saved as one durable step.
- Use **Gemini Interactions** when your code calls Gemini Interactions, including Antigravity managed-agent calls, and you want the response captured as replayable Kitaru output.
- Use **LangGraph** when your agent runs as a LangGraph graph, or when you are using LangChain agents on top of LangGraph and want graph, model, and tool boundaries recorded.

## Migrating existing agent code

If you already have framework-specific agent code, the [`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills) package includes migration skills that walk your coding agent through the conservative adapter path.

In Claude Code, invoke the migration skill that matches your current framework:

- `/kitaru:kitaru-pydantic-ai-migration`
- `/kitaru:kitaru-openai-agents-migration`
- `/kitaru:kitaru-langgraph-migration`
- `/kitaru:kitaru-claude-agent-sdk-migration`
- `/kitaru:kitaru-gemini-interactions-migration`

For install instructions and the full skill list, see [Agent Skills](../agent-native/claude-code-skill.md).

## What adapters do not promise

Adapters do not magically checkpoint every action inside another framework. If the framework hides an internal model call, shell command, browser step, or tool call from Kitaru, Kitaru cannot replay that hidden step by itself later.

Think of an adapter as a careful recorder at the doorway. When work passes through the doorway, Kitaru can save it. If work happens behind a closed door inside the framework, the adapter can only save the result that comes back out.

## Try one end to end

The examples include small adapter scripts you can run locally, plus larger flows that show how adapters fit into a real workflow.

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Runnable examples</strong></td><td>Browse adapter examples and end-to-end agent workflows.</td><td><a href="../getting-started/examples.md">../getting-started/examples.md</a></td></tr></tbody></table>
