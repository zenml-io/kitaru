---
description: Runnable Kitaru examples — start with the Agent Harness Platform tour, or jump to a feature-focused example
icon: code
---

# Examples

The Kitaru repo includes a set of runnable examples grouped by purpose:

- **Agent Harness Platform** — a stage-by-stage tour through building a durable agent harness platform on Kitaru + PydanticAI. Read this first if you're new.
- **Other end-to-end examples** — production-shaped, self-contained scenarios that exercise multiple primitives at once. No required reading order.
- **Feature-focused examples** — small examples that demo one Kitaru primitive in isolation.

Every example is a standalone project — clone the repo, `cd` into the example you want, and run it directly.

Clone the repo, install dependencies with `uv`, initialize the project once, and then run the example you want.

```bash
git clone https://github.com/zenml-io/kitaru.git
cd kitaru
uv sync --extra local
uv run kitaru init
uv run python examples/features/basic_flow/first_working_flow.py
```

{% hint style="info" %}
Run `uv run kitaru init` once in the repo checkout before your first example.
It creates the project marker Kitaru uses when replaying or resolving flow
source from a saved execution.
{% endhint %}

Most examples can be run from the repository root with `uv run python path/to/script.py`. Some end-to-end examples (including the Agent Harness Platform tour) tell you to `cd` into their directory first because they read a local `.env` file or have a multi-step README.

{% hint style="info" %}
Adapting an existing PydanticAI, OpenAI Agents, LangGraph, Claude Agent SDK, or
Gemini Interactions project? See [Agent Skills](../agent-native/claude-code-skill.md)
for migration skills that guide a coding agent through the adapter-specific path.
{% endhint %}

## Connection context

Examples use whatever Kitaru connection context is already active.

- If you are just trying Kitaru locally, run `uv run kitaru login` and use them as-is.
- If you already have a deployed Kitaru server and want the examples to use it, connect first and verify the active context before running the example.

```bash
uv run kitaru login https://my-server.example.com
uv run kitaru status
```

## Start here — Agent Harness Platform

A platform engineer's starter kit for building their org's internal agent harness platform on top of Kitaru + PydanticAI. **Bring Docker and one model-provider API key; then `bash setup.sh && uv run python stage_N_*.py`.** Stages build progressively from a 30-line durable agent to a sandboxed, credential-isolated agent with HITL — each stage adds exactly one tool or one architectural primitive, the library grows monotonically, and the per-stage `Profile` gates which capabilities each agent actually exercises.

Use it as the first thing you read end-to-end, and as the thing you fork for your team.

→ [Agent Harness Platform tour](../agent-harness-platform/README.md) — read the stage-by-stage docs, then [grab the code on GitHub](https://github.com/zenml-io/kitaru/tree/develop/examples/end_to_end/agent_harness_platform).

```bash
git clone https://github.com/zenml-io/kitaru.git
cd kitaru/examples/end_to_end/agent_harness_platform
uv sync
uv run kitaru init
export OPENAI_API_KEY=sk-...
uv run python stage_1_basic_agent.py
```

## Other end-to-end examples

Production-shaped examples that exercise multiple primitives in one runnable scenario. Each is self-contained and focused on one harness or scenario — no progressive tour, no required reading order. Pick the one closest to your domain.

| Example | Demonstrates | Path |
|---|---|---|
| Compliance review | Multi-stage Claude audit using the [Claude Agent SDK](https://github.com/anthropics/claude-agent-sdk-python). Each agent turn is a checkpoint; later stages add domain decomposition with partial replay and conversational `kitaru.wait()` resume across crashes. | [`examples/end_to_end/compliance_review/`](https://github.com/zenml-io/kitaru/tree/develop/examples/end_to_end/compliance_review) |
| OpenAI research bot | Multi-agent OpenAI research bot using `KitaruRunner(checkpoint_strategy="runner_call")` — planner/writer runner checkpoints with submitted search fan-out. Publishes `research_plan`, `search_summaries`, and `final_report` artifacts. | [`examples/end_to_end/openai_research_bot/`](https://github.com/zenml-io/kitaru/tree/develop/examples/end_to_end/openai_research_bot) |
| Coding agent | Interactive coding agent built directly on provider SDKs (no PydanticAI, no LangChain). Demos parallel tool execution, durable HITL via `kitaru.wait()`, custom materializers, and descriptive checkpoint names supplied by the LLM. | [`examples/end_to_end/coding_agent/`](https://github.com/zenml-io/kitaru/tree/develop/examples/end_to_end/coding_agent) |
| News scout | PydanticAI agent that scores news across your interest list — `checkpoint_strategy="calls"` makes every search/fetch/score call replayable. Interests come from CLI flags or a built-in default list. No Docker required. | [`examples/end_to_end/news_scout/`](https://github.com/zenml-io/kitaru/tree/develop/examples/end_to_end/news_scout) |

## Feature-focused examples

Small examples that demo one primitive in isolation. Pick by the thing you want to see.

### Core workflow basics

| Example | Demonstrates | Related docs |
|---|---|---|
| `features/basic_flow/first_working_flow.py` | Smallest `@flow` + `@checkpoint` example | [Quickstart](quickstart.md) |
| `features/basic_flow/flow_with_logging.py` | `kitaru.log()` metadata at flow and checkpoint scope | [Logging](../concepts/logging.md) |
| `features/checkpoint_streaming/checkpoint_streaming.py` | `kitaru.progress()` and `kitaru.events.publish()` from checkpoint bodies | [Checkpoint Live Events](../guides/checkpoint-streaming.md) |
| `features/basic_flow/flow_with_artifacts.py` | `kitaru.save()` and `kitaru.load()` across executions | [Artifacts](../guides/artifacts.md) |
| `features/basic_flow/flow_with_checkpoint_runtime.py` | `@checkpoint(runtime="isolated")` for work that should run outside the runner process | [Checkpoints](../concepts/checkpoints.md) |
| `features/basic_flow/flow_with_configuration.py` | `kitaru.configure()` defaults, overrides, and frozen specs | [Configuration](../guides/configuration.md) |
| `features/sandbox/active_stack_sandbox_command.py` | A tracked `@flow` + `@checkpoint` that calls `kitaru.run_sandbox_command(...)` using the active stack's sandbox component | [Stacks](../stacks/README.md#use-the-active-stack-sandbox-from-python) |

### Execution lifecycle and recovery

| Example | Demonstrates | Related docs |
|---|---|---|
| `features/execution_management/client_execution_management.py` | `KitaruClient` for listing runs, reading details, and loading data | [Execution Management](../guides/execution-management.md) |
| `features/execution_management/wait_and_resume.py` | `kitaru.wait()` with inline prompt or CLI input/resume | [Wait, Input, and Resume](../guides/wait-and-resume.md) |
| `features/replay/replay_with_overrides.py` | Replay from a checkpoint with overridden inputs | [Replay and Overrides](../guides/replay-and-overrides.md) |

### LLMs and agent integrations

| Example | Demonstrates | Related docs |
|---|---|---|
| `features/llm/flow_with_llm.py` | `kitaru.llm()` prompt-response tracking with usage metadata | [Tracked LLM Calls](../guides/llm-calls.md) |
| `integrations/pydantic_ai_agent/pydantic_ai_adapter.py` | Wrap a PydanticAI agent with granular Kitaru replay boundaries | [PydanticAI Adapter](../adapters/pydantic-ai.md) |
| `integrations/pydantic_ai_agent/pydantic_ai_streaming.py` | Watch best-effort `pydantic_ai.stream.*` live events while `.wait()` returns the durable final answer | [PydanticAI Adapter](../adapters/pydantic-ai.md#streaming) |
| `integrations/openai_agents_agent/openai_agents_adapter.py` | Wrap an OpenAI Agents SDK agent with call-level or runner-call durability in a real API-backed support flow | [OpenAI Agents Adapter](../adapters/openai-agents.md) |
| `integrations/openai_agents_agent/openai_agents_streaming.py` | Watch best-effort `openai_agents.stream.*` live events while `.wait()` returns the durable `OpenAIRunResult` | [OpenAI Agents Adapter](../adapters/openai-agents.md#streaming-with-kitaru-durability) |
| `integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py` | Wrap one Claude Agent SDK invocation as one Kitaru checkpoint, with final text, session ID, usage/cost, and audit artifacts (`ANTHROPIC_API_KEY` or Claude SDK provider credentials required) | [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md) |
| `integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py` | Watch best-effort `claude_agent_sdk.stream.*` live events while `.wait()` returns the durable `ClaudeRunResult` (`ANTHROPIC_API_KEY` or Claude SDK provider credentials required) | [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md#live-streaming-with-kitaru-durability) |
| `integrations/gemini_interactions_agent/gemini_interactions_adapter.py` | Wrap one Gemini Interactions API response as one Kitaru checkpoint, with no-network previews, streaming mode, and an Antigravity managed-agent path | [Gemini Interactions Adapter](../adapters/gemini-interactions.md) |
| `integrations/langgraph_agent/langgraph_adapter.py` | Local `graph_call` interrupt/resume demo, plus OpenAI-backed `calls` mode with LangChain model/tool checkpoints and deterministic local ticket tools | [LangGraph Adapter](../adapters/langgraph.md) |
| `integrations/langgraph_agent/langgraph_streaming.py` | Watch best-effort `langgraph.stream.*` live events from a local graph-call stream while `.wait()` returns the durable `LangGraphRunResult` | [LangGraph Adapter](../adapters/langgraph.md#graph-call-streaming) |

| `end_to_end/coding_agent/agent.py` | A tool-using coding agent whose LLM calls and tool decisions are visible as durable execution state | [Tracked LLM Calls](../guides/llm-calls.md) |
| `end_to_end/news_scout/scout.py` | PydanticAI news monitor with per-model/per-tool checkpoints, explicit run inputs, and remote-secret image config | [Examples index](examples.md) |
| `end_to_end/openai_research_bot/research_bot.py` | Multi-agent OpenAI research bot with planner/writer runner checkpoints, submitted search fan-out, and published report artifacts | [Research bot section](../adapters/openai-agents.md#end-to-end-research-bot-example) |
| `end_to_end/compliance_review/README.md` | Four-stage Claude Agent SDK audit: checkpointed turns, partial replay, and durable wait/resume conversation | [Replay and Overrides](../guides/replay-and-overrides.md) |
| `features/mcp/mcp_query_tools.py` | Query executions and data through the Kitaru MCP server | [MCP Server](../agent-native/mcp-server.md) |

{% hint style="info" %}
The LLM and most adapter examples require additional dependencies and provider
API keys. The Gemini Interactions example has `--help` and `--dry-run` paths
that require no credentials or network. The LangGraph `graph_call` strategy
is deterministic and local; the LangGraph `calls` strategy requires
`langgraph-openai` and `OPENAI_API_KEY`.
Check each example's README before running a real model-backed example.
{% endhint %}

## If you'd rather build up primitive-by-primitive first

Agent Harness Platform is the recommended starting point for most readers — it's structured as a tour and each stage's commit message points at the docs page that explains the primitive being introduced. If you'd rather see each primitive in isolation before reading them woven together, follow this path:

1. [Quickstart](quickstart.md) — `@flow` + `@checkpoint` in 6 lines.
2. `features/basic_flow/first_working_flow.py` — the same idea as a runnable file.
3. `features/basic_flow/flow_with_logging.py` — [Logging](../concepts/logging.md).
4. `features/checkpoint_streaming/checkpoint_streaming.py` — [Checkpoint Live Events](../guides/checkpoint-streaming.md).
5. `features/basic_flow/flow_with_artifacts.py` — [Artifacts](../guides/artifacts.md).
6. `features/execution_management/wait_and_resume.py` — [Wait, Input, and Resume](../guides/wait-and-resume.md).
7. `features/replay/replay_with_overrides.py` — [Replay and Overrides](../guides/replay-and-overrides.md).
8. `features/llm/flow_with_llm.py` — [Tracked LLM Calls](../guides/llm-calls.md).
9. `features/sandbox/active_stack_sandbox_command.py` — [Stacks](../stacks/README.md#use-the-active-stack-sandbox-from-python). This example runs the sandbox command inside a tracked flow checkpoint. It needs an active stack with one sandbox; `uv run kitaru stack create sandbox-demo` creates a local one.
10. `integrations/pydantic_ai_agent/pydantic_ai_adapter.py` — [PydanticAI Adapter](../adapters/pydantic-ai.md).
11. `integrations/pydantic_ai_agent/pydantic_ai_streaming.py` — [PydanticAI streaming](../adapters/pydantic-ai.md#streaming).
12. `integrations/openai_agents_agent/openai_agents_adapter.py` — [OpenAI Agents Adapter](../adapters/openai-agents.md).
13. `integrations/openai_agents_agent/openai_agents_streaming.py` — [OpenAI Agents streaming](../adapters/openai-agents.md#streaming-with-kitaru-durability).
14. `integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py` — [Claude Agent SDK Adapter](../adapters/claude-agent-sdk.md).
15. `integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py` — [Claude Agent SDK streaming](../adapters/claude-agent-sdk.md#live-streaming-with-kitaru-durability).
16. `integrations/gemini_interactions_agent/gemini_interactions_adapter.py` — [Gemini Interactions Adapter](../adapters/gemini-interactions.md).
17. `integrations/langgraph_agent/langgraph_adapter.py` — [LangGraph Adapter](../adapters/langgraph.md).
18. `integrations/langgraph_agent/langgraph_streaming.py` — [LangGraph streaming](../adapters/langgraph.md#graph-call-streaming).
19. `end_to_end/openai_research_bot/research_bot.py` — [Research bot](../adapters/openai-agents.md#end-to-end-research-bot-example).
20. `features/mcp/mcp_query_tools.py` — [MCP Server](../agent-native/mcp-server.md).
21. **[Agent Harness Platform](../agent-harness-platform/README.md)** — the same primitives, woven into one runnable agent harness platform.
