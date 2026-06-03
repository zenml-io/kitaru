# Kitaru examples

Welcome — this folder is the fastest way to see what Kitaru feels like in real
code.

Each example group lives in its own directory. To run any example:

```bash
cd examples/<category>/<group>
kitaru init                  # One-time: initialize a Kitaru project
python <module_name>.py      # Run the example
```

Examples use your current Kitaru connection context. If you want them to use a
deployed Kitaru server, connect first with `kitaru login <server>` and confirm
with `kitaru status`. If you are just trying Kitaru locally, run them as-is.

## Start here if you want to...

- **Run the smallest possible durable flow:** `examples/features/basic_flow/first_working_flow.py`
- **See structured metadata logging:** `examples/features/basic_flow/flow_with_logging.py`
- **Publish live checkpoint progress events:** `examples/features/checkpoint_streaming/checkpoint_streaming.py`
- **Persist and reload artifacts:** `examples/features/basic_flow/flow_with_artifacts.py`
- **Run checkpoints in isolated containers with fan-out:** `examples/features/basic_flow/flow_with_checkpoint_runtime.py`
- **Inspect and manage past executions:** `examples/features/execution_management/client_execution_management.py`
- **Pause for human input and resume later:** `examples/features/execution_management/wait_and_resume.py`
- **Replay from a checkpoint with overrides:** `examples/features/replay/replay_with_overrides.py`
- **Track a model call inside a flow:** `examples/features/llm/flow_with_llm.py`
- **Wrap an existing PydanticAI agent:** `examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py`; watch live events with `examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py`
- **Wrap an OpenAI Agents SDK agent:** `examples/integrations/openai_agents_agent/openai_agents_adapter.py`; watch live events with `examples/integrations/openai_agents_agent/openai_agents_streaming.py`
- **Wrap a Claude Agent SDK invocation:** `examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py`; watch live events with `examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py`
- **Wrap LangGraph graphs and agents:** `examples/integrations/langgraph_agent/langgraph_adapter.py` (`graph_call` is local/no-key; `calls` is OpenAI-backed with local ticket tools); watch local graph-call live events with `examples/integrations/langgraph_agent/langgraph_streaming.py`
- **Run a multi-agent OpenAI research bot:** `examples/end_to_end/openai_research_bot/research_bot.py`
- **Build a full coding agent with tool calling and HITL:** `examples/end_to_end/coding_agent/agent.py`
- **Run a granular-checkpoint PydanticAI agent end to end:** `examples/end_to_end/news_scout/scout.py`
- **Wrap a Claude Agent SDK audit with checkpoints, partial replay, and wait/resume:** `examples/end_to_end/compliance_review/`
- **Build a sandboxed, credential-isolated, profile-gated agent harness platform (the stage-by-stage starter kit):** `examples/end_to_end/agent_harness_platform/` — see also the [docs tour](https://kitaru.ai/docs/agent-harness-platform/).
- **Explore Kitaru through MCP tools:** `examples/features/mcp/mcp_query_tools.py`

## Install the extras you need

```bash
uv venv && source .venv/bin/activate   # Create and activate a virtual environment
```

| Goal | Install command |
|---|---|
| Core workflow, execution, replay, and configuration examples | `uv sync --extra local` |
| LLM examples (tracked `kitaru.llm()` calls) | `uv sync --extra local --extra llm` |
| PydanticAI adapter example | `uv sync --extra local --extra pydantic-ai` |
| OpenAI Agents adapter and research bot examples | `uv sync --extra local --extra openai-agents` |
| Claude Agent SDK adapter example | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or Claude SDK provider credentials |
| LangGraph adapter `graph_call` example (local deterministic graph) | `uv sync --extra local --extra langgraph` |
| LangGraph adapter `calls` example (OpenAI-backed model/tool checkpoints) | `uv sync --extra local --extra langgraph-openai` + `OPENAI_API_KEY` |
| Coding agent example | `uv sync --extra local` + model alias / provider credentials |
| Compliance review example | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or remote `anthropic` secret |
| MCP query tools example | `uv sync --extra local --extra mcp` |

## How the examples are organized

- [features/basic_flow/README.md](features/basic_flow/README.md) — smallest flows, logging, artifacts, and runtime configuration
- [features/checkpoint_streaming/README.md](features/checkpoint_streaming/README.md) — publish best-effort live progress and custom events from checkpoints
- [features/execution_management/README.md](features/execution_management/README.md) — inspect executions, resolve waits, and resume work
- [features/replay/README.md](features/replay/README.md) — replay from a checkpoint boundary with targeted overrides
- [features/llm/README.md](features/llm/README.md) — tracked `kitaru.llm()` calls inside flows
- [integrations/pydantic_ai_agent/README.md](integrations/pydantic_ai_agent/README.md) — wrap a PydanticAI agent with Kitaru observability
- [integrations/openai_agents_agent/README.md](integrations/openai_agents_agent/README.md) — real OpenAI API customer-support example with order lookup + shipping policy tools and Kitaru durability
- [integrations/claude_agent_sdk_agent/README.md](integrations/claude_agent_sdk_agent/README.md) — real Claude Agent SDK example showing one invocation as one Kitaru checkpoint
- [integrations/langgraph_agent/README.md](integrations/langgraph_agent/README.md) — LangGraph adapter example with a local/no-key `graph_call` interrupt/resume graph and an OpenAI-backed `calls` agent using deterministic local ticket tools
- [end_to_end/openai_research_bot/README.md](end_to_end/openai_research_bot/README.md) — multi-agent OpenAI research bot with planner/writer runner checkpoints, submitted search fan-out, published report artifacts, and remote secret guidance
- [end_to_end/coding_agent/README.md](end_to_end/coding_agent/README.md) — full coding agent with provider SDK tool calling, HITL, and custom materializers
- [end_to_end/news_scout/README.md](end_to_end/news_scout/README.md) — agentic news monitor with granular per-tool checkpoints, CLI/default interests, and `secret_environment_from` for remote API keys
- [end_to_end/compliance_review/README.md](end_to_end/compliance_review/README.md) — Claude Agent SDK document audit in three runnable stages: crash-resilient turns, sequential domain checkpoints with partial replay, and durable wait/resume conversation
- [end_to_end/agent_harness_platform/README.md](end_to_end/agent_harness_platform/README.md) — stage-by-stage starter kit for an internal agent harness platform: durable PydanticAI agent → DockerSandbox → skills as markdown → credential proxy → typed services → HITL via `kitaru.wait()`. See the [docs tour](https://kitaru.ai/docs/agent-harness-platform/) for the polished surface.
- [features/mcp/README.md](features/mcp/README.md) — inspect flows with the Kitaru MCP server

## Core workflow basics

| Example | Run | Requires | What it demonstrates | Docs | Test |
|---|---|---|---|---|---|
| [Basic flow](features/basic_flow/first_working_flow.py) | `uv run python examples/features/basic_flow/first_working_flow.py` | `uv sync --extra local` | The smallest end-to-end `@flow` + `@checkpoint` workflow | [Quickstart](https://kitaru.ai/docs/getting-started/quickstart) | [tests/test_phase5_example.py](../tests/test_phase5_example.py) |
| [Structured logging](features/basic_flow/flow_with_logging.py) | `uv run python examples/features/basic_flow/flow_with_logging.py` | `uv sync --extra local` | `kitaru.log()` metadata at both flow and checkpoint scope | [Execution Management](https://kitaru.ai/docs/getting-started/execution-management) | [tests/test_phase7_logging_example.py](../tests/test_phase7_logging_example.py) |
| [Checkpoint live events](features/checkpoint_streaming/checkpoint_streaming.py) | `uv run python examples/features/checkpoint_streaming/checkpoint_streaming.py` | `uv sync --extra local` | `kitaru.progress()` and `kitaru.events.publish()` from checkpoint bodies | [Checkpoint Live Events](https://kitaru.ai/docs/guides/checkpoint-streaming) | — |
| [Artifacts](features/basic_flow/flow_with_artifacts.py) | `uv run python examples/features/basic_flow/flow_with_artifacts.py` | `uv sync --extra local` | `kitaru.save()` and `kitaru.load()` across executions | [Artifacts](https://kitaru.ai/docs/getting-started/artifacts) | [tests/test_phase8_artifacts_example.py](../tests/test_phase8_artifacts_example.py) |
| [Configuration](features/basic_flow/flow_with_configuration.py) | `uv run python examples/features/basic_flow/flow_with_configuration.py` | `uv sync --extra local` | `kitaru.configure()` defaults, overrides, and frozen execution specs | [Configuration](https://kitaru.ai/docs/getting-started/configuration) | [tests/test_phase10_configuration_example.py](../tests/test_phase10_configuration_example.py) |
| [Checkpoint runtime](features/basic_flow/flow_with_checkpoint_runtime.py) | `uv run python examples/features/basic_flow/flow_with_checkpoint_runtime.py` | `uv sync --extra local` | `@checkpoint(runtime="isolated")` with `.submit()` fan-out | [Checkpoints](https://kitaru.ai/docs/concepts/checkpoints) | — |

## Execution lifecycle and recovery

| Example | What it demonstrates | Docs |
|---|---|---|
| [Execution management](features/execution_management/client_execution_management.py) | `KitaruClient` for listing runs, reading details, and loading artifacts | [Execution Management](https://kitaru.ai/docs/guides/execution-management) |
| [Wait and resume](features/execution_management/wait_and_resume.py) | `kitaru.wait()` — pause for human input, resume later | [Wait and Resume](https://kitaru.ai/docs/guides/wait-and-resume) |
| [Replay with overrides](features/replay/replay_with_overrides.py) | Replay from a checkpoint boundary while overriding selected inputs | [Replay and Overrides](https://kitaru.ai/docs/guides/replay-and-overrides) |

## LLMs and agent integrations

| Example | Run | Requires | What it demonstrates | Docs | Test |
|---|---|---|---|---|---|
| [Tracked LLM calls](features/llm/flow_with_llm.py) | `uv run python examples/features/llm/flow_with_llm.py` | `uv sync --extra local` + model alias / provider credentials | `kitaru.llm()` prompt-response tracking with usage metadata | [Tracked LLM Calls](https://kitaru.ai/docs/guides/llm-calls) | [tests/test_phase12_llm_example.py](../tests/test_phase12_llm_example.py) |
| [PydanticAI adapter](integrations/pydantic_ai_agent/pydantic_ai_adapter.py) | `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py` | `uv sync --extra local --extra pydantic-ai` | Wrap an existing PydanticAI agent while keeping a Kitaru replay boundary | [PydanticAI Adapter](https://kitaru.ai/docs/adapters/pydantic-ai/) | — |
| [PydanticAI streaming](integrations/pydantic_ai_agent/pydantic_ai_streaming.py) | `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py` | `uv sync --extra local --extra pydantic-ai --extra openai` + `OPENAI_API_KEY` | Watch best-effort `pydantic_ai.stream.*` live events while `.wait()` returns the durable final answer | [PydanticAI Adapter](https://kitaru.ai/docs/adapters/pydantic-ai/#streaming) | — |
| [OpenAI Agents adapter](integrations/openai_agents_agent/openai_agents_adapter.py) | `uv run python examples/integrations/openai_agents_agent/openai_agents_adapter.py` | `uv sync --extra local --extra openai-agents` + `OPENAI_API_KEY` | Real OpenAI API customer-support flow with tool calls, showing call-level vs runner-call durability | [OpenAI Agents Adapter](https://kitaru.ai/docs/adapters/openai-agents/) | — |
| [OpenAI Agents streaming](integrations/openai_agents_agent/openai_agents_streaming.py) | `uv run python examples/integrations/openai_agents_agent/openai_agents_streaming.py` | `uv sync --extra local --extra openai-agents` + `OPENAI_API_KEY` | Watch best-effort `openai_agents.stream.*` live events while `.wait()` returns the durable `OpenAIRunResult` | [OpenAI Agents Adapter](https://kitaru.ai/docs/adapters/openai-agents/#streaming-with-kitaru-durability) | — |
| [Claude Agent SDK adapter](integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py) | `uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py` | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or Claude SDK provider credentials | Real Claude SDK invocation wrapped by `KitaruClaudeRunner`; one invocation becomes one Kitaru checkpoint with final text, session, usage/cost, and audit artifacts | [Claude Agent SDK Adapter](https://kitaru.ai/docs/adapters/claude-agent-sdk/) | — |
| [Claude Agent SDK streaming](integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py) | `uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py` | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or Claude SDK provider credentials | Watch best-effort `claude_agent_sdk.stream.*` live events while `.wait()` returns the durable `ClaudeRunResult` | [Claude Agent SDK Adapter](https://kitaru.ai/docs/adapters/claude-agent-sdk/#live-streaming-with-kitaru-durability) | — |
| [Gemini Interactions adapter](integrations/gemini_interactions_agent/gemini_interactions_adapter.py) | `uv run python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run` | `uv sync --extra local --extra google-genai` for real calls; dry-run needs no provider key | Wrap one Gemini Interactions API response as one Kitaru checkpoint, with optional Antigravity managed-agent mode | [Gemini Interactions Adapter](https://kitaru.ai/docs/adapters/gemini-interactions/) | [tests/test_gemini_interactions_example.py](../tests/test_gemini_interactions_example.py) |
| [LangGraph adapter](integrations/langgraph_agent/langgraph_adapter.py) | `uv run python examples/integrations/langgraph_agent/langgraph_adapter.py --strategy graph_call` or `--strategy calls` | `uv sync --extra local --extra langgraph` for `graph_call`; `uv sync --extra local --extra langgraph-openai` + `OPENAI_API_KEY` for `calls` | Local/no-key LangGraph interrupt/resume checkpoints, plus OpenAI-backed LangChain model/tool checkpoints with deterministic local ticket tools | [LangGraph Adapter](https://kitaru.ai/docs/adapters/langgraph/) | — |
| [LangGraph streaming](integrations/langgraph_agent/langgraph_streaming.py) | `uv run python examples/integrations/langgraph_agent/langgraph_streaming.py` | `uv sync --extra local --extra langgraph` | Watch best-effort `langgraph.stream.*` live events from a local graph-call stream while `.wait()` returns the durable `LangGraphRunResult` | [LangGraph Adapter](https://kitaru.ai/docs/adapters/langgraph/#graph-call-streaming) | — |
| [OpenAI research bot](end_to_end/openai_research_bot/research_bot.py) | `cd examples/end_to_end/openai_research_bot && uv run python research_bot.py "Your query" --max-searches 2` | `uv sync --extra local --extra openai-agents` + local `OPENAI_API_KEY` or remote `openai-research-bot-keys` secret | Planner → submitted search fan-out → writer report using `KitaruRunner(..., checkpoint_strategy="runner_call")`; publishes `research_plan`, `search_summaries`, and `final_report` artifacts | [OpenAI Agents Adapter](https://kitaru.ai/docs/adapters/openai-agents/) | [tests/test_openai_research_bot_example.py](../tests/test_openai_research_bot_example.py) |
| [Coding agent](end_to_end/coding_agent/agent.py) | `cd examples/end_to_end/coding_agent && uv run python agent.py "Your task"` | `uv sync --extra local` + model alias / provider credentials | Full agent loop with provider SDK tool calling, `kitaru.wait()` HITL, custom materializers, and artifact persistence | [Tracked LLM Calls](https://kitaru.ai/docs/guides/llm-calls) | — |
| [News scout](end_to_end/news_scout/scout.py) | `cd examples/end_to_end/news_scout && uv run python scout.py` | `uv sync --extra local --extra pydantic-ai --extra llm` + `ANTHROPIC_API_KEY` locally (or a `news-scout-keys` secret for remote stacks) | PydanticAI agent with `checkpoint_strategy="calls"` — every model/tool call is its own Kitaru checkpoint; `publish_report` promotes the agent output to a named `final_report` artifact; `ImageSettings.secret_environment_from` attaches the provider-keys secret automatically when the active stack is remote | [Examples index](https://kitaru.ai/docs/getting-started/examples) | [tests/test_news_scout_example.py](../tests/test_news_scout_example.py) |
| [Compliance review](end_to_end/compliance_review/README.md) | `uv run python examples/end_to_end/compliance_review/stage_1_single_turn.py` | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or remote `anthropic` secret | Three runnable Claude Agent SDK stages: checkpointed turns, sequential domain checkpoints with partial replay, and durable wait/resume conversation | [Replay and Overrides](https://kitaru.ai/docs/guides/replay-and-overrides) | [tests/test_phase4_compliance_review_stage4.py](../tests/test_phase4_compliance_review_stage4.py) |
| [MCP query tools](features/mcp/mcp_query_tools.py) | `uv run python examples/features/mcp/mcp_query_tools.py` | `uv sync --extra local --extra mcp` | Query executions and artifacts through the Kitaru MCP server | [Execution Management](https://kitaru.ai/docs/guides/execution-management) | [tests/mcp/test_phase19_mcp_example.py](../tests/mcp/test_phase19_mcp_example.py) |

## Recommended learning path

If you are new to Kitaru, this is the smoothest path:

1. `uv run python examples/features/basic_flow/first_working_flow.py`
2. `uv run python examples/features/basic_flow/flow_with_logging.py`
3. `uv run python examples/features/checkpoint_streaming/checkpoint_streaming.py`
4. `uv run python examples/features/basic_flow/flow_with_artifacts.py`
5. `uv run python examples/features/execution_management/client_execution_management.py`
6. `uv run python examples/features/execution_management/wait_and_resume.py`
7. `uv run python examples/features/replay/replay_with_overrides.py`
8. `uv run python examples/features/llm/flow_with_llm.py`
9. `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py`
10. `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py` *(PydanticAI live events with durable `.wait()` result; requires `OPENAI_API_KEY`)*
11. `uv run python examples/integrations/openai_agents_agent/openai_agents_adapter.py`
12. `uv run python examples/integrations/openai_agents_agent/openai_agents_streaming.py` *(OpenAI Agents live events with durable `OpenAIRunResult`; requires `OPENAI_API_KEY`)*
13. `uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py` *(Claude SDK invocation-level checkpoint)*
14. `uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py` *(Claude live events with durable `ClaudeRunResult`; requires Claude credentials)*
15. `uv run python examples/integrations/langgraph_agent/langgraph_adapter.py --strategy graph_call` *(local interrupt/resume with stable thread_id; no API key)*; then try `--strategy calls` after installing `langgraph-openai` and setting `OPENAI_API_KEY`
16. `uv run python examples/integrations/langgraph_agent/langgraph_streaming.py` *(local graph-call live events; no API key)*
17. `cd examples/end_to_end/openai_research_bot && uv run python research_bot.py "Your query" --max-searches 2` *(OpenAI planner → submitted searches → writer report)*
18. `cd examples/end_to_end/coding_agent && uv run python agent.py "Your task"` *(full agent with tools + HITL)*
19. `cd examples/end_to_end/news_scout && uv run python scout.py` *(granular-checkpoint agent with 4 tools, dashboard-readable final_report artifact)*
20. `uv run python examples/end_to_end/compliance_review/stage_1_single_turn.py` *(Claude Agent SDK audit; walk through Stage 1, Stage 2, and conversational Stage 4 to see checkpointing, replay, and wait/resume in turn)*
21. `uv run python examples/features/mcp/mcp_query_tools.py`

If you prefer the hosted docs view, start with the
[Examples page](https://kitaru.ai/docs/getting-started/examples).
