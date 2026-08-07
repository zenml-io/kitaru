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
- **Run a tracked flow checkpoint that uses the active stack's sandbox:** `examples/features/sandbox/active_stack_sandbox_command.py`
- **Inspect and manage past executions:** `examples/features/execution_management/client_execution_management.py`
- **Pause for human input and resume later:** `examples/features/execution_management/wait_and_resume.py`
- **Replay from a checkpoint with overrides:** `examples/features/replay/replay_with_overrides.py`
- **Track a model call inside a flow:** `examples/features/llm/flow_with_llm.py`
- **Wrap an existing PydanticAI agent:** `examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py`; watch live events with `examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py`; let a model call the active-stack sandbox with `examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py`
- **Wrap an OpenAI Agents SDK agent:** `examples/integrations/openai_agents_agent/openai_agents_adapter.py`; give it an active-stack sandbox command tool with `examples/integrations/openai_agents_agent/openai_agents_sandbox_tool.py`; watch live events with `examples/integrations/openai_agents_agent/openai_agents_streaming.py`
- **Wrap a Claude Agent SDK invocation:** `examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py`; watch live events with `examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py`
- **Record a LangGraph graph:** `examples/integrations/langgraph_v2/` runs a deterministic provider-free `StateGraph` through the repository-local v2 adapter
- **Wrap Google ADK agents:** `examples/integrations/google_adk_agent/google_adk_adapter.py` for direct wiring (`local` is deterministic/no-provider; `live` is Gemini-backed), then `examples/integrations/google_adk_agent/google_adk_workflow.py` for a local-only persisted calls-mode workflow with model/tool checkpoints. Both use an isolated no-dev `google-adk` environment.
- **Run a multi-agent OpenAI research bot:** `examples/end_to_end/openai_research_bot/research_bot.py`
- **Build a full coding agent with tool calling and HITL:** `examples/end_to_end/coding_agent/agent.py`
- **Run a granular-checkpoint PydanticAI agent end to end:** `examples/end_to_end/news_scout/scout.py`
- **Run a durable prospect-research sweep with typed qualification and HITL approval:** `examples/end_to_end/prospect_scout/prospector.py`
- **Wrap a Claude Agent SDK audit with checkpoints, partial replay, and wait/resume:** `examples/end_to_end/compliance_review/`
- **Build a sandboxed, credential-isolated, profile-gated agent harness platform (the stage-by-stage starter kit):** `examples/end_to_end/agent_harness_platform/` — see also the [docs tour](https://docs.zenml.io/user-guides/agents-guide/).
- **Connect a coding agent to Kitaru's read-only v2 MCP server:** `examples/v2/mcp/`

## Install the extras you need

```bash
uv venv && source .venv/bin/activate   # Create and activate a virtual environment
```

| Goal | Install command |
|---|---|
| Core workflow, execution, replay, and configuration examples | `uv sync --extra local` |
| LLM examples (tracked `kitaru.llm()` calls) | `uv sync --extra local --extra llm` |
| PydanticAI adapter example | `uv sync --extra local --extra pydantic-ai` |
| PydanticAI sandbox toolset example | `uv sync --extra local --extra pydantic-ai --extra openai` + `OPENAI_API_KEY` + current stack with one sandbox |
| OpenAI Agents adapter and research bot examples | `uv sync --extra local --extra openai-agents` |
| Claude Agent SDK adapter example | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or Claude SDK provider credentials |
| LangGraph v2 recording example | `uv sync`; no provider key required |
| Google ADK adapter examples | `UV_PROJECT_ENVIRONMENT=.venv-google-adk uv run --python 3.12 --no-dev --extra google-adk ...`; keep using the isolated no-dev ADK environment while local/dev extras remain intentionally blocked |
| Coding agent example | `uv sync --extra local` + model alias / provider credentials |
| Compliance review example | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or remote `anthropic` secret |
| Native v2 MCP configuration example | `uv sync --extra mcp` |

## How the examples are organized

- [features/basic_flow/README.md](features/basic_flow/README.md) — smallest flows, logging, artifacts, and runtime configuration
- [features/checkpoint_streaming/README.md](features/checkpoint_streaming/README.md) — publish best-effort live progress and custom events from checkpoints
- [features/execution_management/README.md](features/execution_management/README.md) — inspect executions, resolve waits, and resume work
- [features/replay/README.md](features/replay/README.md) — replay from a checkpoint boundary with targeted overrides
- [features/llm/README.md](features/llm/README.md) — tracked `kitaru.llm()` calls inside flows
- [features/sandbox/README.md](features/sandbox/README.md) — run one command through the active stack's sandbox from inside a tracked flow checkpoint
- [integrations/pydantic_ai_agent/README.md](integrations/pydantic_ai_agent/README.md) — wrap a PydanticAI agent with Kitaru observability
- [integrations/openai_agents_agent/README.md](integrations/openai_agents_agent/README.md) — real OpenAI API customer-support example with order lookup + shipping policy tools and Kitaru durability
- [integrations/claude_agent_sdk_agent/README.md](integrations/claude_agent_sdk_agent/README.md) — real Claude Agent SDK example showing one invocation as one Kitaru checkpoint
- [integrations/langgraph_v2/README.md](integrations/langgraph_v2/README.md) — record one deterministic provider-free `StateGraph` invocation as a Kitaru v2 session
- [integrations/google_adk_agent/README.md](integrations/google_adk_agent/README.md) — experimental Google ADK adapter examples: direct installed-ADK wiring, a persisted calls-mode workflow with explicit model/tool checkpoints, and optional Gemini runner-call mode
- [end_to_end/openai_research_bot/README.md](end_to_end/openai_research_bot/README.md) — multi-agent OpenAI research bot with planner/writer runner checkpoints, submitted search fan-out, published report artifacts, and remote secret guidance
- [end_to_end/coding_agent/README.md](end_to_end/coding_agent/README.md) — full coding agent with provider SDK tool calling, HITL, and custom materializers
- [end_to_end/news_scout/README.md](end_to_end/news_scout/README.md) — agentic news monitor with granular per-tool checkpoints, CLI/default interests, and `secret_environment_from` for remote API keys
- [end_to_end/prospect_scout/README.md](end_to_end/prospect_scout/README.md) — durable prospect-research sweep: one checkpoint per company (crash-resumable via `kitaru executions retry`), enum-typed PydanticAI qualification, `kitaru.wait()` shortlist approval, and per-prospect outreach drafts
- [end_to_end/compliance_review/README.md](end_to_end/compliance_review/README.md) — Claude Agent SDK document audit in three runnable stages: crash-resilient turns, sequential domain checkpoints with partial replay, and durable wait/resume conversation
- [end_to_end/agent_harness_platform/README.md](end_to_end/agent_harness_platform/README.md) — stage-by-stage starter kit for an internal agent harness platform: durable PydanticAI agent → DockerSandbox → skills as markdown → credential proxy → typed services → HITL via `kitaru.wait()`. See the [docs tour](https://docs.zenml.io/user-guides/agents-guide/) for the polished surface.
- [v2/mcp/README.md](v2/mcp/README.md) — configure the native v2 MCP server in read-only mode

## Core workflow basics

| Example | Run | Requires | What it demonstrates | Docs | Test |
|---|---|---|---|---|---|
| [Basic flow](features/basic_flow/first_working_flow.py) | `uv run python examples/features/basic_flow/first_working_flow.py` | `uv sync --extra local` | The smallest end-to-end `@flow` + `@checkpoint` workflow | [Quickstart](https://docs.zenml.io/kitaru/getting-started/quickstart) | — |
| [Structured logging](features/basic_flow/flow_with_logging.py) | `uv run python examples/features/basic_flow/flow_with_logging.py` | `uv sync --extra local` | `kitaru.log()` metadata at both flow and checkpoint scope | [Execution Management](https://docs.zenml.io/kitaru/guides/execution-management) | — |
| [Checkpoint live events](features/checkpoint_streaming/checkpoint_streaming.py) | `uv run python examples/features/checkpoint_streaming/checkpoint_streaming.py` | `uv sync --extra local` | `kitaru.progress()` and `kitaru.events.publish()` from checkpoint bodies | [Checkpoint Live Events](https://docs.zenml.io/kitaru/guides/checkpoint-streaming) | — |
| [Artifacts](features/basic_flow/flow_with_artifacts.py) | `uv run python examples/features/basic_flow/flow_with_artifacts.py` | `uv sync --extra local` | `kitaru.save()` and `kitaru.load()` across executions | [Artifacts](https://docs.zenml.io/kitaru/guides/artifacts) | — |
| [Configuration](features/basic_flow/flow_with_configuration.py) | `uv run python examples/features/basic_flow/flow_with_configuration.py` | `uv sync --extra local` | `kitaru.configure()` defaults, overrides, and frozen execution specs | [Configuration](https://docs.zenml.io/kitaru/guides/configuration) | — |
| [Checkpoint runtime](features/basic_flow/flow_with_checkpoint_runtime.py) | `uv run python examples/features/basic_flow/flow_with_checkpoint_runtime.py` | `uv sync --extra local` | `@checkpoint(runtime="isolated")` with `.submit()` fan-out | [Checkpoints](https://docs.zenml.io/kitaru/concepts/checkpoints) | — |
| [Active stack sandbox command](features/sandbox/active_stack_sandbox_command.py) | `uv run python examples/features/sandbox/active_stack_sandbox_command.py` | `uv sync --extra local` + active stack with a sandbox | A tracked `@flow` + `@checkpoint` that calls `kitaru.run_sandbox_command(...)` using the active stack's sandbox component | [Stacks](https://docs.zenml.io/kitaru/stacks) | — |

## Execution lifecycle and recovery

| Example | What it demonstrates | Docs |
|---|---|---|
| [Execution management](features/execution_management/client_execution_management.py) | `KitaruClient` for listing runs, reading details, and loading artifacts | [Execution Management](https://docs.zenml.io/kitaru/guides/execution-management) |
| [Wait and resume](features/execution_management/wait_and_resume.py) | `kitaru.wait()` — pause for human input, resume later | [Wait and Resume](https://docs.zenml.io/kitaru/guides/wait-and-resume) |
| [Replay with overrides](features/replay/replay_with_overrides.py) | Replay from a checkpoint boundary while overriding selected inputs | [Replay and Overrides](https://docs.zenml.io/kitaru/guides/replay-and-overrides) |

## LLMs and agent integrations

| Example | Run | Requires | What it demonstrates | Docs | Test |
|---|---|---|---|---|---|
| [Tracked LLM calls](features/llm/flow_with_llm.py) | `uv run python examples/features/llm/flow_with_llm.py` | `uv sync --extra local` + model alias / provider credentials | `kitaru.llm()` prompt-response tracking with usage metadata | [Tracked LLM Calls](https://docs.zenml.io/kitaru/guides/llm-calls) | — |
| [PydanticAI adapter](integrations/pydantic_ai_agent/pydantic_ai_adapter.py) | `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py` | `uv sync --extra local --extra pydantic-ai` | Wrap an existing PydanticAI agent while keeping a Kitaru replay boundary | [PydanticAI Adapter](https://docs.zenml.io/kitaru/adapters/pydantic-ai/) | — |
| [PydanticAI streaming](integrations/pydantic_ai_agent/pydantic_ai_streaming.py) | `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py` | `uv sync --extra local --extra pydantic-ai --extra openai` + `OPENAI_API_KEY` | Watch best-effort `pydantic_ai.stream.*` live events while `.wait()` returns the durable final answer | [PydanticAI Adapter](https://docs.zenml.io/kitaru/adapters/pydantic-ai/#streaming) | — |
| [PydanticAI sandbox toolset](integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py) | `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py` | `uv sync --extra local --extra pydantic-ai --extra openai` + `OPENAI_API_KEY` + current stack with one sandbox | Let a PydanticAI model call `run_sandbox_command`; the dashboard shows the per-tool `run_sandbox_command_tool` checkpoint before the final answer checkpoint | [PydanticAI Adapter](https://docs.zenml.io/kitaru/adapters/pydantic-ai/) | — |
| [OpenAI Agents adapter](integrations/openai_agents_agent/openai_agents_adapter.py) | `uv run python examples/integrations/openai_agents_agent/openai_agents_adapter.py` | `uv sync --extra local --extra openai-agents` + `OPENAI_API_KEY` | Real OpenAI API customer-support flow with tool calls, showing call-level vs runner-call durability | [OpenAI Agents Adapter](https://docs.zenml.io/kitaru/adapters/openai-agents/) | — |
| [OpenAI Agents sandbox tool](integrations/openai_agents_agent/openai_agents_sandbox_tool.py) | `uv run python examples/integrations/openai_agents_agent/openai_agents_sandbox_tool.py` | `uv sync --extra local --extra openai-agents` + `OPENAI_API_KEY` + current stack with one sandbox | Let an OpenAI agent call `kitaru_sandbox_command`, which runs a command through your current stack's sandbox and returns compact JSON | [OpenAI Agents sandbox tool](../docs/book/adapters/openai-agents.md#sandbox-command-tool) | — |
| [OpenAI Agents streaming](integrations/openai_agents_agent/openai_agents_streaming.py) | `uv run python examples/integrations/openai_agents_agent/openai_agents_streaming.py` | `uv sync --extra local --extra openai-agents` + `OPENAI_API_KEY` | Watch best-effort `openai_agents.stream.*` live events while `.wait()` returns the durable `OpenAIRunResult` | [OpenAI Agents Adapter](https://docs.zenml.io/kitaru/adapters/openai-agents/#streaming-with-kitaru-durability) | — |
| [Claude Agent SDK adapter](integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py) | `uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py` | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or Claude SDK provider credentials | Real Claude SDK invocation wrapped by `KitaruClaudeRunner`; one invocation becomes one Kitaru checkpoint with final text, session, usage/cost, and audit artifacts | [Claude Agent SDK Adapter](https://docs.zenml.io/kitaru/adapters/claude-agent-sdk/) | — |
| [Claude Agent SDK streaming](integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py) | `uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py` | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or Claude SDK provider credentials | Watch best-effort `claude_agent_sdk.stream.*` live events while `.wait()` returns the durable `ClaudeRunResult` | [Claude Agent SDK Adapter](https://docs.zenml.io/kitaru/adapters/claude-agent-sdk/#live-streaming-with-kitaru-durability) | — |
| [Gemini Interactions adapter](integrations/gemini_interactions_agent/gemini_interactions_adapter.py) | `uv run python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run` | `uv sync --extra local --extra gemini` for real calls; dry-run needs no provider key | Wrap one Gemini Interactions API response as one Kitaru checkpoint, with direct model streaming and Antigravity background same-id observation/polling | [Gemini Interactions Adapter](https://docs.zenml.io/kitaru/adapters/gemini-interactions/) | — |
| [Google ADK adapter](integrations/google_adk_agent/google_adk_adapter.py) | `UV_PROJECT_ENVIRONMENT=.venv-google-adk uv run --python 3.12 --no-dev --extra google-adk python examples/integrations/google_adk_agent/google_adk_adapter.py` | isolated no-dev `google-adk`; optional `GEMINI_API_KEY` or `GOOGLE_API_KEY` for `--mode live` | Experimental ADK runner-call result capture plus explicit ADK model/tool wrappers in a deterministic local run | [Google ADK Adapter](../docs/book/adapters/google-adk.md) | — |
| [Google ADK workflow](integrations/google_adk_agent/google_adk_workflow.py) | `UV_PROJECT_ENVIRONMENT=.venv-google-adk uv run --python 3.12 --no-dev --extra google-adk python examples/integrations/google_adk_agent/google_adk_workflow.py` | isolated no-dev `google-adk` | Persisted Kitaru flow using `checkpoint_strategy="calls"`, explicit `KitaruADKModel` / `KitaruADKTool`, deterministic tool-confirmation resume, and structured output | [Google ADK Adapter](../docs/book/adapters/google-adk.md) | — |
| [LangGraph v2 recording](integrations/langgraph_v2/README.md) | `uv run python -m examples.integrations.langgraph_v2` | `uv sync` + configured Kitaru v2 server and existing agent or agent-version ID; no provider key | Record a deterministic `StateGraph` invocation, its input and output, and observable public graph callbacks | [LangGraph Adapter](https://docs.zenml.io/kitaru/adapters/langgraph/) | `plugins/tests/adapters/langgraph/test_compatibility.py` |
| [OpenAI research bot](end_to_end/openai_research_bot/research_bot.py) | `cd examples/end_to_end/openai_research_bot && uv run python research_bot.py "Your query" --max-searches 2` | `uv sync --extra local --extra openai-agents` + local `OPENAI_API_KEY` or remote `openai-research-bot-keys` secret | Planner → submitted search fan-out → writer report using `KitaruRunner(..., checkpoint_strategy="runner_call")`; publishes `research_plan`, `search_summaries`, and `final_report` artifacts | [OpenAI Agents Adapter](https://docs.zenml.io/kitaru/adapters/openai-agents/) | — |
| [Coding agent](end_to_end/coding_agent/agent.py) | `cd examples/end_to_end/coding_agent && uv run python agent.py "Your task"` | `uv sync --extra local` + model alias / provider credentials | Full agent loop with provider SDK tool calling, `kitaru.wait()` HITL, custom materializers, and artifact persistence | [Tracked LLM Calls](https://docs.zenml.io/kitaru/guides/llm-calls) | — |
| [News scout](end_to_end/news_scout/scout.py) | `cd examples/end_to_end/news_scout && uv run python scout.py` | `uv sync --extra local --extra pydantic-ai --extra llm` + `ANTHROPIC_API_KEY` locally (or a `news-scout-keys` secret for remote stacks) | PydanticAI agent with `checkpoint_strategy="calls"` — every model/tool call is its own Kitaru checkpoint; `publish_report` promotes the agent output to a named `final_report` artifact; `ImageSettings.secret_environment_from` attaches the provider-keys secret automatically when the active stack is remote | [Examples index](https://docs.zenml.io/kitaru/getting-started/examples) | — |
| [Prospect scout](end_to_end/prospect_scout/prospector.py) | `cd examples/end_to_end/prospect_scout && uv run python prospector.py` | `uv sync --extra local --extra pydantic-ai` + `OPENAI_API_KEY` (`EXA_API_KEY` optional; `PROSPECT_SCOUT_MODEL=test` runs keyless) | Durable research sweep — one checkpoint per company so `kitaru executions retry` resumes a crashed run without repeating finished searches; enum-typed PydanticAI qualification; `kitaru.wait()` shortlist approval before per-prospect outreach drafts | [Wait and Resume](https://docs.zenml.io/kitaru/guides/wait-and-resume) | — |
| [Compliance review](end_to_end/compliance_review/README.md) | `uv run python examples/end_to_end/compliance_review/stage_1_single_turn.py` | `uv sync --extra local --extra claude-agent-sdk` + local `ANTHROPIC_API_KEY` or remote `anthropic` secret | Three runnable Claude Agent SDK stages: checkpointed turns, sequential domain checkpoints with partial replay, and durable wait/resume conversation | [Replay and Overrides](https://docs.zenml.io/kitaru/guides/replay-and-overrides) | — |
| [Native v2 MCP configuration](v2/mcp/README.md) | Configure `.mcp.json.example` in an MCP client | `uv sync --extra mcp` | Inspect the v2 registry and activity surface with a read-only server | [MCP Server](../docs/book/agent-native/mcp-server.md) | `tests/mcp/test_installed_contract.py` |

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
9. `uv run python examples/features/sandbox/active_stack_sandbox_command.py` *(requires an active stack with a sandbox; `uv run kitaru stack create sandbox-demo` creates a local one)*
10. `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py`
11. `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py` *(PydanticAI live events with durable `.wait()` result; requires `OPENAI_API_KEY`)*
12. `uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py` *(PydanticAI model calls the active-stack sandbox; requires `OPENAI_API_KEY` and one sandbox component)*
13. `uv run python examples/integrations/openai_agents_agent/openai_agents_adapter.py`
14. `uv run python examples/integrations/openai_agents_agent/openai_agents_sandbox_tool.py` *(OpenAI agent calls the active stack's sandbox command tool; requires `OPENAI_API_KEY` and one active sandbox)*
15. `uv run python examples/integrations/openai_agents_agent/openai_agents_streaming.py` *(OpenAI Agents live events with durable `OpenAIRunResult`; requires `OPENAI_API_KEY`)*
16. `uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py` *(Claude SDK invocation-level checkpoint)*
17. `uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py` *(Claude live events with durable `ClaudeRunResult`; requires Claude credentials)*
18. `UV_PROJECT_ENVIRONMENT=.venv-google-adk uv run --python 3.12 --no-dev --extra google-adk python examples/integrations/google_adk_agent/google_adk_adapter.py` *(Google ADK direct local mode; isolated ADK environment)*
19. `UV_PROJECT_ENVIRONMENT=.venv-google-adk uv run --python 3.12 --no-dev --extra google-adk python examples/integrations/google_adk_agent/google_adk_workflow.py` *(Google ADK persisted workflow with explicit model/tool checkpoints and deterministic tool-confirmation resume)*
20. `uv run python -m examples.integrations.langgraph_v2` *(deterministic LangGraph v2 session recording; no provider key)*
21. `cd examples/end_to_end/openai_research_bot && uv run python research_bot.py "Your query" --max-searches 2` *(OpenAI planner → submitted searches → writer report)*
22. `cd examples/end_to_end/coding_agent && uv run python agent.py "Your task"` *(full agent with tools + HITL)*
23. `cd examples/end_to_end/news_scout && uv run python scout.py` *(granular-checkpoint agent with 4 tools, dashboard-readable final_report artifact)*
24. `uv run python examples/end_to_end/compliance_review/stage_1_single_turn.py` *(Claude Agent SDK audit; walk through Stage 1, Stage 2, and conversational Stage 4 to see checkpointing, replay, and wait/resume in turn)*
25. Configure `examples/v2/mcp/.mcp.json.example` and inspect one page with the read-only native MCP server

If you prefer the hosted docs view, start with the
[Examples page](https://docs.zenml.io/kitaru/getting-started/examples).
