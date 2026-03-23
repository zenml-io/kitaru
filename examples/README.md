# Kitaru examples

Welcome — this folder is the fastest way to see what Kitaru feels like in real
code.

Each example group lives in its own directory. To run any example:

```bash
cd examples/<group>
kitaru init                  # One-time: initialize a Kitaru project
python <module_name>.py      # Run the example
```

Examples use your current Kitaru connection context. If you want them to use a
deployed Kitaru server, connect first with `kitaru login <server>` and confirm
with `kitaru status`. If you are just trying Kitaru locally, run them as-is.

## Start here if you want to...

- **Run the smallest possible durable flow:** `basic_flow/first_working_flow.py`
- **See structured metadata logging:** `basic_flow/flow_with_logging.py`
- **Persist and reload artifacts:** `basic_flow/flow_with_artifacts.py`
- **Inspect and manage past executions:** `execution_management/client_execution_management.py`
- **Pause for human input and resume later:** `execution_management/wait_and_resume.py`
- **Replay from a checkpoint with overrides:** `replay/replay_with_overrides.py`
- **Track a model call inside a flow:** `llm/flow_with_llm.py`
- **Wrap an existing PydanticAI agent:** `pydantic_ai_agent/pydantic_ai_adapter.py`
- **Build a full coding agent with tool calling and HITL:** `coding_agent/agent.py`
- **Explore Kitaru through MCP tools:** `mcp/mcp_query_tools.py`

## Install the extras you need

| Goal | Install command |
|---|---|
| Core workflow, execution, replay, configuration, and LLM examples | `uv sync --extra local` |
| PydanticAI adapter example | `uv sync --extra local --extra pydantic-ai` |
| MCP query tools example | `uv sync --extra local --extra mcp` |

## How the examples are organized

- [basic_flow/](basic_flow/README.md) — smallest flows, logging, artifacts, and runtime configuration
- [execution_management/](execution_management/README.md) — inspect executions, resolve waits, and resume work
- [replay/](replay/README.md) — replay from a checkpoint boundary with targeted overrides
- [llm/](llm/README.md) — tracked `kitaru.llm()` calls inside flows
- [pydantic_ai_agent/](pydantic_ai_agent/README.md) — wrap a PydanticAI agent with Kitaru observability
- [coding_agent/](coding_agent/README.md) — full coding agent with LiteLLM tool calling, HITL, and custom materializers
- [mcp/](mcp/README.md) — inspect flows with the Kitaru MCP server

## Core workflow basics

| Example | What it demonstrates | Docs |
|---|---|---|
| [Basic flow](basic_flow/first_working_flow.py) | The smallest end-to-end `@flow` + `@checkpoint` workflow | [Quickstart](https://kitaru.ai/docs/getting-started/quickstart) |
| [Structured logging](basic_flow/flow_with_logging.py) | `kitaru.log()` metadata at both flow and checkpoint scope | [Execution Management](https://kitaru.ai/docs/getting-started/execution-management) |
| [Artifacts](basic_flow/flow_with_artifacts.py) | `kitaru.save()` and `kitaru.load()` across executions | [Artifacts](https://kitaru.ai/docs/getting-started/artifacts) |
| [Configuration](basic_flow/flow_with_configuration.py) | `kitaru.configure()` defaults, overrides, and frozen execution specs | [Configuration](https://kitaru.ai/docs/getting-started/configuration) |

## Execution lifecycle and recovery

| Example | What it demonstrates | Docs |
|---|---|---|
| [Execution management](execution_management/client_execution_management.py) | `KitaruClient` for listing runs, reading details, and loading artifacts | [Execution Management](https://kitaru.ai/docs/getting-started/execution-management) |
| [Wait and resume](execution_management/wait_and_resume.py) | `kitaru.wait()` — pause for human input, resume later | [Wait and Resume](https://kitaru.ai/docs/getting-started/wait-and-resume) |
| [Replay with overrides](replay/replay_with_overrides.py) | Replay from a checkpoint boundary while overriding selected inputs | [Replay and Overrides](https://kitaru.ai/docs/getting-started/replay-and-overrides) |

## LLMs and agent integrations

| Example | What it demonstrates | Docs |
|---|---|---|
| [Tracked LLM calls](llm/flow_with_llm.py) | `kitaru.llm()` prompt-response tracking with usage metadata | [Tracked LLM Calls](https://kitaru.ai/docs/getting-started/llm-calls) |
| [PydanticAI adapter](pydantic_ai_agent/pydantic_ai_adapter.py) | Wrap an existing PydanticAI agent while keeping a Kitaru replay boundary | [PydanticAI Adapter](https://kitaru.ai/docs/getting-started/pydantic-ai-adapter) |
| [Coding agent](coding_agent/agent.py) | Full agent loop with LiteLLM tool calling, HITL, and artifact persistence | [Tracked LLM Calls](https://kitaru.ai/docs/getting-started/llm-calls) |
| [MCP query tools](mcp/mcp_query_tools.py) | Query executions and artifacts through the Kitaru MCP server | [Execution Management](https://kitaru.ai/docs/getting-started/execution-management) |

## Recommended learning path

If you are new to Kitaru, this is the smoothest path:

1. `cd basic_flow && kitaru init && python first_working_flow.py`
2. `python flow_with_logging.py`
3. `python flow_with_artifacts.py`
4. `cd ../execution_management && kitaru init && python client_execution_management.py`
5. `python wait_and_resume.py`
6. `cd ../replay && kitaru init && python replay_with_overrides.py`
7. `cd ../llm && kitaru init && python flow_with_llm.py`
8. `cd ../pydantic_ai_agent && kitaru init && python pydantic_ai_adapter.py`
9. `cd ../coding_agent && kitaru init && python agent.py "Your task"` *(full agent with tools + HITL)*
10. `cd ../mcp && kitaru init && python mcp_query_tools.py`

If you prefer the hosted docs view, start with the
[Examples page](https://kitaru.ai/docs/getting-started/examples).
