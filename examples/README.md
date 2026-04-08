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
- **Run checkpoints in isolated containers with fan-out:** `basic_flow/flow_with_checkpoint_runtime.py`
- **Inspect and manage past executions:** `execution_management/client_execution_management.py`
- **Pause for human input and resume later:** `execution_management/wait_and_resume.py`
- **Replay from a checkpoint with overrides:** `replay/replay_with_overrides.py`
- **Track a model call inside a flow:** `llm/flow_with_llm.py`
- **Wrap an existing PydanticAI agent:** `pydantic_ai_agent/pydantic_ai_adapter.py`
- **Build a full coding agent with tool calling and HITL:** `coding_agent/agent.py`
- **Explore Kitaru through MCP tools:** `mcp/mcp_query_tools.py`

## Install the extras you need

```bash
uv venv && source .venv/bin/activate   # Create and activate a virtual environment
```

| Goal | Install command |
|---|---|
| Core workflow, execution, replay, and configuration examples | `uv sync --extra local` |
| LLM examples (tracked `kitaru.llm()` calls) | `uv sync --extra local --extra llm` |
| PydanticAI adapter example | `uv sync --extra local --extra pydantic-ai` |
| Coding agent example | `uv sync --extra local` + model alias / provider credentials |
| MCP query tools example | `uv sync --extra local --extra mcp` |

## How the examples are organized

- [basic_flow/](basic_flow/README.md) — smallest flows, logging, artifacts, and runtime configuration
- [execution_management/](execution_management/README.md) — inspect executions, resolve waits, and resume work
- [replay/](replay/README.md) — replay from a checkpoint boundary with targeted overrides
- [llm/](llm/README.md) — tracked `kitaru.llm()` calls inside flows
- [pydantic_ai_agent/](pydantic_ai_agent/README.md) — wrap a PydanticAI agent with Kitaru observability
- [coding_agent/](coding_agent/README.md) — full coding agent with provider SDK tool calling, HITL, and custom materializers
- [mcp/](mcp/README.md) — inspect flows with the Kitaru MCP server

## Core workflow basics

| Example | Run | Requires | What it demonstrates | Docs | Test |
|---|---|---|---|---|---|
| [Basic flow](basic_flow/first_working_flow.py) | `uv run examples/basic_flow/first_working_flow.py` | `uv sync --extra local` | The smallest end-to-end `@flow` + `@checkpoint` workflow | [Quickstart](https://kitaru.ai/docs/getting-started/quickstart) | [tests/test_phase5_example.py](../tests/test_phase5_example.py) |
| [Structured logging](basic_flow/flow_with_logging.py) | `uv run examples/basic_flow/flow_with_logging.py` | `uv sync --extra local` | `kitaru.log()` metadata at both flow and checkpoint scope | [Execution Management](https://kitaru.ai/docs/getting-started/execution-management) | [tests/test_phase7_logging_example.py](../tests/test_phase7_logging_example.py) |
| [Artifacts](basic_flow/flow_with_artifacts.py) | `uv run examples/basic_flow/flow_with_artifacts.py` | `uv sync --extra local` | `kitaru.save()` and `kitaru.load()` across executions | [Artifacts](https://kitaru.ai/docs/getting-started/artifacts) | [tests/test_phase8_artifacts_example.py](../tests/test_phase8_artifacts_example.py) |
| [Configuration](basic_flow/flow_with_configuration.py) | `uv run examples/basic_flow/flow_with_configuration.py` | `uv sync --extra local` | `kitaru.configure()` defaults, overrides, and frozen execution specs | [Configuration](https://kitaru.ai/docs/getting-started/configuration) | [tests/test_phase10_configuration_example.py](../tests/test_phase10_configuration_example.py) |
| [Checkpoint runtime](basic_flow/flow_with_checkpoint_runtime.py) | `uv run examples/basic_flow/flow_with_checkpoint_runtime.py` | `uv sync --extra local` | `@checkpoint(runtime="isolated")` with `.submit()` fan-out | [Checkpoints](https://kitaru.ai/docs/concepts/checkpoints) | — |

## Execution lifecycle and recovery

| Example | What it demonstrates | Docs |
|---|---|---|
| [Execution management](execution_management/client_execution_management.py) | `KitaruClient` for listing runs, reading details, and loading artifacts | [Execution Management](https://kitaru.ai/docs/guides/execution-management) |
| [Wait and resume](execution_management/wait_and_resume.py) | `kitaru.wait()` — pause for human input, resume later | [Wait and Resume](https://kitaru.ai/docs/guides/wait-and-resume) |
| [Replay with overrides](replay/replay_with_overrides.py) | Replay from a checkpoint boundary while overriding selected inputs | [Replay and Overrides](https://kitaru.ai/docs/guides/replay-and-overrides) |

## LLMs and agent integrations

| Example | Run | Requires | What it demonstrates | Docs | Test |
|---|---|---|---|---|---|
| [Tracked LLM calls](llm/flow_with_llm.py) | `uv run examples/llm/flow_with_llm.py` | `uv sync --extra local` + model alias / provider credentials | `kitaru.llm()` prompt-response tracking with usage metadata | [Tracked LLM Calls](https://kitaru.ai/docs/getting-started/llm-calls) | [tests/test_phase12_llm_example.py](../tests/test_phase12_llm_example.py) |
| [PydanticAI adapter](pydantic_ai_agent/pydantic_ai_adapter.py) | `uv run examples/pydantic_ai_agent/pydantic_ai_adapter.py` | `uv sync --extra local --extra pydantic-ai` | Wrap an existing PydanticAI agent while keeping a Kitaru replay boundary | [PydanticAI Adapter](https://kitaru.ai/docs/getting-started/pydantic-ai-adapter) | [tests/test_phase17_pydantic_ai_example.py](../tests/test_phase17_pydantic_ai_example.py) |
| [Coding agent](coding_agent/agent.py) | `cd examples/coding_agent && uv run python agent.py "Your task"` | `uv sync --extra local` + model alias / provider credentials | Full agent loop with provider SDK tool calling, `kitaru.wait()` HITL, custom materializers, and artifact persistence | [Tracked LLM Calls](https://kitaru.ai/docs/getting-started/llm-calls) | — |
| [MCP query tools](mcp/mcp_query_tools.py) | `uv run examples/mcp/mcp_query_tools.py` | `uv sync --extra local --extra mcp` | Query executions and artifacts through the Kitaru MCP server | [Execution Management](https://kitaru.ai/docs/getting-started/execution-management) | [tests/mcp/test_phase19_mcp_example.py](../tests/mcp/test_phase19_mcp_example.py) |

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
