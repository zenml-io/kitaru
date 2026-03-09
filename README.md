# Kitaru

Durable execution for AI agents, built on [ZenML](https://zenml.io).

Kitaru makes agent workflows **persistent, replayable, and observable** using a small set of Python primitives. No graph DSL, no framework lock-in — just decorators on your existing code.

## Features

Kitaru is under active development. The core SDK primitives are implemented and functional:

- **Flows and checkpoints** — `@flow` and `@checkpoint` decorators for durable, replayable workflows with concurrent execution via `.submit()` / `.result()`
- **Artifact persistence** — `kitaru.save()` / `kitaru.load()` for explicit artifact storage and cross-execution reuse inside checkpoints
- **Structured logging** — `kitaru.log()` attaches metadata to executions and checkpoints, with configurable runtime log backends (`kitaru log-store ...`)
- **Configuration** — `kitaru.configure(...)`, environment variables, and `[tool.kitaru]` in `pyproject.toml`, with precedence resolved at flow start and persisted per execution
- **Execution management** — `KitaruClient` for inspecting executions (`get`, `list`, `latest`), replaying from checkpoints/waits (`replay`), same-execution recovery (`retry`), cancellation (`cancel`), and artifact browsing/loading
- **Secrets** — `kitaru secrets set/show/list/delete` for managing credentials (private by default, create-or-update semantics)
- **LLM calls** — `kitaru.llm()` with LiteLLM backend, automatic prompt/response capture, usage/cost/latency metadata, and local model aliases (`kitaru model register/list`)
- **Error handling** — Typed exception hierarchy (`KitaruContextError`, `KitaruExecutionError`, `KitaruUserCodeError`, etc.) with failure journaling via `execution.failure` and per-checkpoint `checkpoint.attempts`
- **Execution CLI** — `kitaru run`, `kitaru executions get/list/input/replay/retry/resume/cancel` for full lifecycle management from the terminal
- **Durable wait/resume** — `kitaru.wait(...)` pauses a flow until external input arrives via `client.executions.input(...)` / `client.executions.resume(...)`
- **Framework adapters** — `kitaru.adapters.pydantic_ai.wrap(agent)` tracks model requests and tool calls under the enclosing checkpoint (or a synthetic flow-scope checkpoint for `run()` / `run_sync()`), with per-tool capture modes (`full`, `metadata_only`, `off`) and HITL support via `hitl_tool(...)`
- **Agent-native integrations** — Optional MCP server (`kitaru-mcp`) with execution/artifact/status query tools, plus a Claude Code authoring skill available via the plugin marketplace

### SDK primitives

```python
from kitaru import checkpoint, flow

@checkpoint
def fetch_data(url: str) -> str:
    """A checkpoint is a unit of work whose outcome is persisted."""
    _ = url
    return "some data"

@checkpoint
def process_data(data: str) -> str:
    """Checkpoints are composed inside a flow."""
    return data.upper()

@flow
def my_agent(url: str) -> str:
    """A flow is the outer durable execution boundary."""
    data = fetch_data(url)
    return process_data(data)

# Run and wait for result
result = my_agent.run("https://example.com").wait()
print(result)  # SOME DATA
```

#### Concurrent checkpoints

Checkpoints support `.submit()` for concurrent execution inside a flow:

```python
@flow
def parallel_agent(urls: list[str]) -> list[str]:
    futures = [fetch_data.submit(url) for url in urls]
    return [f.result() for f in futures]
```

### Examples

The `examples/` directory contains runnable workflows showcasing each feature. Install dependencies first:

```bash
uv sync --extra local              # Core examples
uv sync --extra local --extra mcp  # MCP server example
```

| Example | File | What it demonstrates |
|---|---|---|
| Basic flow | `examples/first_working_flow.py` | `@flow` / `@checkpoint` decorators, sync execution |
| Artifact save/load | `examples/flow_with_artifacts.py` | `kitaru.save()` / `kitaru.load()` inside checkpoints |
| Structured logging | `examples/flow_with_logging.py` | `kitaru.log()` metadata on executions and checkpoints |
| Configuration | `examples/flow_with_configuration.py` | `kitaru.configure()` with precedence resolution |
| Execution management | `examples/client_execution_management.py` | `KitaruClient` for inspecting and managing executions |
| LLM calls | `examples/flow_with_llm.py` | `kitaru.llm()` with model aliases and metadata capture |
| Wait/resume | `examples/wait_and_resume.py` | `kitaru.wait()` and external input via client |
| Replay/overrides | `examples/replay_with_overrides.py` | replay from checkpoint boundaries with `checkpoint.*` overrides |
| PydanticAI adapter | `examples/pydantic_ai_adapter.py` | `wrap(agent)` with child-event lineage, run summaries, and capture policy |
| MCP query tools | `examples/mcp_query_tools.py` | MCP server execution/artifact query tools |

Run any example with:

```bash
uv run python -m examples.<module_name>
```

For the LLM example, register a model alias and set your API key first:

```bash
kitaru model register fast --model openai/gpt-4o-mini
export OPENAI_API_KEY=sk-...
```

### CLI

```
kitaru --version              Show the SDK version
kitaru --help                 Show available commands
kitaru-mcp                    Run the MCP server (requires kitaru[mcp])

kitaru login <server>         Connect to a Kitaru server
kitaru login <server> --api-key <key>
kitaru logout                 Log out and clear stored auth state
kitaru status                 Show connection state and active stack
kitaru info                   Show detailed environment information

kitaru run <target> --args <json> [--stack <name>]
kitaru executions get <exec_id>
kitaru executions list [--status <status>] [--flow <flow>] [--limit <n>]
kitaru executions input <exec_id> --wait <wait_name_or_id> --value <json>
kitaru executions replay <exec_id> --from <selector> [--args <json>] [--overrides <json>]
kitaru executions resume <exec_id>
kitaru executions retry <exec_id>
kitaru executions cancel <exec_id>

kitaru stack list             List visible stacks
kitaru stack current          Show the active stack
kitaru stack use <name-or-id> Switch active stack

kitaru log-store show         Show effective global runtime log backend
kitaru log-store set <backend> --endpoint <url> [--api-key <secret>]
kitaru log-store reset        Clear global runtime log backend override

kitaru secrets set <name> --KEY=value [--KEY=value ...]
kitaru secrets show <name-or-id> [--show-values]
kitaru secrets list
kitaru secrets delete <name-or-id>

kitaru model register <alias> --model <provider/model> [--secret <name-or-id>]
kitaru model list
```

### Primitives still in progress

| Primitive | Purpose |
|---|---|
| `kitaru executions logs ...` | Backend-agnostic execution log streaming |

## Development

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), and [just](https://github.com/casey/just).

```bash
uv sync                            # Install dependencies
uv sync --extra local              # Include local ZenML runtime components
uv sync --extra mcp                # Include MCP server dependencies
uv sync --extra local --extra mcp  # Local runtime + MCP tools
just --list                        # Show all available recipes
just check             # Run all checks (format, lint, typecheck, typos, yaml)
just test              # Run tests
just fix               # Auto-fix formatting, lint, and yaml
just build             # Build wheel + sdist locally
```

Typo checking uses [`typos`](https://github.com/crate-ci/typos) (config in `.typos.toml`, run via `just typos`).

### Contributing

The default branch is `develop` — all PRs should target it. `main` only contains released versions and is updated automatically by the release workflow.

### Claude Code skills

Install the official Astral skills for ty, ruff and uv:

```shell
/plugin marketplace add astral-sh/claude-code-plugins
/plugin install astral@astral-sh
```
