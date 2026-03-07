# Kitaru

Durable execution for AI agents, built on [ZenML](https://zenml.io).

Kitaru makes agent workflows **persistent, replayable, and observable** using a small set of Python primitives. No graph DSL, no framework lock-in — just decorators on your existing code.

## What works today

Kitaru is under active development. The core flow and checkpoint decorators are implemented and functional, the Phase 5 first working workflow milestone is complete, `kitaru.log()` attaches structured metadata to executions/checkpoints, and `kitaru.save()` / `kitaru.load()` support explicit artifact persistence and cross-execution reuse inside checkpoints. Runtime log storage also has a global default/override model via `kitaru log-store ...`, and you can inspect/switch your active stack with `kitaru stack ...` (or `kitaru.list_stacks()`, `kitaru.current_stack()`, `kitaru.use_stack()`).

Phase 10 configuration is now implemented via `kitaru.configure(...)`, environment variables, and project-level `[tool.kitaru]` settings in `pyproject.toml`, with precedence resolved at flow start time and persisted as a frozen execution spec on each run.

Phase 11 introduces the first real `KitaruClient` surface for execution management. You can now inspect executions (`get`, `list`, `latest`), perform same-execution recovery (`retry`), cancel running executions (`cancel`), and browse/load artifacts (`client.artifacts.list/get`, `artifact.load()`). `client.executions.input(...)` and replay are still intentionally deferred.

Phase 11.5 adds a Kitaru secrets CLI surface: `kitaru secrets set/show/list/delete`. Secrets are private by default, `set` behaves as create-or-update, and key names should use env-var style identifiers such as `OPENAI_API_KEY`.

Phase 12 adds `kitaru.llm()` with LiteLLM as the backend engine, automatic prompt/response artifact capture, usage/cost/latency metadata logging, and local model alias registration (`kitaru model register/list`) with optional secret-backed credential lookup.

Phase 13 adds a typed Kitaru error hierarchy (`KitaruContextError`, `KitaruExecutionError`, `KitaruUserCodeError`, etc.), clearer runtime-vs-user-code failure surfacing, and failure journaling in `KitaruClient` via `execution.failure` plus per-checkpoint attempt history in `checkpoint.attempts`.

### SDK primitives

```python
import kitaru

@kitaru.checkpoint
def fetch_data(url: str) -> str:
    """A checkpoint is a unit of work whose outcome is persisted."""
    _ = url
    return "some data"

@kitaru.checkpoint
def process_data(data: str) -> str:
    """Checkpoints are composed inside a flow."""
    return data.upper()

@kitaru.flow
def my_agent(url: str) -> str:
    """A flow is the outer durable execution boundary."""
    data = fetch_data(url)
    return process_data(data)

# Run synchronously — blocks until complete, returns the result
result = my_agent("https://example.com")
print(result)  # SOME DATA
```

#### Concurrent checkpoints

Checkpoints support `.submit()` for concurrent execution inside a flow:

```python
@kitaru.flow
def parallel_agent(urls: list[str]) -> list[str]:
    futures = [fetch_data.submit(url) for url in urls]
    return [f.result() for f in futures]
```

### Run the first working workflow

The repository includes a runnable Phase 5 example at
`examples/first_working_flow.py`.

```bash
uv sync --extra local
uv run python -m examples.first_working_flow
```

Expected output:

```text
SOME DATA
```

You can also run the integration test for this example:

```bash
uv run pytest tests/test_phase5_example.py
```

### Run the configuration workflow

The repository includes a runnable Phase 10 example at
`examples/flow_with_configuration.py`.

```bash
uv sync --extra local
uv run python -m examples.flow_with_configuration
```

You can also run the integration test for this example:

```bash
uv run pytest tests/test_phase10_configuration_example.py
```

### Run the artifact save/load workflow

The repository includes a runnable Phase 8 example at
`examples/flow_with_artifacts.py`.

```bash
uv sync --extra local
uv run python -m examples.flow_with_artifacts
```

You can also run the integration test for this example:

```bash
uv run pytest tests/test_phase8_artifacts_example.py
```

### Run the execution management workflow

The repository includes a runnable Phase 11 example at
`examples/client_execution_management.py`.

```bash
uv sync --extra local
uv run python -m examples.client_execution_management
```

You can also run the integration test for this example:

```bash
uv run pytest tests/test_phase11_client_example.py
```

### Run the LLM workflow

The repository includes a runnable Phase 12 example at
`examples/flow_with_llm.py`.

```bash
uv sync --extra local
kitaru model register fast --model openai/gpt-4o-mini
export OPENAI_API_KEY=sk-...
uv run python -m examples.flow_with_llm
```

You can also run the integration test for this example:

```bash
uv run pytest tests/test_phase12_llm_example.py
```

### CLI

```
kitaru --version              Show the SDK version
kitaru --help                 Show available commands

kitaru login <server>         Connect to a Kitaru server
kitaru login <server> --api-key <key>
kitaru logout                 Log out and clear stored auth state
kitaru status                 Show connection state and active stack
kitaru info                   Show detailed environment information

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
| `kitaru.wait()` | Suspend a flow until external input arrives (requires ZenML server support) |

## Development

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/), and [just](https://github.com/casey/just).

```bash
uv sync                # Install dependencies
uv sync --extra local  # Include local ZenML runtime components
just --list            # Show all available recipes
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
