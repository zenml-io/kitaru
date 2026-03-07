# Kitaru

Durable execution for AI agents, built on [ZenML](https://zenml.io).

Kitaru makes agent workflows **persistent, replayable, and observable** using a small set of Python primitives. No graph DSL, no framework lock-in — just decorators on your existing code.

## What works today

Kitaru is under active development. The core flow and checkpoint decorators are implemented and functional. Several additional primitives (`wait`, `log`, `save`, `load`, `llm`) are scaffolded but not yet implemented.

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

### CLI

```
kitaru --version              Show the SDK version
kitaru --help                 Show available commands

kitaru login <server>         Connect to a Kitaru server
kitaru login <server> --api-key <key>
kitaru logout                 Log out and clear stored auth state
kitaru status                 Show connection state and active stack
kitaru info                   Show detailed environment information
```

### Planned primitives (scaffolded, not yet implemented)

| Primitive | Purpose |
|---|---|
| `kitaru.wait()` | Suspend a flow until external input arrives (requires ZenML server support) |
| `kitaru.log()` | Attach structured metadata (cost, tokens, latency) to checkpoints |
| `kitaru.save()` | Persist a named artifact inside a checkpoint |
| `kitaru.load()` | Load a named artifact from a previous execution |
| `kitaru.llm()` | Tracked LLM calls with automatic artifact and metadata capture |

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
