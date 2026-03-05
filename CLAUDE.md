# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Kitaru?

Kitaru is ZenML's **durable execution layer for AI agents**. It provides primitives (`saga`, `checkpoint`, `wait`, `log`) that make agent workflows persistent, replayable, and observable — without requiring users to learn a graph DSL or change their Python control flow.

**Core philosophy:** Primitives first, frameworks second. Sync-first. Every checkpoint output persisted invisibly for replay. Zero config locally, one-line connect for production.

**ZenML mapping:** `@kitaru.saga` → `@pipeline(dynamic=True)`, `@kitaru.checkpoint` → `@step`, `kitaru.log()` → `log_metadata()`, `kitaru.wait()` → new ZenML core work.

## Project layout

```
src/kitaru/           # Main package (src layout)
  adapters/           # Framework adapters (e.g. pydantic_ai)
tests/                # pytest tests
design/               # Design docs, meeting notes (gitignored, never commit)
```

## Development commands

```bash
# Setup
uv sync                              # Install dependencies
uv run pytest                         # Run all tests
uv run pytest tests/test_foo.py       # Run a single test file
uv run pytest tests/test_foo.py::test_bar  # Run a single test
uv run pytest -x                      # Stop on first failure

# Linting & formatting
uv run ruff check .                   # Lint
uv run ruff check . --fix             # Lint + auto-fix
uv run ruff format .                  # Format

# Type checking
uv run ty check                       # Type check with ty
```

When working with Python, invoke the relevant /astral:<skill> for uv, ty, and ruff to ensure best practices are followed.

## Architecture

### MVP primitives

| Primitive | Purpose |
|---|---|
| `@kitaru.saga` | Outer boundary — marks a durable execution |
| `@kitaru.checkpoint` | Checkpointed unit of work, with optional `type=` for dashboard visualization |
| `kitaru.wait()` | Suspend until a webhook event arrives (MVP: webhook only) |
| `kitaru.log()` | Attach typed metadata to current checkpoint |
| `kitaru.save()` | Explicit named artifact (inside checkpoint only) |
| `kitaru.load()` | Cross-execution artifact loading (requires exec_id) |

### Key patterns

- **Sagas cannot nest** — no `@kitaru.saga` inside another saga
- **Checkpoints can nest** — each independently persisted
- **Concurrency** uses `.submit()` + `.result()` (ZenML futures), not a dedicated primitive
- **Replay** works by re-running the saga from the top: checkpoints before the replay point return cached outputs; checkpoints at/after the replay point re-execute
- **Artifact overrides** let you swap a checkpoint's cached output during replay

### Framework adapters

The PydanticAI adapter (`kitaru.adapters.pydantic_ai`) wraps agents so each model request → `checkpoint(type='llm_call')` and each tool call → `checkpoint(type='tool_call')` automatically.

### Observability

Kitaru emits OpenTelemetry spans. It does **not** own the tracing backend — users configure their own OTel exporter (Logfire, Datadog, etc.).

## Conventions

- Python 3.12+
- Use modern type annotations: `list[str]` not `List[str]`, `str | None` not `Optional[str]`, `dict[str, int]` not `Dict[str, int]` — no `from typing import` for these
- src layout (`src/kitaru/`)
- Use `uv` for all package management (never raw pip)
- Use `ruff` for linting/formatting, `ty` for type checking
- Use `pytest` for testing
- Prefer Pydantic models for data structures
- Return values from checkpoints must be serializable (prefer Pydantic models or JSON-compatible types)
- Design docs live in `design/` — this folder is gitignored and must never be committed
