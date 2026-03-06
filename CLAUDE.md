# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Kitaru?

Kitaru is ZenML's **durable execution layer for AI agents**. It provides primitives (`saga`, `checkpoint`, `wait`, `log`) that make agent workflows persistent, replayable, and observable — without requiring users to learn a graph DSL or change their Python control flow.

**Core philosophy:** Primitives first, frameworks second. Sync-first. Every checkpoint output persisted invisibly for replay. Zero config locally, one-line connect for production.

**ZenML mapping:** `@kitaru.saga` → `@pipeline(dynamic=True)`, `@kitaru.checkpoint` → `@step`, `kitaru.log()` → `log_metadata()`, `kitaru.wait()` → new ZenML core work.

## Project layout

```
src/kitaru/           # Main package (src layout)
  cli.py              # CLI entry point (cyclopts)
  adapters/           # Framework adapters (e.g. pydantic_ai)
tests/                # pytest tests
design/               # Design docs, meeting notes (gitignored, never commit)
```

## Branching strategy

- **`develop`** is the default branch and the target for all PRs.
- **`main`** contains only released versions. Updated by force-pushing during releases. Never push directly to `main`.
- **`release/X.Y.Z`** branches are archival snapshots created during the release process.
- **Tags** follow `vX.Y.Z` (e.g. `v0.1.0`).

### Releasing a new version

1. Ensure `develop` has all changes for the release.
2. Go to Actions > Release > Run workflow (or push a `vX.Y.Z` tag).
3. Enter the version (e.g. `0.2.0`); optionally enable dry-run.
4. The workflow bumps version, runs CI, publishes to PyPI, creates `release/X.Y.Z`, updates `main`, tags, and creates a GitHub Release.

## Development commands

This project uses [just](https://github.com/casey/just) as a command runner. Run `just --list` to see all recipes.

```bash
# Setup
uv sync                              # Install dependencies

# Common workflows
just check                            # Run all checks (format, lint, typecheck, typos, yaml)
just test                             # Run all tests
just test tests/test_foo.py           # Run a single test file
just test tests/test_foo.py::test_bar # Run a single test
just test -x                          # Stop on first failure
just fix                              # Auto-fix formatting, lint, and yaml

# Individual checks
just lint                             # Lint only
just typecheck                        # Type check only
just typos                            # Typo check only
just format-check                     # Check formatting without modifying
just build                            # Build wheel + sdist locally
```

CI runs lint, type check, typos, and tests on push/PR to `develop` (`.github/workflows/ci.yml`). Tests run against Python 3.12 and 3.13.

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

## Code style

- **US English spelling** everywhere (code, comments, docs): "initialize", "color", "serialize"
- **Comments explain *why*, not *what*.** No change-tracking comments ("Updated from X", "Refactored this"). No narrating obvious code (`x = x + 1  # increment x`). Add comments only for intent, trade-offs, constraints, edge cases, or non-obvious decisions. Prefer expressive names and small functions over inline commentary.
- **Prefer typing over dynamic attribute checks.** Use Protocols/ABCs or `isinstance` narrowing instead of `getattr`/`hasattr`. If dynamic access is unavoidable, isolate it in a small typed helper.
- **Util function placement:** Put a helper on the class if it's tied to the class's behavior or heavily used by subclasses (saves imports, subclasses just call `self.method()`). Use standalone util files only for truly generic functions used across unrelated modules.
- **`_underscore` means private.** `_method()` on a class → only call from within that class. `_function()` in a module → only call from within that module. Do not call private methods/functions from outside their owning class or module.

## Versioning and changelog

- **Single source of truth:** the `version` field in `pyproject.toml`. The release workflow bumps it automatically — never change it by hand.
- **Never hardcode the version** in tests or application code. Use `importlib.metadata.version("kitaru")` to read it at runtime.
- **Update `CHANGELOG.md`** when making user-facing changes. Add entries under the `[Unreleased]` heading. The release workflow moves `[Unreleased]` to a versioned heading (e.g. `[0.2.0] - 2026-04-01`) at release time.

## Commits and PRs

- **Commits:** Imperative mood, concise summary (50 chars or less): "Add feature" not "Added feature". Explain *why* in the body (blank line after summary), reference issues when applicable (`Fixes #1234`).
- **Bug fixes:** Always add a regression test that would have caught the bug. Understand root cause before implementing the fix.
- **PRs:** Human-readable titles (no "feat:"/"doc:" prefixes). Write comprehensive descriptions: what the changes do, why they're needed, key implementation decisions, and areas needing reviewer attention.

## CLI

The CLI uses [cyclopts](https://cyclopts.readthedocs.io/) (`src/kitaru/cli.py`). The `kitaru` console script is registered in `pyproject.toml` under `[project.scripts]`.

- Add new subcommands with `@app.command` in `cli.py`
- Version is read automatically from package metadata via `importlib.metadata.version()`
- When testing CLI commands, always pass an explicit arg list: `app(["--help"])`, never bare `app()` (which reads `sys.argv`)
- CLI commands raise `SystemExit(0)` on success — wrap in `pytest.raises(SystemExit)` in tests

## Conventions

- Python 3.12+
- Type hint all function parameters and return values
- Use modern type annotations: `list[str]` not `List[str]`, `str | None` not `Optional[str]`, `dict[str, int]` not `Dict[str, int]` — no `from typing import` for these
- src layout (`src/kitaru/`)
- Use `uv` for all package management (never raw pip)
- Use `ruff` for linting/formatting, `ty` for type checking
- Use `pytest` for testing
- Prefer Pydantic models for data structures
- Return values from checkpoints must be serializable (prefer Pydantic models or JSON-compatible types)
- Design docs live in `design/` — this folder is gitignored and must never be committed
- Follow Google Python style for docstrings