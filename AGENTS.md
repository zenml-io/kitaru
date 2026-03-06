# Repository Guidelines

## Project Structure & Module Organization
Kitaru is a Python 3.12+ project for durable AI agent execution on ZenML. Use a `src` layout:
- `src/kitaru/`: core package and runtime primitives
- `src/kitaru/cli.py`: CLI entry point using [cyclopts](https://cyclopts.readthedocs.io/)
- `src/kitaru/adapters/`: framework integrations (for example PydanticAI)
- `tests/`: `pytest` test suite, mirroring package paths
- `design/`: design notes and meeting docs (ignored by git; do not commit)

At the moment, the repository is still bootstrapping; place new production code under `src/kitaru/` and tests under `tests/`.

## Build, Test, and Development Commands
Use `uv` for dependency management and `just` as the command runner.
- `uv sync`: install and sync dependencies
- `just check`: run all checks (format, lint, typecheck, typos, yaml)
- `just test`: run full test suite
- `just test tests/test_file.py::test_name`: run one test
- `just fix`: auto-fix formatting, lint issues, and yaml
- `just lint`: run lint checks only
- `just typecheck`: run static type checks only
- `just typos`: run typo/spelling checks only
- `just build`: build wheel and sdist locally

## Coding Style & Naming Conventions
- Follow US English spelling in code and docs (`initialize`, `serialize`, `color`).
- Use type hints on all public functions and return values.
- Prefer modern annotations (`list[str]`, `str | None`) over legacy `typing` aliases.
- Follow Google Python style for docstrings.
- Keep comments focused on *why* (intent/trade-offs), not line-by-line narration.
- Treat leading underscore names as private to module/class boundaries.
- Prefer Protocols/ABCs or `isinstance` over `getattr`/`hasattr` for capability checks.
- Put helpers on the class when tied to its behavior; use standalone utils only for generic cross-module functions.
- Prefer Pydantic models for data structures; checkpoint return values must be serializable.

## CLI
The `kitaru` console script is defined in `pyproject.toml` under `[project.scripts]` and implemented with cyclopts in `src/kitaru/cli.py`. Add subcommands via `@app.command`. When testing CLI commands, always pass an explicit arg list (`app(["--help"])`, not bare `app()`). CLI invocations raise `SystemExit(0)` on success.

## Testing Guidelines
Use `pytest` for unit and integration tests. Name files `test_*.py` and test functions `test_*`. Mirror source paths (example: `src/kitaru/runtime.py` -> `tests/test_runtime.py`). Every bug fix should include a regression test that fails before the fix and passes after it.

## Commit & Pull Request Guidelines
Recent history uses short, imperative subjects (for example: `Add ...`, `Update ...`, `Create ...`). Keep commit titles concise (about 50 chars), and explain the why in the body when needed.

For pull requests, use a clear human-readable title and include:
- what changed
- why it was needed
- key implementation decisions
- reviewer focus areas

Link related issues (for example `Fixes #123`) when applicable.

## CI
CI (`.github/workflows/ci.yml`) runs lint, type check, typos, and tests on push/PR to `develop` against Python 3.12 and 3.13.
Typo checking uses `crate-ci/typos` with config in `.typos.toml`.

## Branching and Release Strategy

- Default branch is `develop`. All PRs target `develop`.
- `main` tracks the latest released version only; do not push directly.
- Releases are cut via the Release workflow (`workflow_dispatch` on `develop` or `v*` tag push).
- Release branches (`release/X.Y.Z`) and tags (`vX.Y.Z`) are created automatically.
- Version is maintained in `pyproject.toml` and bumped by the release workflow. Never hardcode it — use `importlib.metadata.version("kitaru")`.
- Update `CHANGELOG.md` under `[Unreleased]` when making user-facing changes.

## Security & Configuration Notes
Do not commit local secrets, `.env` files, or anything in `design/`. Use `uv` (not raw `pip`) for dependency management to keep environments reproducible.
