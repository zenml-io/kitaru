# Repository Guidelines

## Project Structure & Module Organization
Kitaru is a Python 3.12+ project for durable AI agent execution on ZenML. Use a `src` layout:
- `src/kitaru/`: core package and runtime primitives
- `src/kitaru/adapters/`: framework integrations (for example PydanticAI)
- `tests/`: `pytest` test suite, mirroring package paths
- `design/`: design notes and meeting docs (ignored by git; do not commit)

At the moment, the repository is still bootstrapping; place new production code under `src/kitaru/` and tests under `tests/`.

## Build, Test, and Development Commands
Use `uv` for all Python environment and tooling commands.
- `uv sync`: install and sync dependencies
- `uv run pytest`: run full test suite
- `uv run pytest tests/test_file.py::test_name`: run one test
- `uv run ruff check .`: run lint checks
- `uv run ruff check . --fix`: auto-fix lint issues
- `uv run ruff format .`: apply formatting
- `uv run ty check`: run static type checks
- `typos --config ./.typos.toml .`: run typo/spelling checks

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
CI (`.github/workflows/ci.yml`) runs lint, type check, and tests on push/PR against Python 3.12 and 3.13.
Spell checking runs in `.github/workflows/spellcheck.yml` via `crate-ci/typos` using `./.typos.toml`.

## Security & Configuration Notes
Do not commit local secrets, `.env` files, or anything in `design/`. Use `uv` (not raw `pip`) for dependency management to keep environments reproducible.
