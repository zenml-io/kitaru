# Repository Guidelines

Kitaru is a mixed Python + docs repo. It produces the Python package (SDK and
server) and the SDK reference docs app. The public marketing site and
`kitaru.ai` runtime live in the sibling `zenml-io-v2` repository, not here.

## Project Map

- `src/kitaru/`: Python package (`src` layout); see `src/kitaru/AGENTS.md`
- `src/kitaru/api_models/`: versioned request/response DTOs shared by server and SDK
- `src/kitaru/client/`: async SDK making REST calls
- `src/kitaru/server/`: FastAPI server (API, application, domain, adapters layers)
- `tests/`: pytest suite; see `tests/AGENTS.md` when working there
- `examples/`: runnable SDK examples
- `docs/book/`: hand-written GitBook docs; see `docs/book/AGENTS.md`
- `docs/content/docs/`: generated SDK reference content
- `docs/app/`, `docs/scripts/`, `docs/worker/`: reference app, generation, and redirect worker code
- `scripts/`: OpenAPI generation, doc generation, and UI bundle scripts
- `docker/`: Dockerfiles
- `design/`: gitignored design notes; never commit anything from this directory

For docs routing, CI/release details, and task-specific runbooks, load the
Kitaru repo skills under `.agents/skills/`.

## Core Commands

Use `uv` for Python dependency management and `just` for the normal command
stack.

- `uv sync`: install and sync dependencies
- `uv sync --extra server`: include server components
- `just fix`: auto-fix formatting, lint, and YAML issues
- `just check`: run format, lint, typecheck, typos, YAML, actions lint, and links
- `just test`: run the full pytest suite
- `just test tests/test_file.py::test_name`: run one targeted test
- `just build`: build wheel and sdist locally

Typical loop: write code -> `just fix` -> `just check` -> `just test`.

After code changes, run the aggregate `just check` and `just test` commands rather than raw underlying tools or only piecemeal recipes; during development, run targeted tests as `just test <pytest-node>`.

When running the full suite through output that may truncate, preserve the
failure names:

```bash
just test 2>&1 | grep -E "FAILED|ERROR|passed|failed" | tail -20
```

## Coding Style

- Follow US English spelling in code and docs (`initialize`, `serialize`, `color`).
- Use type hints on public functions and return values.
- Prefer modern annotations (`list[str]`, `str | None`) over legacy `typing` aliases.
- Do not use `from __future__ import annotations`. Pydantic and FastAPI
  inspect annotations at runtime, and string annotations break that
  inspection.
- Follow Google Python style for docstrings.
- Keep comments focused on why the code exists or why a trade-off was chosen.
- Treat leading underscore names as private to their module or class.
- Prefer Protocols/ABCs or `isinstance` over `getattr`/`hasattr` for capability checks.
- Put helpers on the class when tied to its behavior; use standalone utilities
  only for generic cross-module functions.
- Prefer Pydantic models for data structures.

## Testing

Use `pytest` for unit and integration tests. Name files `test_*.py` and test
functions `test_*`. Mirror source paths where practical. Every bug fix should
include a regression test that fails before the fix and passes after it.

When you work under `tests/`, also read `tests/AGENTS.md`.

## Docs

Kitaru docs live on three different surfaces:

- Hand-written docs are GitBook Markdown in `docs/book/`, published to
  `docs.zenml.io/kitaru`.
- Generated SDK reference docs live under `docs/content/docs/` and are
  served at `sdkdocs.kitaru.ai`.
- `kitaru.ai/docs/*` is handled by the redirect worker under `docs/worker/`.

Do not add hand-written pages to the FumaDocs app under `docs/content/docs/`;
they belong in `docs/book/`. Do not hand-edit generated reference output.

Only document shipped features.

## Git, Commits, and PRs

- Default branch is `develop`; all PRs target `develop`.
- `main` tracks the latest released version only; do not push directly.
- Use short, imperative commit subjects such as `Add ...` or `Update ...`.
- Keep commit titles concise, around 50 characters when practical.
- Never include a `[Codex] ` prefix in PR titles.
- Link related issues when applicable.
- Every PR description should include a `Reviewer Notes` H2 or H3 section with
  a concrete reproduction path for reviewers.

## CI/CD

Python CI runs on push/PR to `develop`; docs CI runs for docs/reference-related
changes.

When changing Kitaru UI bundling, frontend smoke testing, Docker dashboard
packaging, or release UI selection, read `FRONTEND-TESTING.md` first.

## Security and Generated Files

Do not commit local secrets, `.env` files, or anything in `design/`. Use `uv`,
not raw `pip`, for dependency management to keep environments reproducible.

Do not commit RepoPrompt/orchestration scratch documents such as plans, reviews,
handoffs, prompt exports, or ad-hoc coordination docs unless the user explicitly
asks for that artifact to be part of repository history.
