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
- `examples/`: runnable SDK examples, grouped as `python/` and `typescript/`
- `docs/book/`: hand-written GitBook docs; see `docs/book/AGENTS.md`
- `docs/content/docs/`: generated SDK reference content
- `docs/app/`, `docs/scripts/`, `docs/worker/`: reference app, generation, and redirect worker code
- `scripts/`: OpenAPI generation, doc generation, and UI bundle scripts
- `docker/`: Dockerfiles
- `design/`: gitignored design notes; never commit anything from this directory

For adapter, importer, specialized UI API, docs, CI, and release work, load the
matching Kitaru repo skill under `.agents/skills/`. Keep each logical repo skill
available under the same name in `.claude/skills/`; share host-neutral guidance
and diverge only for a documented host-specific reason.

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
- Name a function or method for the action it performs, not the value it
  returns. Write `_get_bearer_credential`, `_get_account_name`,
  `_get_name_taken_message`, not `_bearer_credential`, `_account_name`,
  `_name_taken`. A bare noun reads as an attribute at the call site.
- Follow Google Python style for docstrings.
- Describe the symbol in a docstring, never its callers. Write "Set the account
  name and contact email", not "Set the identity fields mirrored from an
  external account". A caller named in a docstring is wrong as soon as a second
  caller appears.
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

Tests cover ordinary changes. When a change can only be proven against a live
stack, such as import, replay, experiment, job, or worker behavior, `devtools/`
runs one locally: `uv run python devtools/seed.py --db-name kitaru_<yourtask>
--keep` seeds a server end to end. Read `devtools/AGENTS.md` first, and clean up
the stacks and databases you create.

## Docs

- Never hard-wrap prose in Markdown files. Keep each paragraph and each list item on one logical line; use line breaks only for Markdown structure such as headings, blank lines, list items, tables, and fenced code blocks.

Kitaru docs live on three different surfaces:

- Hand-written docs are GitBook Markdown in `docs/book/`, published to
  `docs.zenml.io/kitaru`.
- Generated SDK reference docs live under `docs/content/docs/` and are
  served at `sdkdocs.kitaru.ai`.
- `kitaru.ai/docs/*` is handled by the redirect worker under `docs/worker/`.

Do not add hand-written pages to the FumaDocs app under `docs/content/docs/`;
they belong in `docs/book/`. Do not hand-edit generated reference output.

Only document shipped features.

The canonical returns implementation lives in the public [`zenml-io/kitaru-template`](https://github.com/zenml-io/kitaru-template) repository. Its root README owns clone, frozen installation, local workspace setup, agent registration, worker startup, and the checked-in trace import. The conceptual quickstart in `docs/book/getting-started/quickstart.md` and the detailed tutorial in `docs/book/tutorials/returns-agent/` explain the same method without copying that setup contract. When changing template commands, fixtures, behavior, or terminology, inspect both documentation paths and validate the affected template contract. Apply the same check in the other direction when changing either documentation path.

## Git, Commits, and PRs

- Default branch is `develop`; PRs normally target `develop`. During the v2 migration, v2 feature work may target its explicitly named integration branch.
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
