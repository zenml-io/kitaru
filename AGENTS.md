# Repository Guidelines

Kitaru is a mixed Python + docs repo. It produces the Python SDK package and
the SDK/CLI reference docs app. The public marketing site and `kitaru.ai`
runtime live in the sibling `zenml-io-v2` repository, not here.

## Kitaru and ZenML

Kitaru builds on ZenML rather than replacing its data model or backend APIs.
Before adding Kitaru-specific state, lineage, filtering, persistence, or other
infrastructure, check whether ZenML already provides the required behavior or
whether a focused ZenML change would solve it for both projects. Prefer ZenML's
authoritative models and relationships over parallel Kitaru metadata or
compatibility machinery; otherwise the two implementations can disagree and
working ZenML behavior can silently disappear from Kitaru.

Some features deliberately remain in Kitaru—for example, cost tracking when we
do not want to add that contract to the ZenML API. Make that choice explicitly,
not merely because a Kitaru-only implementation is immediately convenient. If
the boundary is unclear, check with Michael Schuster (`@schustmi`) on the ZenML
team before designing a substantial new mechanism.

## Project Map

- `src/kitaru/`: Python SDK package (`src` layout)
- `src/kitaru/cli.py`: thin CLI facade / console entrypoint
- `src/kitaru/_cli/`: Cyclopts command implementations and shared helpers
- `src/kitaru/adapters/`: framework adapters
- `src/kitaru/mcp/`: optional MCP server tools
- `tests/`: pytest suite; see `tests/AGENTS.md` when working there
- `examples/`: runnable SDK examples
- `docs/book/`: hand-written GitBook docs; see `docs/book/AGENTS.md`
- `docs/content/docs/`: generated SDK + CLI reference content
- `docs/app/`, `docs/scripts/`, `docs/worker/`: reference app, generation, and redirect worker code
- `scripts/`: doc generation, smoke tests, and UI bundle scripts
- `docker/`: production, server-dev, and dev-flow Dockerfiles
- `design/`: gitignored design notes; never commit anything from this directory

For the full project map, docs routing, command catalog, CI/release details, and
task-specific runbooks, load the Kitaru repo skills under `.agents/skills/`.

## Core Commands

Use `uv` for Python dependency management and `just` for the normal command
stack.

- `uv sync`: install and sync dependencies
- `uv sync --extra local`: install local ZenML runtime components
- `uv run kitaru init`: required in a fresh `git worktree`; without `.kitaru/`,
  example-driven flow tests can fail because the dynamic pipeline source cannot
  be resolved
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

Docs commands, release smoke commands, and the full command catalog live in the
`kitaru-dev` skill.

## Coding Style

- Follow US English spelling in code and docs (`initialize`, `serialize`, `color`).
- Use type hints on public functions and return values.
- Prefer modern annotations (`list[str]`, `str | None`) over legacy `typing` aliases.
- Do not use `from __future__ import annotations` in files that define Kitaru
  `@flow`/`@checkpoint` functions or ZenML `@pipeline`/`@step` functions. ZenML
  inspects step output annotations at runtime and currently rejects postponed
  string annotations such as `"dict[str, Any]"`.
- Follow Google Python style for docstrings.
- Keep comments focused on why the code exists or why a trade-off was chosen.
- Treat leading underscore names as private to their module or class.
- Prefer Protocols/ABCs or `isinstance` over `getattr`/`hasattr` for capability checks.
- Put helpers on the class when tied to its behavior; use standalone utilities
  only for generic cross-module functions.
- Prefer Pydantic models for data structures; checkpoint return values must be serializable.

## CLI

The `kitaru` console script is defined in `pyproject.toml` under
`[project.scripts]`. `src/kitaru/cli.py` is the facade; command implementations
live in `src/kitaru/_cli/`.

- Add new subcommands in the appropriate `src/kitaru/_cli/_*.py` module and
  register them on the shared Cyclopts app there.
- When testing CLI commands, always pass an explicit arg list such as
  `app(["--help"])`; do not call bare `app()`.
- CLI invocations raise `SystemExit(0)` on success.
- Keep the shared `--output json` / `-o json` contract consistent.

For detailed CLI output contracts, diagnostics commands, cleanup behavior, and
analytics wiring, load the `kitaru-dev` skill.

## Testing

Use `pytest` for unit and integration tests. Name files `test_*.py` and test
functions `test_*`. Mirror source paths where practical. Every bug fix should
include a regression test that fails before the fix and passes after it.

Default pytest runs exclude live provider checks via `-m 'not live_llm'`. Tests
that call OpenAI, Anthropic/Claude, Gemini, or similar paid/external providers
must live under `tests/live/`, carry `live_llm` plus the provider-specific
marker, use short bounded prompts, and skip cleanly when credentials are absent.
The shared provider-spend guard in `tests/conftest.py` blocks accidental
provider calls from deterministic tests while allowing localhost/Kitaru/ZenML
local traffic.

When you work under `tests/`, also read `tests/AGENTS.md`. For test design
patterns, fixture guidance, CI, and release checks, load the
`kitaru-tests-release` skill.

## Docs

Kitaru docs live on three different surfaces:

- Hand-written docs are GitBook Markdown in `docs/book/`, published to
  `docs.zenml.io/kitaru`.
- Generated SDK + CLI reference docs live under `docs/content/docs/` and are
  served at `sdkdocs.kitaru.ai`.
- `kitaru.ai/docs/*` is handled by the redirect worker under `docs/worker/`.

Do not add hand-written pages to the FumaDocs app under `docs/content/docs/`;
they belong in `docs/book/`. Do not hand-edit generated CLI or SDK reference
output. If generated CLI reference syntax is wrong, fix
`scripts/generate_cli_docs.py` and/or the relevant `src/kitaru/_cli/_*.py`
module.

Only document shipped features. Users should not need to know Kitaru is built
on ZenML: use Kitaru terms such as workflow, checkpoint, and storage instead of
ZenML terms such as orchestrator, artifact store, pipeline, or step.

For docs URLs, GitBook authoring, stack docs, environment variable docs, model
registry docs, and example README rules, load the `kitaru-docs` skill.

## Analytics

Kitaru collects anonymous usage analytics for opted-in users. Track only
non-sensitive metadata: event names, boolean flags, enum values, and counts.
Never include user content, file paths, prompts, or secret values.

All event names must be added to the `AnalyticsEvent` enum in
`src/kitaru/analytics.py`. All `track()` calls must fail silently and must never
break user-facing functionality. For wiring patterns by surface, load the
`kitaru-dev` skill.

## Git, Commits, and PRs

- Default branch is `develop`; all PRs target `develop`.
- `main` tracks the latest released version only; do not push directly.
- Use short, imperative commit subjects such as `Add ...` or `Update ...`.
- Keep commit titles concise, around 50 characters when practical.
- Never include a `[Codex] ` prefix in PR titles.
- Link related issues when applicable.
- Every PR description should include a `Reviewer Notes` H2 or H3 section with
  a concrete reproduction path for reviewers.

For the detailed PR-description format, feature completion checklist, release
branch behavior, and release smoke requirements, load the `kitaru-tests-release`
skill.

## CI/CD

Python CI runs on push/PR to `develop`; docs CI runs for docs/reference-related
changes. Do not add live provider calls to PR CI or `release.yml`; provider
validation belongs in `.github/workflows/llm-integration.yml` or release smoke
with explicit credentials and waivers.

When changing Kitaru UI bundling, frontend smoke testing, Docker dashboard
packaging, or release UI selection, read `FRONTEND-TESTING.md` first.

For workflow inventory and release evidence rules, load the `kitaru-tests-release`
skill.

## Security and Generated Files

Do not commit local secrets, `.env` files, or anything in `design/`. Use `uv`,
not raw `pip`, for dependency management to keep environments reproducible.

Do not commit RepoPrompt/orchestration scratch documents such as plans, reviews,
handoffs, prompt exports, or ad-hoc coordination docs unless the user explicitly
asks for that artifact to be part of repository history.
