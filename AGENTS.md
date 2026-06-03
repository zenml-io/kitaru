# Repository Guidelines

## Project Structure

Kitaru is a **mixed Python + docs repo** that produces two things: a Python SDK package and the Kitaru documentation app. The marketing landing page and public `kitaru.ai` runtime now live in the sibling `zenml-io-v2` repository.

```
src/kitaru/           # Python SDK package (src layout)
  cli.py              # CLI facade / console entrypoint (cyclopts)
  _cli/               # Internal command modules + shared CLI helpers
  adapters/           # Framework adapters (includes PydanticAI and OpenAI Agents)
  mcp/                # MCP server tools (optional `kitaru[mcp]` extra)
tests/                # pytest tests
tests/mcp/            # MCP-specific tests (runs in `[mcp]` CI path)
examples/             # Runnable SDK examples
docs/                 # FumaDocs Next.js app — documentation content + static export
  content/docs/       # Documentation content (MDX files)
  scripts/            # Node-side doc generation (convert-sdk-docs.mjs)
  app/                # Next.js app routes, layout, metadata
scripts/              # Doc generation, smoke test, and UI bundle scripts
FRONTEND-TESTING.md   # Internal runbook for Kitaru UI bundle/frontend testing
                         and stable/prerelease release validation
docker/               # Dockerfiles (production server, server-dev, and dev flow images)
design/               # Design docs, meeting notes (gitignored, never commit)
```

### Docs and web ownership

The docs app stays in this repo because it is generated from the Python SDK and CLI source:

1. Python scripts generate docs content (`scripts/generate_cli_docs.py`, `scripts/generate_changelog_docs.py`, `scripts/generate_sdk_docs.py`)
2. `docs/` builds a static export into `docs/out/` (Next.js with `basePath: '/docs'`)
3. Root `wrangler.toml` deploys a docs-only Cloudflare Worker named `kitaru-site` so `kitaru.ai/docs` and PR previews still come from this repo

The public marketing/runtime site for Kitaru now lives in `zenml-io-v2`. If a task involves Astro pages, public site assets, marketing Cloudflare Pages deployment, or runtime web APIs such as waitlist/get-started/newsletter endpoints, switch to that repository instead of adding that code back here.

## Build, Test, and Development Commands

Use `uv` for Python dependency management and `just` as the command stack.

### Python workflows

- `uv sync`: install and sync dependencies
- `uv sync --extra local`: install with local ZenML runtime components
- `uv run kitaru init`: **required in a fresh `git worktree`.** Creates the `.kitaru/` project marker that ZenML's dynamic pipeline resolver needs to re-import example modules by dotted path. Without it, ~14 `test_phase*_example::*_runs_end_to_end` tests fail with `RuntimeError: Unable to resolve dynamic pipeline source`. Worktrees do not inherit `.kitaru/` from the main checkout.

**Core dev loop — these three commands handle the vast majority of development needs:**

| Command | What it does | When to run |
|---|---|---|
| **`just check`** | All checks: format, lint, typecheck, typos, yaml, actions lint, links | After every chunk of work, before commit/push |
| **`just fix`** | Auto-fixes formatting, lint issues, yaml | When `just check` reports fixable issues — resolves most linting problems automatically |
| **`just test`** | Full pytest suite | After code changes, before commit/push |

**Typical loop:** write code → `just fix` → `just check` → `just test` → commit.

- `just test tests/test_file.py::test_name`: run one test
- **Agent tip:** the full suite takes ~4 minutes. When running it through a pager or any stream that may truncate output, pipe through grep so the failure names survive the truncation: `just test 2>&1 | grep -E "FAILED|ERROR|passed|failed" | tail -20`. This keeps the PASS/FAIL summary and every `FAILED` line without forcing a rerun just to recover the list.
- `just lint`: lint only
- `just typecheck`: type check only
- `just typos`: typo check only
- `just format-check`: check formatting without modifying
- `just yaml-check`: check YAML formatting
- `just actions-lint`: lint GitHub Actions workflows (requires `actionlint`: `brew install actionlint`)
- `just zizmor`: audit GitHub Actions workflow security with `zizmor`
- `just audit`: audit Python dependencies with `pip-audit` and the documented ignore list
- `just links`: check markdown links offline (requires `lychee`: `brew install lychee`)
- `just links-external`: check links including external URLs (slow)
- `just build`: build wheel and sdist locally

### Docs workflows

These require Node 22+ and pnpm.

- `just generate-docs`: generate CLI reference + changelog + SDK reference docs
- `just docs`: preview docs locally (dev server at localhost:3000)
- `just docs-build`: build docs static export
- `just docs-validate`: validate the static export as served under `/docs`

## Coding Style & Naming Conventions

- Follow US English spelling in code and docs (`initialize`, `serialize`, `color`).
- Use type hints on all public functions and return values.
- Prefer modern annotations (`list[str]`, `str | None`) over legacy `typing` aliases.
- **Do not use `from __future__ import annotations` in files that define Kitaru `@flow`/`@checkpoint` functions or ZenML `@pipeline`/`@step` functions.** ZenML inspects step output annotations at runtime and currently rejects postponed/string annotations such as `"dict[str, Any]"`. Use real runtime annotations instead; Python 3.11+ supports `list[str]` / `str | None` without the future import.
- Follow Google Python style for docstrings.
- Keep comments focused on *why* (intent/trade-offs), not line-by-line narration.
- Treat leading underscore names as private to module/class boundaries.
- Prefer Protocols/ABCs or `isinstance` over `getattr`/`hasattr` for capability checks.
- Put helpers on the class when tied to its behavior; use standalone utils only for generic cross-module functions.
- Prefer Pydantic models for data structures; checkpoint return values must be serializable.

## CLI

The `kitaru` console script is defined in `pyproject.toml` under `[project.scripts]`. `src/kitaru/cli.py` is the thin facade / entrypoint, while command implementations live in `src/kitaru/_cli/`. Add new subcommands in the appropriate `src/kitaru/_cli/_*.py` module and register them on the shared Cyclopts app there. When testing CLI commands, always pass an explicit arg list (`app(["--help"])`, not bare `app()`). CLI invocations raise `SystemExit(0)` on success.

Agent-facing commands should keep the shared `--output json` / `-o json` contract consistent:
- single-item commands emit `{command, item}`
- list commands emit `{command, items, count}`
- `kitaru executions logs --follow --output json` emits JSONL event objects instead of one final document
- Document login consistently: bare `kitaru login` starts the local server, while `kitaru login <server>` is the remote-login path. Local server support requires the `kitaru[local]` extra.

### Diagnostics and cleanup

- `kitaru info` shows a multi-section diagnostic overview (connection, config provenance, connection sources, system info). Use `--all` for a full dump including all installed packages and environment type. Use `--file debug.json` (or `.yaml`) to export diagnostics to a file (environment variable secrets are masked).
- `kitaru clean project|global|all` resets Kitaru state. The `project` subcommand removes `.kitaru/`, `global` removes the global config directory (with auto-backup and local server teardown), and `all` does both. Use `--dry-run` to preview, `--force` when model registry aliases exist (global/all only). The `clean` command is bootstrap-safe — it works even when the store is broken.

## Testing Guidelines

Use `pytest` for unit and integration tests. Name files `test_*.py` and test functions `test_*`. Mirror source paths (example: `src/kitaru/runtime.py` -> `tests/test_runtime.py`). Every bug fix should include a regression test that fails before the fix and passes after it.

## Analytics Instrumentation

Kitaru collects anonymous usage analytics for opted-in users. When adding new features, discuss analytics coverage with the core team to decide what should be tracked.

- All event names must be added to the `AnalyticsEvent` enum in `src/kitaru/analytics.py`.
- Track only non-sensitive metadata (event names, boolean flags, enum values, counts). Never include user content, file paths, prompts, or secret values.
- Follow the existing patterns for each surface:
  - **CLI:** feature events via `track()` in subcommand handlers (`src/kitaru/_cli/`).
  - **MCP:** `@tracked_mcp_tool` decorator in `src/kitaru/mcp/server.py`.
  - **Core SDK:** `track(AnalyticsEvent.X, {...})` at lifecycle points in the relevant module.
- All `track()` calls must fail silently — never break user-facing functionality for analytics.

## Commit & Pull Request Guidelines

Use short, imperative subjects (for example: `Add ...`, `Update ...`, `Create ...`). Keep commit titles concise (about 50 chars), and explain any important or useful implementation details (esp those relating to 'why' choices were made) in the commit body message.

For pull requests, use a clear human-readable title and include:
- what changed
- why it was needed
- key implementation decisions
- reviewer focus areas

Link related issues (for example `Fixes #123`) when applicable.

Never include a "[Codex] " prefix to PR titles.

Every PR description should include a "Reviewer Notes" H2 or H3 section. Treat that section as a narrative guide for a human reviewer, not a file-by-file checklist:
- Start with the story of the change: where the important behavior now happens, what used to go wrong, and what would break if the implementation is wrong.
- Point reviewers toward the genuinely tricky or high-risk areas. Mention files only when the file name helps the story, and explain what to inspect there.
- Include a concrete "Reproduction" subsection either inside Reviewer Notes or immediately after it. Prefer an example, CLI flow, or UI path that proves the behavior end to end. For example: run a specific `examples/...` script, then open the UI or run `kitaru executions list` / `kitaru executions logs` and describe the exact thing the reviewer should see.
- Do not use a standalone "Verification" section as a substitute for reproduction when it only says `just check`, `just test`, or `/simplify`. Those commands are useful local hygiene, but they do not tell the reviewer how to see the feature or bug fix. If they are worth mentioning, keep them as a short "Local checks run" note after the reproduction steps.

### Feature completion checklist

When adding a new CLI command, MCP tool, or SDK feature:

- **Smoke test**: add a non-destructive invocation to `scripts/smoke-test.sh` (e.g. `kitaru <command> --dry-run` or `kitaru <command> --help`). The smoke test runs before every release, so new features should be exercised there to catch regressions.
- **Analytics**: check whether the feature needs a tracking event. Add the event to `AnalyticsEvent` in `src/kitaru/analytics.py` and wire it into the appropriate surface (CLI handler via `track()`, MCP tool via `@tracked_mcp_tool`, or SDK lifecycle point). If the CLI command is multi-word (e.g. `clean project`), add it to `_MULTI_TOKEN_COMMANDS` in `cli.py`.

## CI/CD

### Python CI (`ci.yml`)

Runs on push/PR to `develop`. Jobs: lint + format check + yaml check, typos, type check, dependency audit, link check, Docker server smoke test, wheel-packaging check, base tests (Python 3.11 + 3.12 + 3.13), and additional test lanes with `kitaru[mcp]` installed (3.11 + 3.12).

When changing Kitaru UI bundling, frontend smoke testing, Docker dashboard packaging, or release UI selection, read `FRONTEND-TESTING.md` first. It is the runbook for stable vs prerelease `kitaru-ui-v*` bundle testing and the trusted-event/token boundaries.

### Docs CI (`docs.yml`)

Runs on manual dispatch, `main` pushes, and PRs touching `docs/**`, docs generation scripts, SDK source, `CHANGELOG.md`, `pyproject.toml`, `uv.lock`, or `wrangler.toml`. It generates the CLI/changelog/SDK reference docs, builds and validates the static docs export, deploys production docs to `kitaru.ai/docs`, and creates/cleans same-repo PR preview Workers. Marketing site deployment is handled from `zenml-io-v2`, not this repository.

### Other workflows

- `release.yml`: release automation (version bump, PyPI publish, Docker image publish, GitHub Release)
- `spellcheck.yml`: separate typo/spell checking on `develop` pushes and non-draft PRs
- `image-optimiser.yml`: PR-only compression for changed JPG/JPEG/PNG/WebP files in same-repo non-draft PRs
- `zizmor.yml`: GitHub Actions security analysis for workflow/dependabot changes, plus weekly and manual runs

## Branching and Release Strategy

- Default branch is `develop`. All PRs target `develop`.
- `main` tracks the latest released version only; do not push directly.
- Before releasing, run `./scripts/smoke-test.sh` to exercise CLI, SDK flows, MCP tools, and LLM integration end-to-end against a local server. Use `-s` to skip reinstall, `-k` to keep the server running afterward. Set `OPENAI_API_KEY` to include LLM tests.
- Releases are cut via the Release workflow (`workflow_dispatch` on `develop` or `v*` tag push).
- Release branches (`release/X.Y.Z`) and tags (`vX.Y.Z`) are created automatically.
- Version is maintained in `pyproject.toml` and bumped by the release workflow. Never hardcode it — use `importlib.metadata.version("kitaru")`.
- Update `CHANGELOG.md` under `[Unreleased]` when making user-facing changes.
## Docs Content Rules

- **Only document shipped features.** No "Coming Soon" sections.
- **ZenML invisibility:** users should never need to know Kitaru is built on ZenML. Use Kitaru terminology (workflow, checkpoint, storage), not ZenML terms (orchestrator, artifact store, pipeline).
- **Generated vs static docs:** generated CLI reference content, changelog output, and SDK reference pages come from generation scripts and should not be hand-edited. Static hand-written MDX pages under `docs/content/docs/` (for example `getting-started/*.mdx` or `cli/login.mdx`) are tracked and may be edited directly when the feature behavior changes. SDK reference still uses a two-step pipeline: `scripts/generate_sdk_docs.py` (Python → JSON) then `docs/scripts/convert-sdk-docs.mjs` (JSON → MDX via fumadocs-python).
- **Docs URL references:** inside `docs/content/docs/` and generated docs MDX, link to other docs pages using docs-app-root paths such as `/cli/executions/` or `/concepts/checkpoints/`, not `/docs/...`; Next's `basePath: '/docs'` adds the public prefix for HTML. From public surfaces such as the marketing site in `zenml-io-v2`, link to docs with the full public `/docs/...` path. Public materialized Markdown is rewritten during export by `docs/scripts/materialize-markdown-pages.mjs`.
- **Planning artifacts stay untracked:** do not commit temporary agent planning/review files such as `docs/plans/*`, `docs/reviews/*`, or prompt exports unless the user explicitly asks for a durable tracked document. These are coordination scratchpads, not product docs.
- **Secret docs accuracy:** only `kitaru.llm()` auto-resolves alias-linked secrets today. If you need to document non-LLM secret access, label it clearly as the current low-level pattern instead of implying there is already a dedicated Kitaru secret getter.
- **CLI docs source of truth:** if generated CLI reference syntax is wrong, fix `scripts/generate_cli_docs.py` and/or the relevant `src/kitaru/_cli/_*.py` module (use `src/kitaru/cli.py` only for facade/bootstrap issues), never the generated `docs/content/docs/cli/*` output.
- **Stack docs accuracy:** current shipped stack-create types on CLI/MCP are `local`, `kubernetes`, `vertex`, `sagemaker`, and `azureml`. Advanced CLI/MCP stack creation also supports `--extra` / structured `extra` plus the remote-only `--async` / `async_mode` convenience flag. The public Python SDK `kitaru.create_stack(...)` remains local-only, so docs should keep that distinction explicit.
- **Environment-variable docs:** document `KITARU_*` env vars as the public surface. Mention `ZENML_*` only as a compatibility note when necessary to explain migration or interop.
- **Model-registry docs:** `kitaru model register` still writes aliases to local config, but submitted/replayed runs automatically receive a transported registry snapshot via `KITARU_MODEL_REGISTRY`. `kitaru model list` should be described as listing aliases available in the current environment, not just aliases stored locally.
- **Frontmatter required:** every `.mdx` page needs `title` and `description`.
- **Example READMEs are user-facing, not contributor-facing:** `examples/**/README.md` files exist to teach new users what Kitaru does and walk them through the specific example. Keep them focused on concepts, the primitives used, and how to run the example. Do **not** add maintainer-oriented sections such as "Testing" (internal test commands), CI-only credential setup, or notes about how stubbed/mocked test runs work — those are implementation details for the Kitaru team and belong in `tests/`, contributor docs, or PR descriptions. If a section would not help a first-time user understand Kitaru, it does not belong in an example README.

## Security & Configuration Notes

Do not commit local secrets, `.env` files, or anything in `design/`. Use `uv` (not raw `pip`) for dependency management to keep environments reproducible.

Do not commit RepoPrompt/orchestration scratch documents such as plans, reviews, handoffs, or prompt exports. In particular, do not add `docs/plans/*.md`, `docs/reviews/*.md`, `docs/investigations/*.md`, `prompt-exports/*.md`, or ad-hoc handoff Markdown files unless the user explicitly asks for that artifact to be part of the repository history.
