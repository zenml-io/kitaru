# Repository Guidelines

## Project Structure

Kitaru is a **mixed Python + web repo** that produces three things: a Python SDK package, a documentation site, and a marketing landing page — all deployed together.

```
src/kitaru/           # Python SDK package (src layout)
  cli.py              # CLI facade / console entrypoint (cyclopts)
  _cli/               # Internal command modules + shared CLI helpers
  adapters/           # Framework adapters (includes PydanticAI and OpenAI Agents)
  mcp/                # MCP server tools (optional `kitaru[mcp]` extra)
tests/                # pytest tests
tests/mcp/            # MCP-specific tests (runs in `[mcp]` CI path)
examples/             # Runnable SDK examples
docs/                 # FumaDocs Next.js app — documentation at kitaru.ai/docs
  content/docs/       # Documentation content (MDX files)
  scripts/            # Node-side doc generation (convert-sdk-docs.mjs)
  app/                # Next.js app routes, layout, metadata
site/                 # Astro landing page + runtime shell at kitaru.ai/
  src/pages/api/      # Server-side API routes (e.g. /api/waitlist with KV)
scripts/              # Doc generation, site merge, and smoke test scripts
docker/               # Dockerfiles (production server, server-dev, and dev flow images)
design/               # Design docs, meeting notes (gitignored, never commit)
wrangler.toml         # Unified Cloudflare Worker deployment config
```

### Unified deployment model

The docs and landing page deploy as **one Cloudflare Worker**:

1. Python scripts generate docs content (`scripts/generate_cli_docs.py`, `scripts/generate_changelog_docs.py`, `scripts/generate_sdk_docs.py`)
2. `docs/` builds a static export into `docs/out/` (Next.js with `basePath: '/docs'`)
3. `site/` builds the Astro app into `site/dist/`
4. `scripts/merge_site.sh` copies `docs/out/*` into `site/dist/docs/`
5. Root `wrangler.toml` deploys the merged output as one Worker

Astro is the runtime shell because it owns the `/api/waitlist` endpoint (backed by Cloudflare KV). The docs are pure static files mounted under `/docs`.

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

### Docs/site workflows

These require Node 22+ and pnpm.

- `just generate-docs`: generate CLI reference + changelog + SDK reference docs
- `just docs`: preview docs locally (dev server at localhost:3000)
- `just docs-build`: build docs static export
- `just site`: preview landing page locally (dev server at localhost:4321)
- `just site-build-only`: build landing page only (no docs merge)
- `just site-build`: full unified build (generate docs, build docs, build site, merge)

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

Never include a "[Codex] " prefix to PR titles. Also all PR descriptions should include a "Reviewer Notes" H2 or H3 section which explains what they should take care to check out during their reviews (i.e. code highlights) and ideally it also includes a code snippet they can run (you can assume they have both the ability to run flows locally as well as against a Kubernetes remote stack) to reproduce either the fix or the error etc.

### Feature completion checklist

When adding a new CLI command, MCP tool, or SDK feature:

- **Smoke test**: add a non-destructive invocation to `scripts/smoke-test.sh` (e.g. `kitaru <command> --dry-run` or `kitaru <command> --help`). The smoke test runs before every release, so new features should be exercised there to catch regressions.
- **Analytics**: check whether the feature needs a tracking event. Add the event to `AnalyticsEvent` in `src/kitaru/analytics.py` and wire it into the appropriate surface (CLI handler via `track()`, MCP tool via `@tracked_mcp_tool`, or SDK lifecycle point). If the CLI command is multi-word (e.g. `clean project`), add it to `_MULTI_TOKEN_COMMANDS` in `cli.py`.

## CI/CD

### Python CI (`ci.yml`)

Runs on push/PR to `develop`. Jobs: lint + format check + yaml check, typos, type check, dependency audit, link check, Docker server smoke test, wheel-packaging check, base tests (Python 3.11 + 3.12 + 3.13), and additional test lanes with `kitaru[mcp]` installed (3.11 + 3.12).

### Site CI (`site.yml`)

Runs on manual dispatch, push to `main` (production deploy), and PRs touching `site/**`, `docs/**`, `scripts/generate_*.py`, `scripts/merge_site.sh`, `CHANGELOG.md`, or `wrangler.toml`. Generates docs content, builds both apps, merges output, and deploys:
- **Production:** deploys unified Worker to `kitaru.ai` on `main` push
- **PR previews:** deploys a preview Worker for same-repo PRs; cleans up on PR close

### Other workflows

- `release.yml`: release automation (version bump, PyPI publish, Docker image publish, GitHub Release)
- `spellcheck.yml`: separate typo/spell checking on `develop` pushes and non-draft PRs
- `image-optimiser.yml`: PR-only compression for changed JPG/JPEG/PNG/WebP files in same-repo non-draft PRs, with `site/public/dashboard.png` ignored
- `zizmor.yml`: GitHub Actions security analysis for workflow/dependabot changes, plus weekly and manual runs

## Branching and Release Strategy

- Default branch is `develop`. All PRs target `develop`.
- `main` tracks the latest released version only; do not push directly.
- Before releasing, run `./scripts/smoke-test.sh` to exercise CLI, SDK flows, MCP tools, and LLM integration end-to-end against a local server. Use `-s` to skip reinstall, `-k` to keep the server running afterward. Set `OPENAI_API_KEY` to include LLM tests.
- Releases are cut via the Release workflow (`workflow_dispatch` on `develop` or `v*` tag push).
- Release branches (`release/X.Y.Z`) and tags (`vX.Y.Z`) are created automatically.
- Version is maintained in `pyproject.toml` and bumped by the release workflow. Never hardcode it — use `importlib.metadata.version("kitaru")`.
- Update `CHANGELOG.md` under `[Unreleased]` when making user-facing changes.
- The site deploys on `main` pushes, so the site goes live at release time.

## Docs Content Rules

- **Only document shipped features.** No "Coming Soon" sections.
- **ZenML invisibility:** users should never need to know Kitaru is built on ZenML. Use Kitaru terminology (workflow, checkpoint, storage), not ZenML terms (orchestrator, artifact store, pipeline).
- **Generated vs static docs:** generated CLI reference content, changelog output, and SDK reference pages come from generation scripts and should not be hand-edited. Static hand-written MDX pages under `docs/content/docs/` (for example `getting-started/*.mdx` or `cli/login.mdx`) are tracked and may be edited directly when the feature behavior changes. SDK reference still uses a two-step pipeline: `scripts/generate_sdk_docs.py` (Python → JSON) then `docs/scripts/convert-sdk-docs.mjs` (JSON → MDX via fumadocs-python).
- **Memory docs accuracy:** document the shipped memory surfaces precisely. Module-level `kitaru.memory.*` uses the currently configured typed scope and does not take per-call `scope=` arguments; `KitaruClient.memories`, CLI memory commands, and MCP memory tools use explicit typed scopes (`scope` + `scope_type`) on scoped operations. Memory remains forbidden inside `@checkpoint`.
- **Secret docs accuracy:** only `kitaru.llm()` auto-resolves alias-linked secrets today. If you need to document non-LLM secret access, label it clearly as the current low-level pattern instead of implying there is already a dedicated Kitaru secret getter.
- **CLI docs source of truth:** if generated CLI reference syntax is wrong, fix `scripts/generate_cli_docs.py` and/or the relevant `src/kitaru/_cli/_*.py` module (use `src/kitaru/cli.py` only for facade/bootstrap issues), never the generated `docs/content/docs/cli/*` output.
- **Stack docs accuracy:** current shipped stack-create types on CLI/MCP are `local`, `kubernetes`, `vertex`, `sagemaker`, and `azureml`. Advanced CLI/MCP stack creation also supports `--extra` / structured `extra` plus the remote-only `--async` / `async_mode` convenience flag. The public Python SDK `kitaru.create_stack(...)` remains local-only, so docs should keep that distinction explicit.
- **Environment-variable docs:** document `KITARU_*` env vars as the public surface. Mention `ZENML_*` only as a compatibility note when necessary to explain migration or interop.
- **Model-registry docs:** `kitaru model register` still writes aliases to local config, but submitted/replayed runs automatically receive a transported registry snapshot via `KITARU_MODEL_REGISTRY`. `kitaru model list` should be described as listing aliases available in the current environment, not just aliases stored locally.
- **Frontmatter required:** every `.mdx` page needs `title` and `description`.
- **Example READMEs are user-facing, not contributor-facing:** `examples/**/README.md` files exist to teach new users what Kitaru does and walk them through the specific example. Keep them focused on concepts, the primitives used, and how to run the example. Do **not** add maintainer-oriented sections such as "Testing" (internal test commands), CI-only credential setup, or notes about how stubbed/mocked test runs work — those are implementation details for the Kitaru team and belong in `tests/`, contributor docs, or PR descriptions. If a section would not help a first-time user understand Kitaru, it does not belong in an example README.

## Security & Configuration Notes

Do not commit local secrets, `.env` files, or anything in `design/`. Use `uv` (not raw `pip`) for dependency management to keep environments reproducible.
