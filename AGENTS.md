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
docs/                 # Two docs surfaces — see "Documentation surfaces" below
  book/               # GitBook source for docs.zenml.io/kitaru (hand-written .md)
  content/docs/       # FumaDocs SDK+CLI reference content (generated cli/ + reference/)
  scripts/            # Node-side doc generation (convert-sdk-docs.mjs)
  app/                # Next.js app routes for the sdkdocs.kitaru.ai reference site
  worker/             # Cloudflare worker: redirect.mjs (kitaru.ai/docs) + routing maps
scripts/              # Doc generation, smoke test, and UI bundle scripts
FRONTEND-TESTING.md   # Internal runbook for Kitaru UI bundle/frontend testing
                         and stable/prerelease release validation
docker/               # Dockerfiles (production server, server-dev, and dev flow images)
design/               # Design docs, meeting notes (gitignored, never commit)
```

### Documentation surfaces

Kitaru docs live on three surfaces — know which one a task touches:

1. **Hand-written docs → GitBook.** Concepts, guides, adapters, getting-started, etc. are plain Markdown in **`docs/book/`**, published to **`docs.zenml.io/kitaru`** via GitBook Git Sync. Edit those `.md` directly; nav is `docs/book/toc.md`, config is `docs/book/.gitbook.yaml`, authoring conventions are in `docs/book/AGENTS.md`.
2. **Generated SDK + CLI reference → `sdkdocs.kitaru.ai`.** The FumaDocs app in `docs/` is now reference-only (generated `content/docs/cli/` + `content/docs/reference/python/` + a landing). Built/deployed to the `kitaru-sdkdocs` worker (root `wrangler.toml`). See `docs/CLAUDE.md`.
3. **`kitaru.ai/docs` → redirects.** The `kitaru-site` worker (`docs/worker/redirect.mjs`, `wrangler.redirect.toml`) 301-redirects old `kitaru.ai/docs/*` URLs to GitBook / `sdkdocs.kitaru.ai` / the changelog.

Do not add hand-written pages to the FumaDocs app (`docs/content/docs/`) — they belong in `docs/book/`. The public changelog is owned by the changelog repo (`docs.zenml.io/changelog`); this repo may still generate a gitignored `docs/content/docs/changelog.mdx` for local/reference builds, but agents should not hand-edit or commit it.

The public marketing/runtime site for Kitaru now lives in `zenml-io-v2`. If a task involves Astro pages, public site assets, marketing Cloudflare Pages deployment, or runtime web APIs such as waitlist/get-started/newsletter endpoints, switch to that repository instead of adding that code back here.

## Build, Test, and Development Commands

Use `uv` for Python dependency management and `just` as the command stack.

### Python workflows

- `uv sync`: install and sync dependencies
- `uv sync --extra local`: install with local ZenML runtime components
- `uv run kitaru init`: **required in a fresh `git worktree`.** Creates the `.kitaru/` project marker that ZenML's dynamic pipeline resolver needs to re-import example modules by dotted path. Without it, ~14 `test_phase*_example::*_runs_end_to_end` tests fail with `RuntimeError: Unable to resolve dynamic pipeline source`. Worktrees do not inherit `.kitaru/` from the main checkout.
- **Lockfile merge caution:** when merging `develop` into a feature branch and resolving `pyproject.toml` / `uv.lock`, do not assume a broad `uv lock` preserves recent dependency-security fixes. Check recent commits touching `uv.lock` / `.github/pip-audit-ignored.txt`, use targeted commands such as `uv lock --upgrade-package <package>` when a package was intentionally bumped, and run `just audit` before pushing.

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
- `just example-coverage-audit`: validate `examples/example-coverage.yaml` against public example docs, referenced tests/smoke/provider metadata, and explicit waivers for `missing`, `planned`, or `manual_only` coverage. Audit-only: it does not run examples or call providers, so a pass means the metadata is honest and internally consistent, not that every example executed.
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

Default pytest runs exclude live provider checks via `-m 'not live_llm'`. Tests that call OpenAI, Anthropic/Claude, Gemini, or similar paid/external providers must live under `tests/live/`, carry `live_llm` plus the provider-specific marker, use short bounded prompts, and skip cleanly when credentials are absent. The shared provider-spend guard in `tests/conftest.py` blocks accidental provider calls from deterministic tests while allowing localhost/Kitaru/ZenML local traffic.

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
- **Example coverage manifest**: when adding, removing, renaming, or publicly documenting an example under `examples/`, update `examples/example-coverage.yaml` and run `just example-coverage-audit`. The manifest records coverage status and required waivers only; it is not an example runner, so manual-only or missing coverage remains a release-review item.
- **Analytics**: check whether the feature needs a tracking event. Add the event to `AnalyticsEvent` in `src/kitaru/analytics.py` and wire it into the appropriate surface (CLI handler via `track()`, MCP tool via `@tracked_mcp_tool`, or SDK lifecycle point). If the CLI command is multi-word (e.g. `clean project`), add it to `_MULTI_TOKEN_COMMANDS` in `cli.py`.

## CI/CD

### Python CI (`ci.yml`)

Runs on push/PR to `develop`. PR jobs run lint + format check + yaml check, typos, type check, dependency audit, link check, base tests (Python 3.11 + 3.12 + 3.13), and additional test lanes with `kitaru[mcp]` installed (3.11 + 3.12). Push jobs also run Docker server smoke and wheel-packaging checks, because those paths may need trusted UI release credentials.

When changing Kitaru UI bundling, frontend smoke testing, Docker dashboard packaging, or release UI selection, read `FRONTEND-TESTING.md` first. It is the runbook for stable vs prerelease `kitaru-ui-v*` bundle testing and the trusted-event/token boundaries.

### Docs CI (`docs.yml`)

Runs on manual dispatch, `main` pushes, and PRs touching `docs/**`, docs generation scripts, SDK source, `CHANGELOG.md`, `pyproject.toml`, `uv.lock`, or Wrangler config. It regenerates the CLI/SDK reference and builds the FumaDocs static export on all runs. It deploys the **SDK+CLI reference site to `sdkdocs.kitaru.ai`** (worker `kitaru-sdkdocs`) plus the **`kitaru.ai/docs` redirect worker** (`kitaru-site`, `wrangler.redirect.toml`) only on `main` push or manual dispatch; PRs build only and do not create preview Workers. Hand-written docs (`docs/book/`) publish separately to `docs.zenml.io/kitaru` via GitBook Git Sync (not this workflow). Marketing site deployment is handled from `zenml-io-v2`.

### Other workflows

- `release.yml`: release automation (version bump, PyPI publish, Docker image publish, GitHub Release). Do not add live provider calls here; provider validation happens before release dispatch.
- `llm-integration.yml`: trusted weekly/manual live OpenAI/Anthropic provider checks. It has only `schedule` and `workflow_dispatch` triggers, runs paid tests outside PR CI, can target an exact ref/SHA, uploads logs/results only, and sends a compact Discord failure alert via `DISCORD_WEBHOOK_SRE`. Its secret-bearing jobs use the GitHub Environment `live-provider-tests`; configure `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, and `DISCORD_WEBHOOK_SRE` as Environment secrets there, with `kitaru-admins` approval/restrictions.
- `spellcheck.yml`: separate typo/spell checking on `develop` pushes and non-draft PRs
- `image-optimiser.yml`: PR-only compression for changed JPG/JPEG/PNG/WebP files in same-repo non-draft PRs
- `zizmor.yml`: GitHub Actions security analysis for workflow/dependabot changes, plus weekly and manual runs

## Branching and Release Strategy

- Default branch is `develop`. All PRs target `develop`.
- `main` tracks the latest released version only; do not push directly.
- Before releasing, run release-grade smoke with structured output: `./scripts/smoke-test.sh --release --json-out smoke-results.json` plus repeatable `--required-provider-area <area>` flags for any changed provider-backed behavior (`openai`, `anthropic`, `gemini-model`, `gemini-antigravity`, `google-adk`, `research-bot`). Bare `./scripts/smoke-test.sh` remains useful for local development, but it is not enough to prove a provider-relevant release because skipped provider checks do not fail outside `--release`. Use `-s` to skip reinstall, `-k` to keep the server running afterward. Set the relevant provider credentials and opt-in env vars before marking provider areas covered.
- Also check `.github/workflows/llm-integration.yml` before release dispatch. A weekly green run on `develop` is a canary that provider paths are generally healthy; it is not exact release evidence. If OpenAI or Anthropic adapter/example behavior changed, trigger a manual `llm-integration.yml` run for the exact release ref or SHA and require it to pass, or record an explicit waiver in the release conversation. Gemini remains local release-smoke evidence or waiver for v1.
- Releases are cut via the Release workflow (`workflow_dispatch` on `develop` or `v*` tag push).
- Release branches (`release/X.Y.Z`) and tags (`vX.Y.Z`) are created automatically.
- Version is maintained in `pyproject.toml` and bumped by the release workflow. Never hardcode it — use `importlib.metadata.version("kitaru")`.
- Update `CHANGELOG.md` under `[Unreleased]` when making user-facing changes.
## Docs Content Rules

- **Only document shipped features.** No "Coming Soon" sections.
- **ZenML invisibility:** users should never need to know Kitaru is built on ZenML. Use Kitaru terminology (workflow, checkpoint, storage), not ZenML terms (orchestrator, artifact store, pipeline).
- **Where to edit:** hand-written docs are GitBook Markdown in **`docs/book/`** (see `docs/book/AGENTS.md`) — add new pages there and to `docs/book/toc.md`. The FumaDocs app under `docs/content/docs/` is reference-only and **fully generated** (CLI + SDK); do not hand-edit it. SDK reference still uses a two-step pipeline: `scripts/generate_sdk_docs.py` (Python → JSON) then `docs/scripts/convert-sdk-docs.mjs` (JSON → MDX via fumadocs-python).
- **Docs URL references:** inside `docs/book/`, link to sibling pages with relative `.md` paths (e.g. `../concepts/checkpoints.md`, `flows.md#runtime-options`). Link to the SDK/CLI reference with `https://sdkdocs.kitaru.ai`, to other ZenML docs with absolute `https://docs.zenml.io/...`, and to diagrams with `https://assets.kitaru.ai/docs/diagrams/<slug>.png`.
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
