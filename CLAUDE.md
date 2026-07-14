# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Kitaru?

Kitaru is ZenML's runtime for **recording, replaying, and improving AI agents in production**. It provides primitives (`flow`, `checkpoint`, `save`, `load`, `wait`, `log`) that record every step of an agent run as a replayable checkpoint — without requiring users to learn a graph DSL or change their Python control flow. Durable execution is the underlying mechanism, not the headline: positioning surfaces (README, docs leads, PyPI, marketing) lead with record → replay → improve (diagnose failures, test model/prompt swaps via replay overrides, compare cost and quality, ship updates with confidence).

**Core philosophy:** Primitives first, frameworks second. Sync-first. Every checkpoint output persisted invisibly for replay. Zero config locally, one-line connect for production.

**ZenML mapping:** `@flow` → `@pipeline(dynamic=True)`, `@checkpoint` → `@step`, `kitaru.log()` → `log_metadata()`, `kitaru.wait()` → new ZenML core work. `kitaru init` creates `.kitaru/` (not `.zen/`) as the local project marker via `ZENML_REPOSITORY_DIRECTORY_NAME`.

**Build on ZenML; do not duplicate it:** Before adding Kitaru-specific state,
lineage, filtering, persistence, or backend infrastructure, check whether ZenML
already provides the required behavior or whether a focused ZenML change is the
better solution. Treat ZenML's models and relationships as authoritative; do
not create a parallel Kitaru representation that can disagree with them. A
Kitaru-only feature can be appropriate when the distinction is deliberate—for
example, cost tracking when its contract should not enter the ZenML API—but
immediate convenience is not enough reason. When the boundary is unclear,
consult Michael Schuster (`@schustmi`) on the ZenML team before designing a
substantial new mechanism.

**Unified config directory:** Kitaru and ZenML share a single config directory. The `kitaru_init_hook` sets `ZENML_CONFIG_PATH` to Kitaru's app dir (e.g. `~/.config/kitaru/` on Linux, `~/Library/Application Support/kitaru/` on macOS) so the database, credentials, local stores, and Kitaru's own `kitaru.yaml` all live together. `KITARU_CONFIG_PATH` overrides this for both. Server subprocesses that set `ZENML_CONFIG_PATH` directly are respected.

## Project layout

```
src/kitaru/           # Python SDK package (src layout)
  cli.py              # CLI facade / console entrypoint (cyclopts)
  _cli/               # Internal command modules + shared CLI helpers
  adapters/           # Framework adapters (includes PydanticAI and OpenAI Agents)
  mcp/                # MCP server tools (optional `kitaru[mcp]` extra)
tests/                # pytest tests
tests/mcp/            # MCP-specific unit tests (runs in `[mcp]` CI path)
examples/             # Runnable SDK examples
docs/                 # Two docs surfaces — see "Documentation surfaces" below
  book/               # GitBook source for docs.zenml.io/kitaru (hand-written .md)
  content/docs/       # FumaDocs SDK+CLI reference content (generated cli/ + reference/)
  scripts/            # Node-side doc generation (convert-sdk-docs.mjs)
  app/                # Next.js app routes for the sdkdocs.kitaru.ai reference site
  worker/             # Cloudflare worker: redirect.mjs (kitaru.ai/docs) + routing maps
scripts/              # Doc generation, smoke test, and UI bundle scripts
  download-ui.sh             # Bundles stable/prerelease Kitaru UI releases into the package tree
  generate_cli_docs.py       # Generates CLI reference MDX from cyclopts introspection
  generate_changelog_docs.py # Generates changelog MDX from CHANGELOG.md
  generate_sdk_docs.py       # Extracts Python SDK API to JSON (griffe → docs/.generated/sdk-api.json)
  smoke-test.sh              # Pre-release end-to-end sanity check (CLI, flows, MCP, LLM)
FRONTEND-TESTING.md   # Read first for Kitaru UI bundle/frontend testing,
                       # stable/prerelease release validation, and token boundaries
docker/               # Dockerfiles — see docker/CLAUDE.md for full architecture details
  Dockerfile          # Production server (FROM zenmldocker/zenml-server + Kitaru + Kitaru UI)
  Dockerfile.server-dev  # Dev server for local UI testing (local source + local UI dist)
  Dockerfile.dev      # Flow-execution image for remote stacks (K8s, etc.)
design/               # Design docs, meeting notes (gitignored, never commit)
```

### Documentation surfaces

Kitaru docs live on three surfaces — know which one a task touches:

1. **Hand-written docs → GitBook.** Concepts, guides, adapters, getting-started,
   agent-harness-platform, stacks, deploy, agent-native, etc. live as plain
   Markdown in **`docs/book/`** and publish to **`docs.zenml.io/kitaru`** via
   GitBook Git Sync. Edit those `.md` files directly; the nav is
   `docs/book/toc.md` and the space config is `docs/book/.gitbook.yaml`. See
   **`docs/book/AGENTS.md`** for GitBook authoring conventions.
2. **Generated SDK + CLI reference → `sdkdocs.kitaru.ai`.** The FumaDocs app in
   `docs/` is now a **reference-only** site — just the generated
   `content/docs/cli/` + `content/docs/reference/python/` + a landing index.
   Built and deployed to the `kitaru-sdkdocs` Cloudflare worker (root
   `wrangler.toml`). See **`docs/CLAUDE.md`** for the app + deploy process.
3. **`kitaru.ai/docs` → redirects.** The `kitaru-site` worker
   (`docs/worker/redirect.mjs`, `wrangler.redirect.toml`) 301-redirects old
   `kitaru.ai/docs/*` URLs to GitBook / `sdkdocs.kitaru.ai` / the changelog.

Do **not** add hand-written pages to the FumaDocs app (`docs/content/docs/`) —
they belong in `docs/book/` (GitBook). The public changelog is owned by the
changelog repo (published to `docs.zenml.io/changelog`), not by either docs
surface here. This repo may still generate a gitignored
`docs/content/docs/changelog.mdx` for local/reference builds; do not hand-edit
or commit that generated output.

The public marketing/runtime site for Kitaru lives in the sibling `zenml-io-v2`
repository. If a task involves Astro pages, public site assets, marketing
Cloudflare Pages deployment, or runtime web APIs such as
waitlist/get-started/newsletter endpoints, switch to `zenml-io-v2` and follow
that repo's instructions instead of adding that code back here.

## Website and marketing assets

The Kitaru marketing site and its asset pipeline now live in `zenml-io-v2`. Do not add Astro pages, public site assets, R2 blog tooling, or runtime website changes to this repository. If a task is about the public website rather than the Python SDK/docs source, work in `zenml-io-v2` instead.

## Docs guidance

Detailed authoring conventions, link rules, and accuracy requirements for all
three docs surfaces live in **`docs/CLAUDE.md`** (loaded when you work under
`docs/`). Rules for example READMEs live in **`examples/CLAUDE.md`**.

Two rules worth keeping in front of you everywhere:

- If generated CLI reference syntax is wrong, fix `scripts/generate_cli_docs.py` and/or the relevant `src/kitaru/_cli/_*.py` module (use `src/kitaru/cli.py` only for facade/bootstrap issues), not the generated `docs/content/docs/cli/*` output.
- Do not commit temporary agent planning/review files such as `docs/plans/*`, `docs/reviews/*`, or prompt exports unless the user explicitly asks for a durable tracked document.

## Branching strategy

- **`develop`** is the default branch and the target for all PRs.
- **`main`** contains only released versions. Updated by force-pushing during releases. Never push directly to `main`.
- **`release/X.Y.Z`** branches are archival snapshots created during the release process.
- **Tags** follow `vX.Y.Z` (e.g. `v0.1.0`).

### Releasing a new version

Use the **`kitaru-release`** skill. It walks the full procedure: diffing
`develop` against the last tag, classifying commits, updating `CHANGELOG.md`,
running release-grade smoke (`./scripts/smoke-test.sh --release`), checking the
live-provider workflow, dispatching the release, and rewriting the GitHub
Release notes into structured sections.

## Development commands

This project uses [just](https://github.com/casey/just) as a command stack. Run `just --list` to see all recipes.

### Core workflow (the three commands you'll use most)

| Command | What it does | When to run |
|---|---|---|
| **`just check`** | Runs *all* checks: format, lint, typecheck, typos, yaml, actions lint, links | After every chunk of work and before committing/pushing |
| **`just fix`** | Auto-fixes formatting, lint issues, and yaml | When `just check` reports fixable issues — handles most linting problems automatically |
| **`just test`** | Runs the full pytest suite | After code changes and before committing/pushing |

**Typical loop:** write code → `just fix` (auto-fix what it can) → `just check` (verify everything passes) → `just test` (make sure nothing is broken) → commit.

**Worktree setup gotcha:** in a fresh `git worktree`, run `uv run kitaru init` once after `uv sync`, or ~14 end-to-end example tests fail with `Unable to resolve dynamic pipeline source`.

```bash
uv sync                # Install dependencies
uv sync --extra local  # Include local ZenML runtime components
uv run kitaru init     # Required in a fresh git worktree — see note above
```

Everything else — individual check recipes, single-test invocations, docs
generation, Kitaru UI bundle scripts, Docker image builds, and the CI/CD
workflow table — is in the **`kitaru-dev-commands`** skill. Run `just --list`
for the full recipe list.

When working with Python, invoke the relevant /astral:<skill> for uv, ty, and ruff to ensure best practices are followed.

## Architecture

> **Note:** Most SDK primitives and CLI commands are implemented (see table below). Replay is now implemented across SDK, flow objects, CLI, and MCP surfaces.

### Current MVP primitives

| Primitive | Status |
|---|---|
| `@flow` | Implemented |
| `@checkpoint` | Implemented |
| `kitaru.wait()` | Implemented |
| `kitaru.llm()` | Implemented |
| `kitaru.log()` | Implemented |
| `kitaru.save()` | Implemented |
| `kitaru.load()` | Implemented |
| Stack lifecycle (`list_stacks` / `current_stack` / `use_stack` / `create_stack` / `delete_stack`) | Implemented |
| `kitaru.configure()` + config precedence | Implemented |
| `KitaruClient` (`get/list/latest/logs/input/retry/resume/cancel/replay` + artifact browsing) | Implemented |
| Execution CLI (`kitaru executions get/list/logs/input/replay/retry/resume/cancel`) | Implemented |
| Secrets CLI (`kitaru secrets set/show/list/delete`) | Implemented |
| `KitaruClient.executions.replay()` | Implemented |
| `kitaru init` (project initialization, creates `.kitaru/`) | Implemented |
| `kitaru clean` (`project` / `global` / `all` — reset Kitaru state with dry-run, backup, and model registry protection) | Implemented |
| Enhanced `kitaru info` (`--all`, `--all-packages`, `--packages`, `--file` — config provenance, connection sources, system info) | Implemented |

### Key design patterns

- **Flows are top-level orchestration boundaries** — direct flow calls are blocked; start executions with `.run()`
- **Nested checkpoint calls are blocked in the current MVP implementation**
- **Concurrency** uses `.submit()` + `.result()` (ZenML futures), not a dedicated primitive
- **Isolated runtime** via `@checkpoint(runtime="isolated")` runs a checkpoint in its own container on remote orchestrators; locally it falls back to inline
- **Replay** works by re-running the flow from the top: checkpoints before the replay point return cached outputs; checkpoints at/after the replay point re-execute
- **Artifact overrides** let you swap a checkpoint's cached output during replay

### Framework adapters

Implemented framework adapters include `kitaru.adapters.pydantic_ai.KitaruAgent(agent, ...)` and the OpenAI Agents adapter under `kitaru.adapters.openai_agents`. The OpenAI Agents adapter is behind the `openai-agents` optional extra (`uv sync --extra openai-agents` or `kitaru[openai-agents]`) and exposes `KitaruRunner` for durable OpenAI Agents runs.

The PydanticAI adapter keeps the enclosing checkpoint as the replay boundary while tracking PydanticAI model requests and tool calls as child events/artifacts under that checkpoint. At flow scope, `run()` / `run_sync()` automatically open a synthetic checkpoint per turn so tracking still works without an explicit outer checkpoint; outside any flow they auto-open a local flow (remote stacks require an explicit `@kitaru.flow`). Capture is controlled via a `CapturePolicy` (`tool_capture="full"|"metadata"|None` plus per-tool overrides). HITL is auto-bridged: PydanticAI's native `requires_approval=True`, `ApprovalRequired`, and `CallDeferred` all route through `kitaru.wait(...)` with no decorator. For explicit HITL markers, use `kitaru.adapters.pydantic_ai.hitl_tool(...)`. Per-turn checkpoint behavior (runtime, retries, type) is configurable via `KitaruAgent(..., turn_checkpoint_config={"runtime": "inline"})`; adapter-managed checkpoints do not yet support `runtime="isolated"`.

### Observability (current MVP + planned)

Current MVP observability includes:

- `kitaru.log()` for structured metadata on executions/checkpoints
- Runtime log retrieval via `KitaruClient.executions.logs(...)`, `kitaru executions logs`, and MCP `get_execution_logs`
- Global runtime log-store configuration via `kitaru log-store set/show/reset`
  (defaults to `artifact-store`, supports global external backend override, and now warns when preference differs from the active stack log store)

Future work will add richer OpenTelemetry-native tracing and exporter integration.

## Code style

- **US English spelling** everywhere (code, comments, docs): "initialize", "color", "serialize"
- **Comments explain *why*, not *what*.** No change-tracking comments ("Updated from X", "Refactored this"). No narrating obvious code (`x = x + 1  # increment x`). Add comments only for intent, trade-offs, constraints, edge cases, or non-obvious decisions. Prefer expressive names and small functions over inline commentary.
- **Prefer typing over dynamic attribute checks.** Use Protocols/ABCs or `isinstance` narrowing instead of `getattr`/`hasattr`. If dynamic access is unavoidable, isolate it in a small typed helper.
- **No postponed annotations in flow/checkpoint modules.** Do not add `from __future__ import annotations` to files that define Kitaru `@flow`/`@checkpoint` functions or ZenML `@pipeline`/`@step` functions. ZenML inspects step output annotations at runtime and currently rejects string annotations such as `"dict[str, Any]"`; use real runtime annotations instead. Python 3.11+ already supports `list[str]` / `str | None` without the future import.
- **Util function placement:** Put a helper on the class if it's tied to the class's behavior or heavily used by subclasses (saves imports, subclasses just call `self.method()`). Use standalone util files only for truly generic functions used across unrelated modules.
- **`_underscore` means private.** `_method()` on a class → only call from within that class. `_function()` in a module → only call from within that module. Do not call private methods/functions from outside their owning class or module.

## Analytics instrumentation

Kitaru collects anonymous usage analytics for opted-in users. Event names live in
the `AnalyticsEvent` enum in `src/kitaru/analytics.py` — never use raw strings,
and never track user content, file paths, prompts, model outputs, secret values,
or positional CLI arguments. Full instrumentation guidance is in
**`src/kitaru/CLAUDE.md`**.

## Versioning and changelog

- **Single source of truth:** the `version` field in `pyproject.toml`. The release workflow bumps it automatically — never change it by hand.
- **Never hardcode the version** in tests or application code. Use `importlib.metadata.version("kitaru")` to read it at runtime.
- **Update `CHANGELOG.md`** when making user-facing changes. Add entries under the `[Unreleased]` heading. The release workflow moves `[Unreleased]` to a versioned heading (e.g. `[0.2.0] - 2026-04-01`) at release time.

## Commits and PRs

- **Run CI checks locally before committing/pushing.** Always run `just check` and `just test` before pushing to `develop`. All checks must pass locally — do not rely on CI to catch failures. This includes format, lint, typecheck, typos, yaml, actions lint, links, and tests.
- **Fix pre-existing failures too.** If `just check` or `just test` surfaces failures that predate your changes, fix them rather than ignoring them. Other people may be working in the same repo, so not every failure is yours — but don't default to "not my problem." Ask the user if unsure whether a failure should be addressed in this commit.
- **Commits:** Imperative mood, concise summary (50 chars or less): "Add feature" not "Added feature". Explain *why* in the body (blank line after summary), reference issues when applicable (`Fixes #1234`).
- **Bug fixes:** Always add a regression test that would have caught the bug. Understand root cause before implementing the fix.
- **PRs:** Human-readable titles (no "feat:"/"doc:" prefixes). Write comprehensive descriptions: what the changes do, why they're needed, key implementation decisions, and areas needing reviewer attention.
- **PR reviewer guidance:** Every PR description should include a "Reviewer Notes" H2 or H3 section, but it should read like a guided walkthrough rather than a file inventory. Explain the story of the change, where the risky behavior lives, what would break if the implementation is wrong, and why the named files or functions matter.
- **PR reproduction:** Include a concrete "Reproduction" subsection inside Reviewer Notes or immediately after it. Prefer a runnable example, CLI flow, or UI path that proves the behavior end to end. Tell the reviewer exactly what to run and what to look for afterward, such as a named `examples/...` script, a UI artifact/checkpoint name, or a `kitaru executions list` / `kitaru executions logs` result.
- **PR local checks:** Do not create a standalone "Verification" section that only lists `just check`, `just test`, or `/simplify`. Those are still required local hygiene, but they are not useful reviewer guidance by themselves. If useful, include them as a short "Local checks run" note after the reproduction instructions.
- **Before opening a PR or making a large commit**, always run `/simplify` to review changed code for reuse opportunities, quality issues, and efficiency improvements. Fix any issues it finds before committing.
- **Update the smoke test** (`scripts/smoke-test.sh`) when adding new CLI commands, MCP tools, or SDK features that can be exercised non-interactively. New commands should have at least a `--dry-run` or `--help` invocation in the smoke script so pre-release validation catches regressions. Use `--dry-run` where available to keep the smoke test non-destructive.
- **Update the example coverage manifest** (`examples/example-coverage.yaml`) when adding, removing, renaming, or publicly documenting examples under `examples/`. Then run `just example-coverage-audit`; it validates paths, coverage metadata, and explicit waivers for missing/planned/manual-only coverage only, without running examples or provider calls. A passing audit does not mean every example executed.
- **Review analytics coverage** when expanding the CLI, MCP, or SDK surface. Check whether the new feature needs a tracking event in `AnalyticsEvent` and whether the event is wired into the appropriate surface (CLI handler, `@tracked_mcp_tool`, or SDK lifecycle point). See the [Analytics instrumentation](#analytics-instrumentation) section for patterns. If multi-word CLI commands are added, update `_MULTI_TOKEN_COMMANDS` in `cli.py` to avoid leaking positional arguments into analytics.
- Never include a "[Codex] " or "feat: " prefix to PR titles.

## CLI

The CLI uses [cyclopts](https://cyclopts.readthedocs.io/). `src/kitaru/cli.py` is the thin facade / console entrypoint, and the actual command implementations live under `src/kitaru/_cli/`. The `kitaru` console script is registered in `pyproject.toml` under `[project.scripts]`.

- Add new subcommands in the appropriate `src/kitaru/_cli/_*.py` module and register them on the shared app there
- Version is read automatically from package metadata via `importlib.metadata.version()`
- When testing CLI commands, always pass an explicit arg list: `app(["--help"])`, never bare `app()` (which reads `sys.argv`)
- CLI commands raise `SystemExit(0)` on success — wrap in `pytest.raises(SystemExit)` in tests

### CLI output styling

CLI output uses [Rich](https://rich.readthedocs.io/) for styled terminal output with a **dual-mode pattern**: Rich panels/colors for interactive terminals, plain text for non-TTY output (pipes, CI, tests). The `_is_interactive()` helper controls mode selection.

- Use `_emit_snapshot()` for key/value views (status, info), `_print_success()` for success messages, `_exit_with_error()` for errors
- Use `rich.text.Text` objects for user-supplied values — never interpolate them into Rich markup strings (avoids `[`/`]` misinterpretation)
- Create `Console()` lazily inside helpers, not at module level (pytest replaces streams after import)
- Tests use `capsys` and assert on plain-text substrings — the non-TTY path keeps this stable

## Conventions

- Python 3.11+
- Type hint all function parameters and return values
- Use modern type annotations: `list[str]` not `List[str]`, `str | None` not `Optional[str]`, `dict[str, int]` not `Dict[str, int]` — no `from typing import` for these
- src layout (`src/kitaru/`)
- Use `uv` for all package management (never raw pip)
- Use `ruff` for linting/formatting, `ty` for type checking
- Use `pytest` for testing
- Prefer Pydantic models for data structures
- Return values from checkpoints must be serializable (prefer Pydantic models or JSON-compatible types)
- Design docs live in `design/` — this folder is gitignored and must never be committed
- Never commit RepoPrompt/orchestration scratch Markdown such as plans, reviews, investigations, handoffs, or prompt exports. Keep `docs/plans/*.md`, `docs/reviews/*.md`, `docs/investigations/*.md`, `prompt-exports/*.md`, and ad-hoc handoff files out of repo history unless the user explicitly asks for that artifact to be committed.
- Follow Google Python style for docstrings
