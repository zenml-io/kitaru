# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Kitaru?

Kitaru is ZenML's **durable execution layer for AI agents**. It provides primitives (`flow`, `checkpoint`, `save`, `load`, `wait`, `log`) that make agent workflows persistent, replayable, and observable — without requiring users to learn a graph DSL or change their Python control flow.

**Core philosophy:** Primitives first, frameworks second. Sync-first. Every checkpoint output persisted invisibly for replay. Zero config locally, one-line connect for production.

**ZenML mapping:** `@kitaru.flow` → `@pipeline(dynamic=True)`, `@kitaru.checkpoint` → `@step`, `kitaru.log()` → `log_metadata()`, `kitaru.wait()` → new ZenML core work.

## Project layout

```
src/kitaru/           # Python SDK package (src layout)
  cli.py              # CLI entry point (cyclopts)
  adapters/           # Framework adapters (includes PydanticAI)
tests/                # pytest tests
examples/             # Runnable SDK examples (Phase 5/7/8/10/11/12/15/17 milestones)
docs/                 # FumaDocs Next.js app — documentation at kitaru.ai/docs
  content/docs/       # Documentation content (MDX files)
  scripts/            # Node-side doc generation (convert-sdk-docs.mjs)
  app/                # Next.js app routes, layout, metadata, search, sitemap
site/                 # Astro landing page + Cloudflare runtime shell at kitaru.ai/
  src/pages/api/      # Server-side API routes (/api/waitlist with KV)
scripts/              # Doc generation + site merge scripts
  generate_cli_docs.py       # Generates CLI reference MDX from cyclopts introspection
  generate_changelog_docs.py # Generates changelog MDX from CHANGELOG.md
  generate_sdk_docs.py       # Extracts Python SDK API to JSON (griffe → docs/.generated/sdk-api.json)
  merge_site.sh              # Merges docs static export into Astro build output
spec/                 # SDK design specifications (temporary, deleted once implemented)
wrangler.toml         # Unified Cloudflare Worker deployment config
design/               # Design docs, meeting notes (gitignored, never commit)
```

### Unified site deployment

The docs and landing page deploy as **one Cloudflare Worker** from `site/dist/`:

1. Python scripts generate docs content (CLI reference, changelog, SDK reference JSON)
2. `docs/` builds a static export into `docs/out/` (Next.js with `basePath: '/docs'`)
3. `site/` builds the Astro app into `site/dist/` (owns runtime `/api/waitlist` + KV)
4. `scripts/merge_site.sh` copies `docs/out/*` into `site/dist/docs/`
5. Root `wrangler.toml` deploys the merged bundle

The site workflow (`.github/workflows/site.yml`) runs this pipeline on `main` pushes (production) and creates preview Workers for PRs.

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
uv sync --extra local                # Include local ZenML runtime components

# Common Python workflows
just check                            # Run all checks (format, lint, typecheck, typos, yaml, links)
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
just yaml-check                       # Check YAML formatting
just links                            # Check markdown links offline (requires lychee)
just build                            # Build wheel + sdist locally

# Docs/site workflows (require Node 22+ and pnpm)
just generate-docs                    # Generate CLI reference + changelog + SDK reference docs
just docs                             # Preview docs dev server (localhost:3000)
just docs-build                       # Build docs static export
just site                             # Preview landing page dev server (localhost:4321)
just site-build-only                  # Build landing page only (no docs merge)
just site-build                       # Full unified build (generate + build + merge)

# Manual deploy to Cloudflare
unset CF_API_TOKEN CLOUDFLARE_API_TOKEN  # Clear stale tokens (use wrangler login credentials)
just site-build && npx wrangler deploy   # Build + deploy
```

### CI/CD workflows

| Workflow | Trigger | Purpose |
|---|---|---|
| `ci.yml` | Push/PR to `develop` | Python checks: lint, format, yaml, typos, links, typecheck, tests (3.12 + 3.13) |
| `site.yml` | Push to `main`; PRs touching docs/site/scripts | Build + deploy unified site; PR preview Workers |
| `release.yml` | Workflow dispatch or `v*` tag | Version bump, PyPI publish, GitHub Release |
| `spellcheck.yml` | Push/PR to `develop` | Separate typo/spell checking |
| `image-optimiser.yml` | PRs only | Image compression for docs assets |

When working with Python, invoke the relevant /astral:<skill> for uv, ty, and ruff to ensure best practices are followed.

## Architecture

> **Note:** The SDK is partially implemented. `@kitaru.flow`, `@kitaru.checkpoint`, `kitaru.log()`, `kitaru.save()`, `kitaru.load()`, `kitaru.wait()`, `kitaru.llm()`, `kitaru.configure()`, stack selection helpers (`list_stacks`, `current_stack`, `use_stack`), local model alias CLI (`kitaru model register/list`), `KitaruClient` execution/artifact browsing + wait-input lifecycle surface, typed Kitaru exceptions + failure journaling (`execution.failure`, checkpoint attempt history), core connection/login CLI paths, `kitaru stack list/current/use`, `kitaru log-store set/show/reset`, `kitaru secrets set/show/list/delete`, and execution lifecycle CLI commands (`kitaru run`, `kitaru executions get/list/input/retry/resume/cancel`) are functional. Replay and some CLI extensions remain in progress.

### Current MVP primitives

| Primitive | Status |
|---|---|
| `@kitaru.flow` | Implemented |
| `@kitaru.checkpoint` | Implemented |
| `kitaru.wait()` | Implemented |
| `kitaru.llm()` | Implemented |
| `kitaru.log()` | Implemented |
| `kitaru.save()` | Implemented |
| `kitaru.load()` | Implemented |
| Stack selection (`list_stacks` / `current_stack` / `use_stack`) | Implemented |
| `kitaru.configure()` + Phase 10 config precedence | Implemented |
| `KitaruClient` (`get/list/latest/input/resume/cancel/retry` + artifact browsing) | Implemented |
| Execution CLI (`kitaru run`, `kitaru executions get/list/input/retry/resume/cancel`) | Implemented |
| Secrets CLI (`kitaru secrets set/show/list/delete`) | Implemented |
| `KitaruClient.executions.replay()` | Stubbed (branch-dependent) |

### Key design patterns

- **Flows cannot nest** — no `@kitaru.flow` inside another flow
- **Nested checkpoint calls are blocked in the current MVP implementation**
- **Concurrency** uses `.submit()` + `.result()` (ZenML futures), not a dedicated primitive
- **Replay** works by re-running the flow from the top: checkpoints before the replay point return cached outputs; checkpoints at/after the replay point re-execute
- **Artifact overrides** let you swap a checkpoint's cached output during replay

### Framework adapters

The first framework adapter is implemented: `kitaru.adapters.pydantic_ai.wrap(agent)`.

It keeps the enclosing checkpoint as the replay boundary, while tracking PydanticAI model requests and tool calls as child events/metadata under that checkpoint. The adapter also supports HITL marker tools via `kitaru.adapters.pydantic_ai.hitl_tool(...)`.

### Observability (current MVP + planned)

Current MVP observability includes:

- `kitaru.log()` for structured metadata on executions/checkpoints
- Global runtime log-store configuration via `kitaru log-store set/show/reset`
  (defaults to `artifact-store`, supports global external backend override)

Future work will add richer OpenTelemetry-native tracing and exporter integration.

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

- **Run CI checks locally before committing/pushing.** Always run `just check` and `just test` before pushing to `develop`. All checks must pass locally — do not rely on CI to catch failures. This includes format, lint, typecheck, typos, yaml, links, and tests.
- **Commits:** Imperative mood, concise summary (50 chars or less): "Add feature" not "Added feature". Explain *why* in the body (blank line after summary), reference issues when applicable (`Fixes #1234`).
- **Bug fixes:** Always add a regression test that would have caught the bug. Understand root cause before implementing the fix.
- **PRs:** Human-readable titles (no "feat:"/"doc:" prefixes). Write comprehensive descriptions: what the changes do, why they're needed, key implementation decisions, and areas needing reviewer attention.

## CLI

The CLI uses [cyclopts](https://cyclopts.readthedocs.io/) (`src/kitaru/cli.py`). The `kitaru` console script is registered in `pyproject.toml` under `[project.scripts]`.

- Add new subcommands with `@app.command` in `cli.py`
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