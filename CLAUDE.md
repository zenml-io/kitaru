# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Kitaru?

Kitaru is a runtime for **recording, replaying, and improving AI agents in production**. Positioning surfaces (README, docs leads, PyPI, marketing) lead with record → replay → improve (diagnose failures, test model/prompt swaps via replay overrides, compare cost and quality, ship updates with confidence).

## Project layout

```
src/kitaru/           # Python package (src layout) — see src/kitaru/AGENTS.md
  analytics/          # Async analytics client and event source tracking
  api_models/         # Versioned request/response DTOs shared by server and SDK
  client/             # Async SDK making REST calls
  server/             # FastAPI server (API, application, domain, adapters layers)
tests/                # pytest tests — see tests/AGENTS.md
examples/             # Runnable SDK examples: python/ (adapter examples) and typescript/ (SDK + adapter examples)
docs/                 # Three docs surfaces — see "Documentation surfaces" below
  book/               # GitBook source for docs.zenml.io/kitaru (hand-written .md)
  content/docs/       # FumaDocs SDK reference content (generated)
  scripts/            # Node-side doc generation (convert-sdk-docs.mjs)
  app/                # Next.js app routes for the sdkdocs.kitaru.ai reference site
  worker/             # Cloudflare worker: redirect.mjs (kitaru.ai/docs) + routing maps
scripts/              # OpenAPI generation, doc generation, and UI bundle scripts
  generate_openapi.py        # Writes openapi/openapi.json from the FastAPI app
  check_openapi.sh           # Fails when openapi/openapi.json is out of date
  download-ui.sh             # Bundles stable/prerelease Kitaru UI releases into the package tree
FRONTEND-TESTING.md   # Read first for Kitaru UI bundle/frontend testing,
                       # stable/prerelease release validation, and token boundaries
docker/               # Dockerfiles — see docker/CLAUDE.md for full architecture details
design/               # Design docs, meeting notes (gitignored, never commit)
```

### Documentation surfaces

Kitaru docs live on three surfaces — know which one a task touches:

1. **Hand-written docs → GitBook.** Concepts, guides, and getting-started
   content live as plain Markdown in **`docs/book/`** and publish to
   **`docs.zenml.io/kitaru`** via GitBook Git Sync. Edit those `.md` files
   directly; the nav is `docs/book/toc.md` and the space config is
   `docs/book/.gitbook.yaml`. See **`docs/book/AGENTS.md`** for GitBook
   authoring conventions.
2. **Generated SDK reference → `sdkdocs.kitaru.ai`.** The FumaDocs app in
   `docs/` is a **reference-only** site. Built and deployed to the
   `kitaru-sdkdocs` Cloudflare worker (root `wrangler.toml`). See
   **`docs/CLAUDE.md`** for the app + deploy process.
3. **`kitaru.ai/docs` → redirects.** The `kitaru-site` worker
   (`docs/worker/redirect.mjs`, `wrangler.redirect.toml`) 301-redirects old
   `kitaru.ai/docs/*` URLs to GitBook / `sdkdocs.kitaru.ai` / the changelog.

Do **not** add hand-written pages to the FumaDocs app (`docs/content/docs/`) —
they belong in `docs/book/` (GitBook). The public changelog is owned by the
changelog repo (published to `docs.zenml.io/changelog`), not by either docs
surface here.

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
`docs/`). Keep the public `zenml-io/kitaru-template` setup and import path runnable without provider credentials.

Do not commit temporary agent planning/review files such as `docs/plans/*`,
`docs/reviews/*`, or prompt exports unless the user explicitly asks for a
durable tracked document.

## Branching strategy

- **`develop`** is the default branch and the normal target for PRs. During the v2 migration, v2 feature work may target its explicitly named integration branch.
- **`main`** contains only released versions. Updated by force-pushing during releases. Never push directly to `main`.
- **`release/X.Y.Z`** branches are archival snapshots created during the release process.
- **Tags** follow `vX.Y.Z` (e.g. `v0.1.0`).

## Development commands

This project uses [just](https://github.com/casey/just) as a command stack. Run `just --list` to see all recipes.

### Core workflow (the three commands you'll use most)

| Command | What it does | When to run |
|---|---|---|
| **`just check`** | Runs *all* checks: format, lint, typecheck, typos, yaml, actions lint, links | After every chunk of work and before committing/pushing |
| **`just fix`** | Auto-fixes formatting, lint issues, and yaml | When `just check` reports fixable issues — handles most linting problems automatically |
| **`just test`** | Runs the full pytest suite | After code changes and before committing/pushing |

**Typical loop:** write code → `just fix` (auto-fix what it can) → `just check` (verify everything passes) → `just test` (make sure nothing is broken) → commit.

```bash
uv sync                 # Install dependencies
uv sync --extra server  # Include server components
```

PostgreSQL-backed tests expect `docker compose up -d db` (see `tests/AGENTS.md`).

When working with Python, invoke the relevant /astral:<skill> for uv, ty, and ruff to ensure best practices are followed.

## Architecture

The server follows a layered architecture (API, application, domain, infrastructure adapters). The client SDK and server never import each other and both sit on the shared `api_models` package. The binding conventions, including the new-resource checklist, live in scoped `AGENTS.md` files: `src/kitaru/AGENTS.md`, the per-layer files under `src/kitaru/server/`, `src/kitaru/api_models/AGENTS.md`, `src/kitaru/client/AGENTS.md`, and `tests/AGENTS.md`.

## Code style

- **US English spelling** everywhere (code, comments, docs): "initialize", "color", "serialize"
- **Comments explain *why*, not *what*.** No change-tracking comments ("Updated from X", "Refactored this"). No narrating obvious code (`x = x + 1  # increment x`). Add comments only for intent, trade-offs, constraints, edge cases, or non-obvious decisions. Prefer expressive names and small functions over inline commentary.
- **Name functions for the action, not the return value.** `_get_bearer_credential()`, not `_bearer_credential()`. `_get_account_name()`, not `_account_name()`. A bare noun reads as an attribute access at the call site, which hides that work is happening.
- **Docstrings describe the symbol, not its callers.** State what the thing does, never who calls it or why. `"""Set the account name and contact email."""`, not `"""Set the identity fields mirrored from an external account."""`. Same for `"""Response body for the statistics endpoint."""` and `"""Used by the job runner."""`. A caller named in a docstring is wrong as soon as a second caller appears, and the reader cannot tell whether the stated context is a real constraint or just where it happened to be used first. If a caller genuinely depends on something, that belongs in the code or in a comment at the line that needs it. Such a comment is short, precise, and technical, and it states why the code below is written the way it is. Lead with the action it explains, as in `# Defer the payload columns because ...`. Never prefix it with `Why:` or any other label.
- **Prefer typing over dynamic attribute checks.** Use Protocols/ABCs or `isinstance` narrowing instead of `getattr`/`hasattr`. If dynamic access is unavoidable, isolate it in a small typed helper.
- **No postponed annotations.** Do not add `from __future__ import annotations`. Pydantic and FastAPI inspect annotations at runtime, and string annotations break that inspection.
- **Util function placement:** Put a helper on the class if it's tied to the class's behavior or heavily used by subclasses (saves imports, subclasses just call `self.method()`). Put truly generic helpers in a standalone generic file, including helpers that are generic enough that other modules might use them in the future, even while they have a single caller.
- **`_underscore` means private.** `_method()` on a class → only call from within that class. `_function()` in a module → only call from within that module. Do not call private methods/functions from outside their owning class or module.

## Versioning and changelog

- **Single source of truth:** the `version` field in `pyproject.toml`. The release workflow bumps it automatically — never change it by hand.
- **Never hardcode the version** in tests or application code. Use `importlib.metadata.version("kitaru")` to read it at runtime.
- **Update `CHANGELOG.md`** when making user-facing changes. Add entries under the `[Unreleased]` heading. The release workflow moves `[Unreleased]` to a versioned heading (e.g. `[0.2.0] - 2026-04-01`) at release time.

## Commits and PRs

- **Run CI checks locally before committing/pushing.** Always run `just check` and `just test` before pushing to `develop`. All checks must pass locally — do not rely on CI to catch failures. This includes format, lint, typecheck, typos, yaml, actions lint, links, and tests.
- **Keep pre-existing failures separate.** If `just check` or `just test` surfaces a failure unrelated to the requested change, diagnose and report it. Fix it only when it blocks the scoped change or the user explicitly approves expanding the task; do not absorb another contributor's work into the current commit by default.
- **Commits:** Imperative mood, concise summary (50 chars or less): "Add feature" not "Added feature". Explain *why* in the body (blank line after summary), reference issues when applicable (`Fixes #1234`).
- **Bug fixes:** Always add a regression test that would have caught the bug. Understand root cause before implementing the fix.
- **PRs:** Human-readable titles (no "feat:"/"doc:" prefixes). Write comprehensive descriptions: what the changes do, why they're needed, key implementation decisions, and areas needing reviewer attention.
- **PR reviewer guidance:** Every PR description should include a "Reviewer Notes" H2 or H3 section, but it should read like a guided walkthrough rather than a file inventory. Explain the story of the change, where the risky behavior lives, what would break if the implementation is wrong, and why the named files or functions matter.
- **PR reproduction:** Include a concrete "Reproduction" subsection inside Reviewer Notes or immediately after it. Prefer a runnable example, API flow, or UI path that proves the behavior end to end. Tell the reviewer exactly what to run and what to look for afterward.
- **PR local checks:** Do not create a standalone "Verification" section that only lists `just check`, `just test`, or `/simplify`. Those are still required local hygiene, but they are not useful reviewer guidance by themselves. If useful, include them as a short "Local checks run" note after the reproduction instructions.
- **Before opening a PR or making a large commit**, always run `/simplify` to review changed code for reuse opportunities, quality issues, and efficiency improvements. Fix any issues it finds before committing.
- **Preserve template compatibility** when changing Kitaru behavior used by the public `zenml-io/kitaru-template` repository. Inspect its current contract and validate the affected template checks before opening a PR.
- Never include a "[Codex] " or "feat: " prefix to PR titles.

## Conventions

- Never hard-wrap prose in Markdown files. Keep each paragraph and each list item on one logical line; use line breaks only for Markdown structure such as headings, blank lines, list items, tables, and fenced code blocks.
- Python 3.11+
- Type hint all function parameters and return values
- Use modern type annotations: `list[str]` not `List[str]`, `str | None` not `Optional[str]`, `dict[str, int]` not `Dict[str, int]` — no `from typing import` for these
- src layout (`src/kitaru/`)
- Use `uv` for all package management (never raw pip)
- Use `ruff` for linting/formatting, `ty` for type checking
- Use `pytest` for testing
- Prefer Pydantic models for data structures
- Design docs live in `design/` — this folder is gitignored and must never be committed
- Never commit RepoPrompt/orchestration scratch Markdown such as plans, reviews, investigations, handoffs, or prompt exports. Keep `docs/plans/*.md`, `docs/reviews/*.md`, `docs/investigations/*.md`, `prompt-exports/*.md`, and ad-hoc handoff files out of repo history unless the user explicitly asks for that artifact to be committed.
