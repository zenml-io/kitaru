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
                             # After regenerating, also regenerate the TypeScript
                             # types via `pnpm run generate`
                             # (the typescript CI job fails otherwise)
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

## Website and marketing assets

The Kitaru marketing site, its asset pipeline, and the runtime web APIs (waitlist, get-started, newsletter) live in the sibling `zenml-io-v2` repository. Do not add Astro pages, public site assets, R2 blog tooling, or runtime website changes here. If a task is about the public website rather than the Python SDK or docs source, work in `zenml-io-v2` and follow that repo's instructions.

## Docs guidance

Detailed authoring conventions, link rules, and accuracy requirements for all
three docs surfaces live in **`docs/CLAUDE.md`** (loaded when you work under
`docs/`). Keep the quickstart example setup and import path runnable without provider credentials.

## Branching strategy

- **`develop`** is the default branch and the target for PRs.
- **`main`** contains only released versions. After a stable core release the release owner fast-forwards it to the immutable core tag with `git merge --ff-only`. Never push to it directly and never force-push it.
- **`release/<unit>/<major.minor>`** branches (for example `release/kitaru/0.25`) are maintenance lines that the release workflow creates or fast-forwards after a stable release; patch releases branch from them. The older `release/X.Y.Z` branches are archival snapshots from the previous release process.
- **Tags** are namespaced per release unit: `python/kitaru/v<X.Y.Z>` for core, `python/<distribution>/v<X.Y.Z>` for Python plugins, and `typescript/kitaru/v<X.Y.Z>` for the TypeScript packages. The `kitaru-release` skill owns the full procedure.

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

For adapter, importer, specialized UI API, docs, CI, and release work, load the matching Kitaru repo skill under `.claude/skills/`. The same logical skills live under `.agents/skills/` for Codex; keep host-neutral guidance synchronized and diverge only for a documented host-specific reason.

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

- **Single source of truth:** the `version` field in `pyproject.toml`. On `develop` it carries a `+dev` suffix. Only a release-preparation PR (see the `kitaru-release` skill) sets it to a release version, and the workflow's development-reset PR restores the `+dev` placeholder afterward. Do not change it in feature PRs.
- **Never hardcode the version** in tests or application code. Use `importlib.metadata.version("kitaru")` to read it at runtime.
- **Update `CHANGELOG.md`** when making user-facing changes. Add entries under the `[Unreleased]` heading. The release-preparation PR converts `[Unreleased]` to a versioned heading (e.g. `[0.2.0] - 2026-04-01`), and the development-reset PR restores an empty `[Unreleased]` afterward. `CHANGELOG.md` uses the `merge=union` driver (see `.gitattributes`), so a local `git merge origin/develop` keeps both sides when two PRs added lines under the same heading. GitHub's conflict check ignores merge drivers, so a PR that shows as conflicting only on the changelog is fixed by merging `develop` locally and pushing; after such a merge, check the `[Unreleased]` section for a duplicated line, which the union driver does not remove.
- **Track deprecations in `DEPRECATIONS.md`** at the repository root, one entry per deprecated surface.

## Commits and PRs

- **Never merge a PR that has not been human-reviewed.** Every PR — including dependabot bumps and small hygiene changes — needs an approving review from a human before it merges. Do not bypass the review requirement with `gh pr merge --admin` or any other mechanism, even when all checks are green and the change looks trivial. Documented review of every change is part of our change-management controls (relevant to compliance frameworks such as SOC 2), so an unreviewed merge is a process violation, not just a style issue. Prepare the PR, request review, and wait.
- **Run CI checks locally before committing/pushing.** Always run `just check` and `just test` before pushing to `develop`. All checks must pass locally — do not rely on CI to catch failures. This includes format, lint, typecheck, typos, yaml, actions lint, links, and tests.
- **Keep pre-existing failures separate.** If `just check` or `just test` surfaces a failure unrelated to the requested change, diagnose and report it. Fix it only when it blocks the scoped change or the user explicitly approves expanding the task; do not absorb another contributor's work into the current commit by default.
- **Commits:** Imperative mood, concise summary (50 chars or less): "Add feature" not "Added feature". Explain *why* in the body (blank line after summary), reference issues when applicable (`Fixes #1234`).
- **Bug fixes:** Always add a regression test that would have caught the bug. Understand root cause before implementing the fix.
- **PRs:** Human-readable titles with no "feat:", "doc:", or "[Codex]" prefixes. Write comprehensive descriptions: what the changes do, why they're needed, key implementation decisions, and areas needing reviewer attention.
- **PR reviewer guidance:** Every PR description should include a "Reviewer Notes" H2 or H3 section, but it should read like a guided walkthrough rather than a file inventory. Explain the story of the change, where the risky behavior lives, what would break if the implementation is wrong, and why the named files or functions matter.
- **PR reproduction:** Include a concrete "Reproduction" subsection inside Reviewer Notes or immediately after it. Prefer a runnable example, API flow, or UI path that proves the behavior end to end. Tell the reviewer exactly what to run and what to look for afterward.
- **PR local checks:** Do not create a standalone "Verification" section that only lists `just check`, `just test`, or `/simplify`. Those are still required local hygiene, but they are not useful reviewer guidance by themselves. If useful, include them as a short "Local checks run" note after the reproduction instructions.
- **Before opening a PR or making a large commit**, always run `/simplify` to review changed code for reuse opportunities, quality issues, and efficiency improvements. Fix any issues it finds before committing.
- **Preserve quickstart example compatibility** when changing Kitaru behavior used by `examples/python/pydantic_ai_ticket_resolver`. Inspect its current contract and validate the affected example checks before opening a PR.

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
