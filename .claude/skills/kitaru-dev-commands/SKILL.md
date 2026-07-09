---
name: kitaru-dev-commands
description: Full reference for Kitaru's just recipes, test/lint/typecheck invocations, docs generation, UI bundle scripts, Docker image builds, and the CI/CD workflow table. Use when you need a command beyond `just check` / `just fix` / `just test`, when a CI workflow fails and you need to know what it runs, or when working on docs generation, the Kitaru UI bundle, or Docker images.
---

# Kitaru development commands

This project uses [just](https://github.com/casey/just) as a command stack. Run `just --list` to see all recipes.

## Core workflow (the three commands you'll use most)

| Command | What it does | When to run |
|---|---|---|
| **`just check`** | Runs *all* checks: format, lint, typecheck, typos, yaml, actions lint, links | After every chunk of work and before committing/pushing |
| **`just fix`** | Auto-fixes formatting, lint issues, and yaml | When `just check` reports fixable issues — handles most linting problems automatically |
| **`just test`** | Runs the full pytest suite | After code changes and before committing/pushing |

**Typical loop:** write code → `just fix` (auto-fix what it can) → `just check` (verify everything passes) → `just test` (make sure nothing is broken) → commit.

**Worktree setup gotcha:** In a fresh `git worktree`, the `test_phase*_example::*_runs_end_to_end` tests (~14 of them) fail with `RuntimeError: Unable to resolve dynamic pipeline source. Make sure your pipeline is defined at the top level of your module.` This is because ZenML's dynamic pipeline resolver uses `get_source_root()` to locate the project root before re-importing the pipeline module by dotted path, and that root comes from the `.kitaru/` marker. Worktrees don't inherit `.kitaru/` from the main checkout, so the resolver can't find `examples.features.basic_flow....` on `sys.path`. **Fix:** run `uv run kitaru init` once in the new worktree after `uv sync`. The unit test suite passes without this — only the end-to-end example tests need it.

```bash
# Setup
uv sync                              # Install dependencies
uv sync --extra local                # Include local ZenML runtime components
uv run kitaru init                   # Required in a fresh git worktree — see note above

# Common Python workflows
just check                            # Run all checks (format, lint, typecheck, typos, yaml, actions lint, links)
just test                             # Run all tests
just test tests/test_foo.py           # Run a single test file
just test tests/test_foo.py::test_bar # Run a single test
just test -x                          # Stop on first failure
uv run pytest -m live_openai          # Explicit live OpenAI checks (requires key)
uv run pytest -m live_anthropic       # Explicit live Anthropic checks (requires key)
just fix                              # Auto-fix formatting, lint, and yaml

# Default pytest excludes live provider tests with -m 'not live_llm'.
# Tests under tests/live/ are paid/external checks and must be selected
# explicitly with provider credentials available.

# Agent tip: the full suite takes ~4 minutes. When running it through a
# pager/truncated stream that may drop the failure list, pipe through
# grep so the failure names survive:
#   just test 2>&1 | grep -E "FAILED|ERROR|passed|failed" | tail -20
# That keeps the PASS/FAIL summary and every FAILED line without
# forcing a rerun just to recover the list.

# Individual checks
just lint                             # Lint only
just typecheck                        # Type check only
just typos                            # Typo check only
just format-check                     # Check formatting without modifying
just yaml-check                       # Check YAML formatting
just actions-lint                     # Lint GitHub Actions workflows (requires actionlint)
just zizmor                           # Audit GitHub Actions workflow security
just audit                            # Audit Python dependencies with pip-audit
just links                            # Check markdown links offline (requires lychee)
just example-coverage-audit           # Validate example metadata and required waivers; audit-only, no provider calls
just build                            # Build wheel + sdist locally

# Docs workflows (require Node 22+ and pnpm)
just generate-docs                    # Generate CLI reference + changelog + SDK reference docs
just docs                             # Preview docs dev server (localhost:3000)
just docs-build                       # Build docs static export
just docs-validate                    # Validate the static export as served under /docs

# Kitaru UI bundle testing
# Read FRONTEND-TESTING.md before changing UI bundle, frontend smoke, Docker dashboard, or release UI workflows.
just ui-bundle                                # Download latest stable/full kitaru-ui-v* bundle
just UI_TAG=kitaru-ui-v0.2.0 ui-bundle        # Download a specific stable UI bundle
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-bundle-prerelease  # Explicit prerelease opt-in
just ui-login                                 # Start local Kitaru with prepared bundle
just ui-smoke                                 # Smoke test prepared bundle and keep server running

# Docker
just server-image                              # Build production server image (bundles latest stable UI first)
just DOCKER_TAG=v0.2.0 server-image            # Build with specific image tag
just UI_TAG=kitaru-ui-v0.2.0 server-image      # Build with specific stable Kitaru UI release
just server-image-push                         # Build + push to Docker Hub
just server-dev-image                          # Build dev server image (requires docker/kitaru-ui-dist/)

# Public website deploys are owned by the sibling zenml-io-v2 repository.
```

Note: `zizmor` is not part of `just check`. `just check` runs `actionlint` (a syntax linter); `zizmor` is a separate security scanner with its own recipe and a path-filtered workflow. Run `just zizmor` locally before pushing workflow changes.

When working with Python, invoke the relevant `/astral:<skill>` for uv, ty, and ruff to ensure best practices are followed.

## CI/CD workflows

| Workflow | Trigger | Purpose |
|---|---|---|
| `ci.yml` | Push/PR to `develop` | PRs run Python checks: lint, format, yaml, typos, typecheck, dependency audit, links, and tests across base installs (3.11 + 3.12 + 3.13) plus additional `kitaru[mcp]` test lanes. Pushes also run Docker server smoke and wheel packaging because those jobs may need trusted UI release credentials. |
| `docs.yml` | Manual dispatch; push to `main`; selected docs/script/source PR paths | Regenerate the CLI/SDK reference and build the static docs app on every run. Deploy the SDK+CLI reference site (`sdkdocs.kitaru.ai`, worker `kitaru-sdkdocs`) plus the `kitaru.ai/docs` redirect worker (`kitaru-site`) only on `main` push or manual dispatch. PRs build only and do not create preview Workers. Hand-written docs (`docs/book/`) publish separately via GitBook Git Sync. |
| `release.yml` | Workflow dispatch or `v*` tag | Stable Kitaru UI bundling, version/changelog/lock handling for dispatch releases, PyPI publish, Docker image publish, Helm OCI chart publish, release branch/main update, GitHub Release. No live provider calls. |
| `llm-integration.yml` | Weekly schedule; manual dispatch | Trusted live OpenAI/Anthropic provider checks outside PR CI. Manual runs can target an exact Kitaru ref/SHA and select `provider-core`, `provider-extended`, OpenAI, Anthropic, and the opt-in research bot. Uploads logs/results only and sends Discord failure alerts. Secret-bearing jobs use the GitHub Environment `live-provider-tests`; put `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, and `DISCORD_WEBHOOK_SRE` in that Environment with `kitaru-admins` approval/restrictions. |
| `ui-prerelease-smoke.yml` | Manual dispatch | Tests an explicit prerelease Kitaru UI bundle against a Kitaru ref without publishing PyPI, Docker, Helm, tags, or releases |
| `spellcheck.yml` | Manual/reusable runs, push to `develop`, non-draft PRs | Separate typo/spell checking |
| `image-optimiser.yml` | PRs changing JPG/JPEG/PNG/WebP files | Image compression for same-repo non-draft PRs |
| `zizmor.yml` | Workflow/dependabot changes, weekly schedule, manual runs | GitHub Actions security analysis |
