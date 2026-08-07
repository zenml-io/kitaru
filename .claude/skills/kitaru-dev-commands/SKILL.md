---
name: kitaru-dev-commands
description: Full reference for Kitaru v2 development commands, tests, docs generation, packaging, Docker builds, and CI workflows. Use when a task needs commands beyond just check, just fix, or just test.
---

# Kitaru v2 development commands

Run `just --list` for the recipe inventory, then verify that a recipe's backing files exist before using it on v2. Treat `pyproject.toml`, source, tests, and existing scripts as authoritative when inherited recipes or workflows disagree.

## Environment setup

```bash
uv sync                              # Base SDK and development dependencies
uv sync --extra cli                  # Optional CLI
uv sync --extra mcp                  # Optional native MCP server
uv sync --extra server               # FastAPI server and database dependencies
uv sync --extra worker               # Worker runtime
uv sync --extra otel                 # OpenTelemetry integrations
```

There is no v2 `local` extra, `kitaru init` command, or `.kitaru/` project-marker setup.

## Core workflow

```bash
just fix                              # Auto-fix formatting, lint, and YAML
just check                            # Format, lint, types, typos, YAML, actions, links
just test                             # Full pytest suite
just test tests/cli                   # One test surface
just test tests/cli/test_app.py::test_help_version_schema_and_scaffold_skip_bootstrap
```

The full suite can be summarized without losing failure names:

```bash
just test 2>&1 | grep -E "FAILED|ERROR|passed|failed" | tail -20
```

## Focused checks

```bash
just format-check
just lint
just typecheck
just typos
just yaml-check
just actions-lint
just zizmor
just audit
just links
just links-external
just example-coverage-audit
just migration-check                    # Requires docker compose up -d db
just build
```

## CLI and MCP contracts

```bash
just cli-artifact-smoke                  # Clean CLI wheel and source installs
just mcp-schema-check                    # Registry schemas, budgets, snapshots
just mcp-wheel-smoke                     # Clean base and [mcp] wheel installs
uv run --extra cli kitaru schema         # Offline CLI command catalog
```

Do not copy MCP tool counts into this skill. Read `tests/mcp/snapshots/metrics.json` and run `just mcp-schema-check`.

CLI tests call `src/kitaru/cli/app.py::main` with explicit arguments and assert its returned integer code. They do not expect `SystemExit(0)`.

## Docs workflows

These require Node 22+ and pnpm.

```bash
just docs                                # Preview at localhost:3000
just docs-build                          # Static export
just docs-validate                       # Validate export under /docs
```

`scripts/generate_sdk_docs.py` is absent from v2, so `just generate-docs` and the SDK-reference generation step in docs CI are blocked. Do not restore the deleted v1 generator without reviewing the v2 public SDK. CLI reference publishing is deferred; `kitaru schema` is the current offline CLI contract.

## UI and Docker

Read `FRONTEND-TESTING.md` before changing UI bundle, frontend smoke, Docker dashboard, or release UI behavior.

```bash
just ui-bundle                           # Download latest stable UI bundle
just UI_TAG=kitaru-ui-v0.2.0 ui-bundle  # Pin a stable UI bundle
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-bundle-prerelease
```

The inherited `just ui-login` recipe calls `kitaru login` without the required `SERVER` argument or `--local`, so do not advertise it as a working v2 command. After preparing a bundle, set the printed `KITARU_UI_DIST_PATH` and use `uv run kitaru login SERVER` or `uv run kitaru login --local` against an already-running server.

Do not advertise `just ui-smoke` or `just release-smoke` as working v2 checks while they still call the removed `scripts/smoke-test.sh`.

The v2 Dockerfiles are `docker/dev-client.Dockerfile`, `docker/dev-server.Dockerfile`, `docker/release-client.Dockerfile`, and `docker/release-server.Dockerfile`. Follow `docker/CLAUDE.md`; do not use inherited v1 `server-image` recipes as v2 release evidence.

## CI/CD workflows

| Workflow | Current v2 role |
|---|---|
| `ci.yml` | Pushes to `develop` and pull requests. Base, CLI, and MCP test matrices cover Python 3.11-3.14; separate jobs validate CLI artifacts, installed MCP wheels, migrations, Docker server startup, and UI wheel packaging. |
| `docs.yml` | Intended SDK reference build/deploy workflow. It is currently blocked because the v2 Python SDK generator is absent; deployment conditions remain `main` push or manual dispatch. |
| `release.yml` | Validates source and built artifacts, then performs versioning, PyPI, GitHub Release, Docker/ECR, Helm, release-branch, and `main` updates. Normal dispatch releases `develop`, not the current feature branch. |
| `spellcheck.yml` | Separate typo/spell checking. |
| `image-optimiser.yml` | Compresses changed images on eligible pull requests. |
| `zizmor.yml` | Audits GitHub Actions security. |

The inherited `llm-integration.yml` still references absent v1 provider tests. Do not use it or the missing `tests/live/` marker suite as proof of v2 behavior.
