---
name: kitaru-dev
description: Use for Kitaru commands, CLI, analytics, PRs.
---

# Kitaru Development, CLI, and PR Workflow

Use this when you need the command catalog beyond the daily loop in the root `AGENTS.md`, or when adding CLI commands, analytics events, or PR descriptions.

## Python Workflows

- `uv sync`: install the base SDK and development dependencies
- `uv sync --extra cli`: include the optional CLI
- `uv sync --extra mcp`: include the optional native MCP server
- `uv sync --extra server`: include server components
- `uv sync --extra worker`: include worker components
- `uv sync --extra otel`: include OpenTelemetry integrations
- `just check`: run formatting, lint, OpenAPI freshness, typecheck, typos, YAML, actions lint, and links
- `just openapi-check`: verify that the committed OpenAPI specification matches the application schema
- `just fix`: auto-fix formatting, lint issues, and YAML
- `just test`: run the full pytest suite
- `just test tests/test_file.py::test_name`: run one targeted test
- `just lint`: lint only
- `just typecheck`: type check only
- `just typos`: typo check only
- `just format-check`: check formatting without modifying files
- `just yaml-check`: check YAML formatting
- `just actions-lint`: lint GitHub Actions workflows; requires `actionlint`
- `just zizmor`: audit GitHub Actions workflow security with `zizmor`
- `just audit`: audit Python dependencies with `pip-audit` and the documented ignore list
- `just links`: check Markdown links offline; requires `lychee`
- `just links-external`: check links including external URLs; slow
- `just example-coverage-audit`: validate `examples/example-coverage.yaml` metadata and waivers
- `just build`: build wheel and sdist locally
- `just cli-artifact-smoke`: verify clean CLI wheel and source installations
- `just plugin-artifact-smoke`: build every default-plugin wheel, load its configured entrypoints, and verify default registration
- `just mcp-schema-check`: verify public MCP registry budgets and committed snapshots
- `just mcp-wheel-smoke`: verify clean base and `[mcp]` installs from the wheel under `dist/`
- `just migration-check`: compare Alembic migrations with the ORM schema; requires PostgreSQL

There is no v2 `kitaru init` command or `local` extra. Do not carry the v1 `.kitaru/` project-marker setup into v2 instructions or tests.

When merging the v2 base into a feature branch and resolving `pyproject.toml` or `uv.lock`, check recent dependency-security changes before regenerating the lockfile broadly. Use targeted upgrades when a package was intentionally bumped and run `just audit` before pushing.

## Docs Workflows

These require Node 22+ and pnpm.

- `just docs`: preview docs locally at `localhost:3000`
- `just docs-build`: build the static docs export
- `just docs-validate`: validate the export as served under `/docs`
- `just generate-docs`: regenerate the SDK and CLI reference content

`scripts/generate_sdk_docs.py` extracts the v2 SDK reference through a `PUBLIC_API` allowlist. Edit that allowlist and `tests/scripts/test_generate_sdk_docs.py` together; the test compares each published module against its `__all__`. The generator needs the fumapy bridge after installing the docs dependencies. `scripts/generate_cli_docs.py` generates CLI reference content from the offline `kitaru schema` contract rather than a hardcoded command list.

## Native MCP Server

The native v2 server is installed with `kitaru[mcp]` and started with `kitaru-mcp`. It defaults to `read-only`; `standard` and `destructive` expose progressively broader capabilities.

Treat `tests/mcp/snapshots/metrics.json` and `src/kitaru/mcp/registry.py` as the inventory authorities. Do not copy tool counts into prose. Run `just mcp-schema-check` after changing MCP models, registry declarations, descriptions, annotations, or SDK versions. Build the wheel and run `just mcp-wheel-smoke` after entrypoint, packaging, lifecycle, or optional-import changes.

## CLI Structure

The `kitaru` console script is defined in `pyproject.toml` under `[project.scripts]`. `src/kitaru/cli/__init__.py` is the lazy entry point, `src/kitaru/cli/app.py` registers the shared Cyclopts applications, and command implementations live under `src/kitaru/cli/`.

Register new leaf commands through the `_spec(...)` and `_register(...)` metadata in `src/kitaru/cli/app.py`. Tests should call `main([...])` with an explicit argument list and assert the returned integer exit code.

## Structured Output Contract

Agent-facing commands use the version-1 structured contract. Success documents include `schema_version`, `command`, `ok`, `warnings`, `links`, and `next_actions`, plus `item` for one result or `items`, `count`, and `page` for a list. Streaming commands emit JSONL events. Structured errors are one JSON object on stderr with a stable error kind and exit code.

For agent-facing use, prefer `--output json --machine --non-interactive --no-browser`. A deliberate dashboard or device-login handoff is the exception.

Document login consistently: `kitaru login SERVER` targets the full managed or self-hosted instance URL, while `kitaru login --local` provisions or reuses the CLI-owned Docker Compose deployment. It defaults to `http://localhost:8000`; `--port` takes precedence over `KITARU_LOCAL_PORT`, and the selected port persists with the deployment. `kitaru logout` stops that deployment when it is selected, and `kitaru logout --volumes` also deletes its PostgreSQL data.

`kitaru status` shows the selected server, provenance, credential state, compatibility, and live-worker count. `kitaru info` adds local package, Python, platform, and server details. `kitaru doctor` runs independent local, server, authentication, and tooling checks without stopping after the first failure. These commands never print secret values.

## Analytics

Analytics events live in `src/kitaru/analytics/events.py`; source attribution lives in `src/kitaru/analytics/source.py`. Server-side feature events are emitted through the application analytics service. MCP attribution is set once for the MCP lifecycle through `AnalyticsSource.MCP`.

- Add event names to `AnalyticsEvent` in `src/kitaru/analytics/events.py`.
- Track only reviewed, non-sensitive metadata such as event names, boolean flags, enum values, and counts.
- Never include user content, file paths, prompts, credentials, or secret values.
- Keep analytics failures non-fatal.

## Pull Requests

Use a clear human-readable title without a `[Codex]` prefix. Include what changed, why it was needed, important implementation decisions, and reviewer focus areas. Link related issues when applicable.

Every PR description should include a `Reviewer Notes` H2 or H3 section that explains the story and risks of the change, plus a concrete `Reproduction` subsection. Keep local hygiene commands as a short note after reproduction rather than using them as a substitute for reviewer guidance.
