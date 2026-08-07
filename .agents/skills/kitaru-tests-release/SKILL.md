---
name: kitaru-tests-release
description: Use for Kitaru tests, CI, releases.
---

# Kitaru Tests, CI, and Release Workflow

Use this when adding, moving, or debugging tests beyond the basic commands and safety rules in `tests/AGENTS.md`, or when changing CI/release behavior.

## Unit and Contract Tests

- Build small typed stubs or `SimpleNamespace` objects instead of booting unrelated runtime state.
- Assert one behavior or contract at a time.
- Include regression coverage for bug fixes.
- Keep tests independent of working-directory state unless the test creates that state explicitly.
- Avoid shared mutable module-level state and wall-clock ordering assumptions.

V2 has no `primed_zenml` fixture or `test_phase*` example suite. Do not copy those v1 patterns into new tests.

## CLI Tests

- Install the surface with `uv sync --extra cli --extra worker`.
- Call `src/kitaru/cli/app.py::main` with an explicit argument list.
- Assert the returned integer exit code, such as `main(["--help"]) == 0`; successful calls do not raise `SystemExit(0)`.
- Use `capsys` and assert stable structured or plain-text contracts.
- Prefer lightweight stubs for remote resources instead of starting the full server.
- Exercise offline help, version, schema, and scaffold commands without reading local configuration.
- Run `just cli-artifact-smoke` after optional-dependency, entrypoint, or packaging changes.

Keep CLI tests focused on argument parsing, command dispatch, output contracts, and the specific resource interaction under test.

## Server and SDK Tests

Follow the four server-resource surfaces in `tests/AGENTS.md`: service tests, ASGI REST tests, shared repository contracts, and PostgreSQL end-to-end tests. Add SDK round-trip coverage under `tests/client/` when a public resource changes.

Use the PostgreSQL-backed tests for transaction, locking, migration, or cross-request behavior that an in-memory fake cannot prove. Run `docker compose up -d db` before those tests and `just migration-check` after schema changes.

## Task and Worker Tests

- Task subprocess contracts live under `tests/task/`.
- Worker lifecycle and handler contracts live under `tests/worker/`.
- Keep subprocess tests bounded and assert structured receipts, exit behavior, and redaction.
- Use the existing worker fakes rather than starting unrelated services.

## Default Plugin Packages

- Run `just plugin-artifact-smoke` after changing plugin package metadata, catalogs, default requirement pins, or release installation paths.
- The smoke builds Kitaru and every selected plugin as wheels, installs them into a clean environment, loads each catalog entrypoint, and verifies idempotent default registration.
- CI runs plugin distributions as a package matrix. Keep the matrix aligned with `plugins/packages/` and the choices in `.github/workflows/release-plugins.yml`.
- Kitaru release dry-runs build client and server images from `docker/candidate-wheels`; publishing builds continue to install exact versions from PyPI.

## MCP Tests

- Synchronize with `uv sync --frozen --extra mcp` and run `just test tests/mcp`.
- Keep handler tests typed and bounded; use resource-shaped fake `KitaruAPIClient` objects unless a protocol or real-server contract requires deeper integration.
- Test capability filtering through `MCPServer.list_tools()`, not by calling decorated functions alone.
- Treat `tests/mcp/snapshots/metrics.json` and `src/kitaru/mcp/registry.py` as the tool-inventory authorities. Do not hardcode copied inventory counts in instructions.
- Run `just mcp-schema-check` after any input/output model, registry, annotation, description, or MCP SDK change. Snapshot changes require explicit MCP API review.
- Build the wheel and run `just mcp-wheel-smoke` after launcher, packaging, lifecycle, or optional-import changes.
- Preserve stable request-ID forwarding, mixed-version refusal, bounded preflight reads, and text/structured response parity where the existing contracts require them.

## Bug Fix Workflow

Every bug fix should include a regression test that would have caught the original problem:

1. Write or update the test so it captures the broken behavior.
2. Run it and observe the expected failure when practical.
3. Make the code change.
4. Rerun the targeted test.
5. Rerun the broader relevant suite.

If code changes after a successful test run, run the affected tests again.

## Feature Completion Checklist

When adding a new CLI command, MCP tool, SDK resource, task, or worker capability:

- Add or update focused tests for the changed surface.
- Update offline CLI registration metadata for CLI commands.
- Update `examples/example-coverage.yaml` and run `just example-coverage-audit` when examples are added, removed, renamed, or publicly documented.
- Review analytics coverage and add events only through the current v2 analytics paths.
- Run the relevant CLI artifact, MCP schema, MCP wheel, migration, OpenAPI, or package-build checks for the changed contract.

## Python CI

`.github/workflows/ci.yml` runs on pushes to `develop` and on pull requests. It includes separate base, CLI, and MCP matrices across Python 3.11 through 3.14, plus installed CLI-artifact and MCP-wheel contracts. Push-only jobs cover Docker server smoke and UI wheel packaging because those paths may require trusted UI release credentials.

Do not describe the inherited `llm-integration.yml` provider markers or absent `tests/live/` suite as v2 release evidence. V2 currently has no tracked `live_llm`, `live_openai`, `live_anthropic`, or `live_gemini` test surface.

## Docs CI

`.github/workflows/docs.yml` runs on manual dispatch, `main` pushes, and selected docs/reference pull-request paths. It is configured to regenerate SDK reference docs and build the FumaDocs export, but the v2 checkout currently lacks `scripts/generate_sdk_docs.py`; treat that workflow as blocked until a v2 generator is restored. Deployment conditions remain `main` push or manual dispatch, and hand-written docs publish separately through GitBook Git Sync.

## Release Workflow

The authoritative release procedure is `.github/workflows/release.yml`. A normal dispatch checks out `develop`; a recovery dispatch may reuse an existing release tag. Therefore a v2 feature branch cannot be released through the normal dispatch until the intended v2 history is present on the release source branch.

Before dispatching:

1. Fetch `develop`, `main`, and tags.
2. Confirm the intended release commits are on `develop` and identify the last immutable release tag.
3. Review the `[Unreleased]` changelog and version classification.
4. Confirm no other release run is active.
5. Run `just check`, the relevant base/CLI/MCP tests, `just mcp-schema-check`, `just cli-artifact-smoke`, `just plugin-artifact-smoke`, `just migration-check`, and `just build` as applicable. Run `just mcp-wheel-smoke` only after `just build`; it consumes the wheel under `dist/`.
6. Use the workflow's `dry-run` input when a non-publishing rehearsal is needed.

The release workflow itself installs the CLI, MCP, and worker extras; checks lint, types, MCP schemas, base/CLI/MCP tests, clean CLI artifacts, migrations, UI wheel contents, and the installed MCP wheel contract; then handles versioning, tagging, PyPI, GitHub Release, public and managed images, Helm packaging, release branches, and `main` advancement.

Do not use the removed `scripts/smoke-test.sh`, provider-area flags, remote-stack smoke, v1 adapters, or local ZenML flow runs as v2 release gates.

## Branching and Releases

- Default branch is `develop`.
- Pull requests normally target `develop`; v2 feature work may target its explicit integration branch until that migration lands.
- `main` tracks the latest released version only; do not push directly.
- Releases are cut through `.github/workflows/release.yml` by dispatch or `v*` tag push.
- The workflow maintains the version in `pyproject.toml`; application code should use `importlib.metadata.version("kitaru")` rather than hardcoding it.
- Update `CHANGELOG.md` under `[Unreleased]` for user-facing changes.
