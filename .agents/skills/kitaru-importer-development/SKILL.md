---
name: kitaru-importer-development
description: Add or change a separately packaged Kitaru trace importer that normalizes provider exports into imported sessions. Use for importer packages under plugins/packages, not recording adapters or unrelated SDK, API, or server work.
---

# Kitaru Importer Development

Read `AGENTS.md`, `plugins/AGENTS.md`, `plugins/DEVELOPMENT.md`, and `src/kitaru/task/importer.py`. Treat the task module as the executable importer contract. Read the JSONL importer first for the smallest example, then the closest provider importer and its fixtures. Load the same-name `kitaru-dev` repo skill for the current host for the general command and PR workflow. When an importer version or another release unit changes, also load the `kitaru-release` skill before opening or updating the PR. State whether each changed distribution will ship in the next applicable release or be deferred past it; attach its exact release-unit label when publication is deferred past that release.

First decide whether the request needs a reusable package. A one-off conversion to Kitaru JSONL or a self-contained registered script may be smaller. Registration stores source metadata; it does not upload or vendor a package.

## Implement the parser contract

Implement `parse(payload: bytes, params: dict[str, Any])` as an iterator of `ImportedSession` or `ImportFailure`.

- Choose and document the source-to-session boundary.
- Derive a stable source `external_id`; Kitaru deduplicates using the importer provider and this ID.
- Preserve source inputs and outputs. Populate selectors, models, tokens, costs, reasoning, framework, attributes, and metadata only when the export supports them without guessing.
- Yield `ImportFailure` for an isolated bad record. An exception raised while starting or advancing the parser ends the import task.
- Preserve valid node ordering and graph relationships. Use either nested nodes or the flat indexed form accepted by `flatten_nodes`; follow that function's validation rules.
- Validate provider parameters and keep normalization or grouping provenance in metadata when it changes how the source is interpreted.

Use representative provider exports as fixtures. Cover malformed records, missing or duplicate IDs, grouping, ordering, parent links, model and tool normalization, reasoning visibility, selector escaping, and stable re-import behavior as applicable.

## Importer-backed adapter

An importer package for a provider with a live read API can also ship an importer-backed adapter: a class subclassing `kitaru.importer_adapter.ImporterBackedAdapter` that wraps the user's agent entrypoint, waits for the provider to ingest the trace, fetches it, and imports it through the package's own parser. Read `src/kitaru/importer_adapter.py` and the langfuse importer package as the reference implementation before adding one.

- Split the code into `adapter.py` (the subclass: trace pinning, SDK buffer flush, per-run cache) and `api.py` (the provider read layer: credential and client resolution, the completeness poll, single fetch, serialization to parser payload bytes). Keep `api.py` adapter-free so future direct-from-API imports can reuse it.
- Run blocking SDK calls inside the async hooks via `asyncio.to_thread`, never in `open_trace()` teardown.
- Provider SDK dependencies belong in the importer package's `adapter` extra. Users import the adapter from the `adapter` submodule, never from the package `__init__`, so `parse` never loads the SDK. Adapter tests live under `plugins/tests/adapters/<slug>/` and run the real parser against a faked provider SDK.
- Agent versions whose run spec command uses such an adapter must declare `runtime_capabilities` with `overrides: false` and `tool_policies: false`, since the runtime cannot intercept model or tool calls. Replay and experiment run creation reject configs the declaration cannot apply, and the adapter raises on such configs as a backstop.

## Package, register, and version

An importer distribution lives under `plugins/packages/<slug>-importer/`, with its source, `pyproject.toml`, changelog, and focused tests under `plugins/tests/importers/`. Export `parse` through the package `__all__`. Add the package to `plugins/README.md`, `release/release-units.toml`, the exact inventory in `tests/scripts/test_release_units.py`, and `plugins/uv.lock`. A non-default package also declares `tool.kitaru.artifact.import-module` so artifact smoke can import it without a default-catalog entry.

Use `kitaru importer scaffold` and `kitaru importer test` for bounded local scripts. Register an in-progress self-contained implementation with `kitaru importer register ... --script ... --entrypoint ...`. Use an exact package requirement when validation must cover wheel installation. Registration creates remote state, is not idempotent, and needs an explicit server plus a worker that can resolve the source. Do not run it without authorization. The package's PyPI version and Kitaru's server-assigned importer version are separate: use `kitaru importer version register` for each new immutable registered implementation, and never mutate the behavior behind an existing version.

A default importer additionally needs:

- `default-catalog = true` in `release/release-units.toml`
- a matching `DEFAULT_PLUGIN_DEFINITIONS` entry in `src/kitaru/server/api/bootstrap.py`
- catalog coverage in `tests/server/test_default_plugins.py`

Do not make an importer a server default merely because its package exists. Default-catalog inclusion is a separate product and deployment decision.

Document only shipped importers. Update the relevant guide under `docs/book/guides/`, the importing overview, and `docs/book/toc.md` when the provider is actually available.

## Core boundary

An importer normally adapts provider data to the existing contract without changing Kitaru core. Stop and surface the missing contract before changing any of these areas merely to complete an importer:

- `openapi/`
- `src/kitaru/api_models/`
- `src/kitaru/client/`
- `src/kitaru/server/`, except an explicitly approved default-catalog entry
- `src/kitaru/worker/`
- CLI or MCP code

Explain which source fact cannot be represented, why metadata or the current node/session models are insufficient, and the smallest separate contract decision that would unblock it. Continue only when that broader change is explicitly in scope.

## Validation

Run the focused importer tests and `plugins/tests/importers/test_normalization.py` when shared normalization semantics are involved. Then run the plugin workspace format, lint, typecheck, and test commands from `plugins/AGENTS.md`.

Run `just plugin-artifact-smoke` after package metadata, default definitions, requirement pins, entrypoints, or release installation paths change. Run `tests/scripts/test_release_units.py` after adding or changing a release unit. Include `tests/server/test_default_plugins.py` for default-catalog changes. Use the candidate-server procedure in `plugins/DEVELOPMENT.md` whenever package registration or task execution changes, whether or not the importer is a default.
