---
name: kitaru-adapter-development
description: Add or change a Kitaru framework adapter that records native agent runs or supports bounded replay. Use for Python adapter distributions under plugins/packages or TypeScript adapter packages under packages, not trace importers or core API work.
---

# Kitaru Adapter Development

Read `AGENTS.md`. For Python adapters, also read `plugins/AGENTS.md` and `plugins/DEVELOPMENT.md`. For TypeScript adapters, read `release/typescript.md` before changing package or release metadata. Load the same-name `kitaru-dev` repo skill for the current host for the general command and PR workflow.

Start from the closest current adapter and the target framework's public API. Do not port a historical adapter wholesale.

## Choose the extension point

Recording adapters are independent Python distributions under `plugins/packages/<slug>/`, with focused tests under `plugins/tests/adapters/<slug>/`. Importer-backed adapters are different: they live inside the provider's importer package as an `ImporterBackedAdapter` subclass, never intercept model or tool calls, and follow the kitaru-importer-development skill instead of this section's package rules. Agent versions running them declare `runtime_capabilities` with `overrides: false` and `tool_policies: false` on the run spec. TypeScript framework adapters are packages such as `packages/mastra/` and `packages/vercel-ai/`; shared adapter primitives live under `packages/core/src/adapter/`.

Before editing, state:

- which public framework call, hook, callback, or wrapper is intercepted
- which model, tool, handoff, child-agent, and failure events are observable there
- when sessions and nodes are written, including what the caller observes if recording fails
- which replay overrides can be applied at a real framework boundary
- which streaming, tool, approval, resume, stateful, or dynamic cases remain unsupported

Preserve the framework's ordinary behavior, configured hooks and state, public entrypoint, and native result type. Reject unsupported replay configurations before model or tool execution. Treat passthrough tools as real side effects, not as a reversible transaction. If the framework has no public per-run model replacement point, do not emulate one by mutating shared agent state or reading private fields; stop and report that replay boundary.

## Package and documentation work

Python feature PRs follow the version and dependency ownership rules in `plugins/AGENTS.md`. Leave existing package versions unchanged. Use the exact development dependency from `plugins/DEVELOPMENT.md` only when the adapter needs unreleased core; release prep selects the published compatibility floor.

For a new Python adapter, complete the new-distribution integration review in the `kitaru-dev` skill before the PR; it covers worker installation and release wiring beyond the adapter package itself. Inspect the current package inventory and update only the required integration points:

- `plugins/packages/<slug>/pyproject.toml`, README, changelog, source package, public exports, and focused tests
- `tool.kitaru.artifact.import-module` for standalone artifact-smoke coverage
- `plugins/pyproject.toml` only when the adapter must be a workspace development dependency or source
- `release/release-units.toml` for an independently released distribution

Python adapters are installed directly by agent projects. Keep `default-catalog = false` in `release/release-units.toml` and do not add them to `DEFAULT_PLUGIN_DEFINITIONS`.

For a new TypeScript adapter, add a separate package instead of framework-specific code in `packages/core/`. Inspect the root workspace scripts, lockstep version rules, packaging smoke, and `.github/workflows/release-typescript.yml`; do not assume a newly added package is automatically built or published.

Document shipped adapters under `docs/book/adapters/`, update `docs/book/adapters/README.md` and `docs/book/toc.md`, and add a runnable example only when it exercises a supported path.

## Core boundary

An adapter request does not authorize a new core abstraction, server resource, or replay protocol. Stop and surface the missing extension point before changing any of these areas merely to finish the adapter:

- `openapi/`
- `src/kitaru/api_models/`
- `src/kitaru/client/`
- `src/kitaru/server/`
- `src/kitaru/worker/`
- CLI or MCP code
- `packages/core/src/adapter/` when the proposed primitive is useful only to one framework package

Explain what the adapter cannot observe or override, why the current public boundary is insufficient, and the smallest separate core decision that would unblock it. Continue only after a maintainer approves that bounded core change; a request to change core "if needed" is not approval to broaden the adapter patch opportunistically.

## Validation

Test the public wrapper, captured node semantics, native result preservation, supported replay, unsupported replay preflight, concurrency, and recording-finalization failures as applicable.

For Python adapters, run the focused adapter tests, then the plugin workspace format, lint, typecheck, and test commands from `plugins/AGENTS.md`. Run `just plugin-artifact-smoke` after package metadata or artifact-loading changes.

For TypeScript adapters, run the affected package's test, typecheck, lint, and build scripts. Run the root `pnpm test`, `pnpm typecheck`, `pnpm lint`, and `pnpm pack:check` when shared primitives, workspace metadata, or packaging changes.

The Mastra adapter is tested against every Mastra release set the memory replay factory accepts. `MEMORY_REPLAY_TESTED_VERSIONS` in `packages/mastra/src/memory-replay-versions.ts` lists those sets (`@mastra/core`, the `@mastra/memory` release built against it, and the `@mastra/pg` release the PostgreSQL tests use), and `assertMemoryReplayVersions` accepts exactly its core and memory pairs. `packages/mastra/` installs the oldest set. Each private `packages/mastra-compat/<major.minor>/` package installs one other set and runs every `packages/mastra` test against it through `packages/mastra-compat/shared/`; the 1.71 package also holds the Mastra compatibility suite. To add a Mastra release, check which memory and pg releases were built against it (their published `devDependencies`), copy a compat package with those pins, run its suite including PostgreSQL, then add the table row and the adapter's `@mastra/memory` peer version. `packages/mastra/test/tested-versions.test.ts` fails when the table, the test packages, the installed versions and the peer range disagree, and the packed-tarball smoke reads the table from the built adapter. Dependabot ignores these pins; see `.github/dependabot.yml`. When you raise the supported `@mastra/core` ceiling for the rest of the adapter, move the compatibility suite to the newest compat package in the same change.

Use live provider or framework tests only when their credentials and external side effects are explicitly in scope.
