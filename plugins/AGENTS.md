# Plugin Guidelines

Read `plugins/DEVELOPMENT.md` before you change package metadata, default definitions, pins, artifact tests, or plugin release workflows.

## Package boundaries

- Treat each directory under `plugins/packages/` as an independent Python distribution.
- Adapter distributions are installed directly by agent projects and do not belong in the default server plugin catalog.
- Release one importer distribution when only that provider changes.
- Release `kitaru-evaluator` when any built-in evaluator changes.
- Do not bump unrelated plugin distributions.
- Keep the definitions in `src/kitaru/server/api/bootstrap.py` aligned with their owning wheels.

## Implementation and release preparation

- Feature PRs change implementation, tests, and `Unreleased` changelog entries. Leave existing package versions and server default requirements/display versions unchanged.
- The release-preparation PR selects plugin versions and updates matching `DEFAULT_PLUGIN_DEFINITIONS` requirements and display versions together. The release inventory validates their equality.
- Preserve a published core dependency floor when it remains supported. If new behavior requires unreleased core, follow the exact development-pin policy in `plugins/DEVELOPMENT.md` and record the core change in `Release context`.
- Release prep replaces development pins with the selected core release floor and regenerates `plugins/uv.lock` before publication.
- New packages need initial metadata. Adding a server default requires an explicit release decision.
- Keep adapter distributions out of the server catalog.
- Run plugin workspace commands with `--project plugins`; the root workspace contains only Kitaru.
- Include relevant `plugins/uv.lock` changes in the patch and in any requested commit.
- Keep `.github/workflows/ci.yml`, `.github/workflows/release-plugins.yml`, and `plugins/packages/` aligned when you add or remove a distribution.

## Required tests

During iteration, run the focused tests and file-scoped checks for the changed behavior. Before handing off a plugin implementation PR, run the workspace format, lint, typecheck, and full test commands below. Metadata, pin, default-definition, and release-path changes also require the artifact smoke check. Documentation-only changes need relevant documentation checks, not the plugin test suite.

- Run the focused plugin tests for changed behavior.
- Run `uv run --project plugins ruff format --config plugins/pyproject.toml --check plugins` and `uv run --project plugins ruff check --config plugins/pyproject.toml plugins`.
- Run `uv run --project plugins ty check --project plugins`.
- Run `uv run --project plugins pytest -q -c plugins/pyproject.toml plugins/tests tests/server/test_default_plugins.py`.
- Run `just plugin-artifact-smoke` for default definitions, package metadata, pins, or release-path changes.
- Use `plugins/docker-compose.candidate.yml` when a change needs a candidate-image rehearsal.
- Include relevant candidate Dockerfile and Compose configuration changes in the patch and in any requested commit. Do not commit generated candidate wheels.
- Do not change production release Dockerfiles to support local plugin wheels.

## Development registration

- Register a self-contained in-progress plugin with `--script` and an explicit entrypoint.
- Use an exact package requirement when the test must cover wheel installation or package imports.
- Remember that package registration stores metadata. It does not upload a wheel.

## Release safety

- Use the `Release Kitaru plugins` workflow. Do not publish plugin distributions from a developer machine.
- Use the non-publishing rehearsal in `plugins/DEVELOPMENT.md` on an eligible reviewed commit.
- Publish only from a namespaced package tag reachable from `develop` or that unit's matching maintenance branch.
- For coordinated releases, wait for the required core version on PyPI before pushing dependent plugin tags. The remaining core jobs can continue in parallel.
- Use the tag format documented in `plugins/DEVELOPMENT.md`.
- Never reuse a PyPI version or package tag.
- Confirm the package name, package version, Git commit, PyPI project, and tag before approval.
