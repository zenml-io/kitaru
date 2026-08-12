# Plugin Guidelines

Read `plugins/DEVELOPMENT.md` before you change package metadata, default definitions, pins, artifact tests, or plugin release workflows.

## Package boundaries

- Treat each directory under `plugins/packages/` as an independent Python distribution.
- Adapter distributions are installed directly by agent projects and do not belong in the default server plugin catalog.
- Release one importer distribution when only that provider changes.
- Release `kitaru-evaluator` when any built-in evaluator changes.
- Do not bump unrelated plugin distributions.
- Keep the definitions in `src/kitaru/server/api/bootstrap.py` aligned with their owning wheels.

## Required updates

- Update the selected package version with `uv version --project plugins --package DISTRIBUTION VERSION --no-sync`.
- For a plugin-only release of a default importer or evaluator, publish the distribution first and update `plugins/default-requirements.txt` plus `DEFAULT_PLUGIN_DEFINITIONS` in the next core release PR.
- When a coordinated plugin and core release is intentional, update the selected plugin version, exact default pins, and core version in the same PR.
- Do not add adapter distributions to the default requirements or server catalog.
- Run plugin workspace commands with `--project plugins`; the root workspace contains only Kitaru.
- Commit the resulting `plugins/uv.lock` change.
- Keep `.github/workflows/ci.yml`, `.github/workflows/release-plugins.yml`, and `plugins/packages/` aligned when you add or remove a distribution.

## Required tests

- Run the focused plugin tests for changed behavior.
- Run `uv run --project plugins ruff format --config plugins/pyproject.toml --check plugins` and `uv run --project plugins ruff check --config plugins/pyproject.toml plugins`.
- Run `uv run --project plugins ty check --project plugins`.
- Run `uv run --project plugins pytest -q -c plugins/pyproject.toml plugins/tests tests/server/test_default_plugins.py`.
- For a plugin-only release, run `uv run --no-sync python scripts/smoke_plugin_artifacts.py --package PACKAGE_DIR --allow-default-pin-mismatch`.
- Run `just plugin-artifact-smoke` for coordinated core releases, default definitions, catalog pins, or release-path changes.
- Use `plugins/docker-compose.candidate.yml` when a change needs a candidate-image rehearsal.
- Commit the candidate Dockerfile and Compose configuration. Do not commit generated candidate wheels.
- Do not change production release Dockerfiles to support local plugin wheels.

## Development registration

- Register a self-contained in-progress plugin with `--script` and an explicit entrypoint.
- Use an exact package requirement when the test must cover wheel installation or package imports.
- Remember that package registration stores metadata. It does not upload a wheel.

## Release safety

- Use the `Release plugin` workflow. Do not publish plugin distributions from a developer machine.
- Run a dry-run from the feature branch before merge.
- Publish only from a package tag whose commit is contained in `main`.
- A correctly labeled PR merged to `main` creates the package tag automatically. Create a tag manually only for recovery or while bootstrapping the automation.
- Use the tag format documented in `plugins/DEVELOPMENT.md`.
- Never reuse a PyPI version or package tag.
- Confirm the package name, package version, Git commit, PyPI project, and tag before approval.
