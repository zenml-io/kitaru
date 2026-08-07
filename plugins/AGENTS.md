# Plugin Guidelines

Read `plugins/DEVELOPMENT.md` before you change package metadata, default definitions, pins, artifact tests, or plugin release workflows.

## Package boundaries

- Treat each directory under `plugins/packages/` as an independent Python distribution.
- Release one importer distribution when only that provider changes.
- Release `kitaru-evaluator` when any built-in evaluator changes.
- Do not bump unrelated plugin distributions.
- Keep the definitions in `src/kitaru/server/api/bootstrap.py` aligned with their owning wheels.

## Required updates

- Update the selected package version with `uv version --project plugins --package DISTRIBUTION VERSION --no-sync`.
- Update the same distribution pin in `plugins/default-requirements.txt`.
- Update the same requirement and display version in `DEFAULT_PLUGIN_DEFINITIONS`.
- Run plugin workspace commands with `--project plugins`; the root workspace contains only Kitaru.
- Commit the resulting `plugins/uv.lock` change.
- Keep `.github/workflows/ci.yml`, `.github/workflows/release-plugins.yml`, and `plugins/packages/` aligned when you add or remove a distribution.

## Required tests

- Run the focused plugin tests for changed behavior.
- Run `uv run --project plugins ruff format --config plugins/pyproject.toml --check plugins` and `uv run --project plugins ruff check --config plugins/pyproject.toml plugins`.
- Run `uv run --project plugins ty check --project plugins`.
- Run `uv run --project plugins pytest -q -c plugins/pyproject.toml plugins/tests tests/server/test_default_plugins.py`.
- Run `just plugin-artifact-smoke` for default definitions, package metadata, pins, or release-path changes.
- Use `plugins/docker-compose.candidate.yml` when a change needs a candidate-image rehearsal.

## Release safety

- Use the `Release plugin` workflow. Do not publish plugin distributions from a developer machine.
- Run a dry-run from the feature branch before merge.
- Publish only from a package tag whose commit is contained in `main`.
- Use the tag format documented in `plugins/DEVELOPMENT.md`.
- Never reuse a PyPI version or package tag.
- Confirm the package name, package version, Git commit, PyPI project, and tag before approval.
