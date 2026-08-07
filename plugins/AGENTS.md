# Plugin Guidelines

Read `plugins/DEVELOPMENT.md` before you change package metadata, catalogs, default pins, artifact tests, or plugin release workflows.

## Package boundaries

- Treat each directory under `plugins/packages/` as an independent Python distribution.
- Release one importer distribution when only that provider changes.
- Release `kitaru-evaluator` when any built-in evaluator changes.
- Do not bump unrelated plugin distributions.
- Keep each `kitaru.default_plugins` catalog aligned with its owning wheel.

## Required updates

- Update the selected package version with `uv version --package DISTRIBUTION VERSION --no-sync`.
- Update the same distribution pin in `plugins/default-requirements.txt`.
- Commit the resulting `uv.lock` change.
- Keep `.github/workflows/ci.yml`, `.github/workflows/release-plugins.yml`, and `plugins/packages/` aligned when you add or remove a distribution.

## Required tests

- Run the focused plugin tests for changed behavior.
- Run `uv run pytest -q plugins/tests tests/server/test_default_plugins.py`.
- Run `just plugin-artifact-smoke` for catalog, package metadata, pin, or release-path changes.
- Use `plugins/docker-compose.candidate.yml` when a change needs a release-image rehearsal.

## Release safety

- Use the `Release plugin` workflow. Do not publish plugin distributions from a developer machine.
- Run a dry-run from the feature branch before merge.
- Publish only from a package tag whose commit is contained in `main`.
- Use the tag format documented in `plugins/DEVELOPMENT.md`.
- Never reuse a PyPI version or package tag.
- Confirm the package name, package version, Git commit, PyPI project, and tag before approval.
