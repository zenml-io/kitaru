---
name: kitaru-release
description: Prepare or execute Kitaru core and plugin releases, including Kitaru UI publication in the frontend monorepo, version and changelog updates, frontend declarations, default plugin pins, validation, release tags, artifact verification, and recovery. Use when a user asks to prepare, cut, publish, verify, or recover a Kitaru, Kitaru UI, or Kitaru plugin release.
---

# Release Kitaru

Use the current workflows as the source of truth:

- Core: `.github/workflows/release.yml`
- Plugins: `.github/workflows/release-plugins.yml`
- Package inventory: `release/release-units.toml`
- Frontend: `zenml-io/zenml-frontend-monorepo/.github/workflows/release-kitaru-ui.yml`

Read `AGENTS.md`. Read `plugins/AGENTS.md` and `plugins/DEVELOPMENT.md` for plugin changes. Read `FRONTEND-TESTING.md` for core or UI work.

Do not create a tag, dispatch a publishing workflow, approve an environment, or publish an artifact without explicit user confirmation. A request to prepare a release authorizes a release PR only.

## Select the release shape

Ask only for choices that cannot be derived from the repository and registries.

1. **Core:** Release `kitaru`, the selected UI, public images, the managed image, and Helm from one core tag.
2. **Plugin only:** Release one selected Python plugin distribution. Do not release the UI or core.
3. **Coordinated:** Release changed plugins first. Then prepare and release core with the new exact default pins.

There is no separate bundle tag. The core tag publishes the Python package and deployables in one workflow.

Confirm the selected distributions, versions, frontend tag, publication order, and release commit before editing.

## Apply version rules

- Use canonical PEP 440 for Python: `X.Y.Z` or `X.Y.ZrcN`.
- The core workflow converts `X.Y.ZrcN` to `X.Y.Z-rc.N` for Docker and Helm.
- Use `vX.Y.Z` or `vX.Y.Z-rc.N` as the frontend workflow input.
- The frontend release tag is `kitaru-ui-vX.Y.Z` or `kitaru-ui-vX.Y.Z-rc.N`.
- Never reuse a published version or move a published tag.
- Increment the RC number for another candidate of the same target.
- Remove the RC suffix to accept a candidate without changing `X.Y.Z`.

## Release or select the frontend

Perform this section before the core preparation PR when the core needs a new UI release.

1. Inspect `zenml-io/zenml-frontend-monorepo` and select an exact commit on `main`.
2. Confirm its Kitaru UI checks pass.
3. Select the frontend version input, such as `v0.2.0-rc.4`.
4. Ask for explicit confirmation before dispatch because the workflow creates a tag and GitHub Release.
5. Dispatch the workflow at the selected ref:

```bash
gh workflow run release-kitaru-ui.yml \
  --repo zenml-io/zenml-frontend-monorepo \
  --ref main \
  -f version=v0.2.0-rc.4
```

6. Capture and monitor the exact run:

```bash
gh run list --repo zenml-io/zenml-frontend-monorepo \
  --workflow release-kitaru-ui.yml --limit 5
gh run view <run-id> --repo zenml-io/zenml-frontend-monorepo
```

7. Verify the release tag and both assets:

```bash
gh release view kitaru-ui-v0.2.0-rc.4 \
  --repo zenml-io/zenml-frontend-monorepo
```

Require `kitaru-ui.tar.gz` and `kitaru-ui.tar.gz.sha256`. The frontend workflow builds with the selected version, creates both files, and publishes a prerelease.

If the user asks only for a preparation PR, do not dispatch the frontend workflow. The user can explicitly select an expected frontend tag before it exists. Mark asset verification as pending and do not create the core tag until both assets exist.

## Prepare a core release PR

Create a branch from current `origin/develop`. Preserve unrelated work in the active checkout.

1. Set `[project].version` in `pyproject.toml`.
2. Convert the current top `[Unreleased]` changelog section to the selected version and preserve all entries in the release range.
3. Add a concise release-specific changelog entry when needed.
4. Create `releases/python/kitaru/<version>.toml`:

```toml
schema-version = 1
kitaru-version = "<python-version>"
ui-tag = "<kitaru-ui-tag>"
```

5. Run `uv lock`. Keep unrelated `exclude-newer` timestamp churn out of the diff.
6. Run `uv run python scripts/generate_openapi.py` and commit `openapi/openapi.json`.
7. If a default plugin pin changes, update `plugins/default-requirements.txt` and every matching server catalog entry.

Read default membership from `release/release-units.toml`. Do not copy a fixed plugin count or a retired plugin name into the skill.

The frontend declaration contains only the schema version, Kitaru version, and trusted frontend tag. The workflow downloads and verifies the published checksum.

## Prepare a plugin-only release PR

Select the distribution from `release/release-units.toml`.

1. Update only the selected package version:

```bash
uv version --project plugins --package <distribution> <version> --no-sync
```

2. Update its Kitaru compatibility range when required.
3. Update its package changelog.
4. Run `uv lock --project plugins`.
5. Leave every unselected package unchanged.

If the plugin becomes a new core default, publish the plugin before the core release. Then prepare a new core version with its exact pin.

## Validate the preparation

Always run:

```bash
git diff --check
just check
uv run --no-project --with packaging==26.2 python scripts/release_units.py validate
```

For core metadata and UI selection, run:

```bash
uv run python scripts/release_ui.py --version <core-version>
uv run pytest -q tests/scripts/test_release_ui.py tests/scripts/test_release_units.py
```

For plugin metadata, dependencies, or default pins, run:

```bash
uv run --project plugins ruff format --config plugins/pyproject.toml --check plugins
uv run --project plugins ruff check --config plugins/pyproject.toml plugins
uv run --project plugins ty check --project plugins
uv run --project plugins pytest -q -c plugins/pyproject.toml plugins/tests tests/server/test_default_plugins.py
just plugin-artifact-smoke
```

Review the final version, frontend tag, dependency ranges, exact default pins, changelog, file list, and validation results.

## Open the preparation PR

Commit only the release files. Push the branch and open a draft PR to `develop`.

Include:

- release shape and versions
- frontend tag and asset status for core
- dependency and default-pin decisions
- validations run
- publication order
- `## Reviewer Notes` with a concrete review path

Stop after the PR unless the user explicitly asks to publish.

## Rehearse before publication

Manual dispatch builds and validates without publishing.

Core:

```bash
gh workflow run release.yml --ref <release-sha-or-tag> \
  -f package-tag=python/kitaru/v<python-version>
```

Plugin:

```bash
gh workflow run release-plugins.yml --ref <release-sha-or-tag> \
  -f package-tag=python/<distribution>/v<python-version>
```

Inspect the exact run and its artifacts. Confirm that no publishing job ran.

## Publish a plugin

After the preparation PR merges, select its exact reachable `develop` commit.

```bash
git fetch origin develop --tags
TAG="python/<distribution>/v<python-version>"
RELEASE_SHA="<reviewed-develop-sha>"
git merge-base --is-ancestor "$RELEASE_SHA" origin/develop
git tag -a "$TAG" "$RELEASE_SHA" -m "$TAG"
git push origin "$TAG"
```

Approve the package's PyPI environment when required. Verify the wheel, source distribution, hashes, and immutable GitHub Release.

## Publish core and deployables

Before tagging, verify the selected frontend release and required plugin versions exist.

```bash
git fetch origin develop --tags
TAG="python/kitaru/v<python-version>"
RELEASE_SHA="<reviewed-develop-sha>"
git merge-base --is-ancestor "$RELEASE_SHA" origin/develop
git tag -a "$TAG" "$RELEASE_SHA" -m "$TAG"
git push origin "$TAG"
```

The tag starts `.github/workflows/release.yml`. The workflow:

1. downloads and verifies the selected frontend
2. builds and tests the wheel
3. publishes Kitaru to PyPI
4. builds and publishes client, worker, server, and managed images
5. publishes the Helm chart
6. moves public Docker `latest` aliases only for a stable release
7. creates the immutable GitHub Release

Approve required environments only after checking the candidate evidence. A managed-image failure is reported as a warning and does not block public deployables.

Verify each published surface independently. Do not infer one surface from another.

## Recover a failed release

Do not delete or move a release tag. Do not reuse a version for different bytes.

1. Inspect the failed step and completed external writes.
2. Confirm existing artifacts match the immutable tag.
3. Fix workflow defects through a reviewed PR.
4. Ask for explicit confirmation before retrying.
5. Dispatch the matching workflow at the immutable tag:

```bash
gh workflow run release.yml --ref <core-tag> -f package-tag=<core-tag>
gh workflow run release-plugins.yml --ref <plugin-tag> -f package-tag=<plugin-tag>
```

Keep credentials, private endpoints, account IDs, and internal infrastructure names out of committed content and PR descriptions.
