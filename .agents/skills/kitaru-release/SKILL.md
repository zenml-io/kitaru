---
name: kitaru-release
description: Prepare Kitaru core, plugin-only, or coordinated release-candidate and stable releases through an interactive interview, edit the required version, changelog, frontend declaration, default plugin, catalog, and lock files, validate the result, and open a draft pull request to develop. Use when a user asks to prepare, cut, stage, or make a Kitaru or Kitaru plugin release, choose package versions, create release metadata, or open a release preparation PR. This skill prepares the PR only and does not create release tags or publish artifacts.
---

# Prepare a Kitaru release PR

Prepare one reviewed release commit through an interview. Stop after opening a draft pull request to `develop`.

Never create or push a release tag, publish to PyPI, push an image or chart, move `latest`, or update `main` while preparing the PR. Perform those operations only after a later explicit user request.

## Read repository instructions

Read these files before editing:

- `AGENTS.md`
- `plugins/AGENTS.md` and `plugins/DEVELOPMENT.md` when any plugin or default pin changes
- `FRONTEND-TESTING.md` when core embeds a frontend release
- `.github/workflows/release-plugins.yml` and `.github/workflows/release.yml` when their current behavior affects the plan
- `release/release-units.toml` for the authoritative package inventory, tag prefixes, default membership, and required checks

Use the current repository files as the source of truth. If this skill conflicts with the workflows or inventory, report the conflict and stop before editing.

## Interview the release owner

Ask short questions in small groups. Do not infer a choice that changes which packages are released.

### 1. Select the release shape

Ask which shape applies:

1. Core only: publish `kitaru`, with no plugin version changes.
2. Plugin only: publish one or more selected plugin distributions.
3. Coordinated: prepare core, selected plugins, exact default pins, and a later deployment bundle.

Explain that a changed default plugin pin requires a new core version. A plugin that needs unpublished core behavior requires core to be released first.

### 2. Select stability and versions

Ask whether this is an RC or stable release and propose versions from the manifests and public registry state.

Apply these rules:

- Use PEP 440 for Python packages: `X.Y.ZrcN`.
- Use SemVer for bundles: `X.Y.Z-rc.N`.
- Never reuse a version already present on PyPI.
- For a package's first public release, recommend `0.1.0rc0`.
- For another candidate of the same target, increment only the RC number.
- For an accepted RC, remove the RC suffix without changing `X.Y.Z`.
- For a backward-compatible fix, increment patch.
- For a backward-compatible feature, increment minor.
- For a breaking 0.x change, increment minor.

Confirm every selected distribution and proposed version in a table before editing.

### 3. Select core dependencies

For core or coordinated releases, ask:

- Which `kitaru-ui-v*` frontend tag should be embedded?
- Which six default plugin versions should core adopt?
- What user-facing changes belong in the core changelog entry?

Require the frontend release to contain `kitaru-ui.tar.gz` and `kitaru-ui.tar.gz.sha256`. Record only the trusted frontend tag in Kitaru. Do not ask the user to copy a checksum.

For each selected plugin, ask for the changelog summary and confirm the Kitaru compatibility range. A 0.22 RC plugin normally uses:

```toml
dependencies = ["kitaru>=0.22.0rc0,<0.23"]
```

Adjust the lower bound to the first public core version that provides the required API.

### 4. Confirm the edit plan

Show:

- release shape;
- core, plugin, and optional bundle versions;
- frontend tag;
- default plugin pins;
- files to change;
- validations to run;
- branch and PR title.

Wait for confirmation before writing files.

## Prepare the branch

Inspect the worktree first. Preserve unrelated changes and ask for direction if they overlap the release files.

Fetch `develop`, then create a feature branch from `origin/develop`. Prefer:

```text
codex/prepare-kitaru-<core-version>
codex/prepare-<plugin-slug>-<plugin-version>
```

Do not work directly on `develop` or `main`.

## Edit a core release

For a selected core version:

1. Set `[project].version` in `pyproject.toml`.
2. Add a concise versioned entry to `CHANGELOG.md` describing user-visible changes.
3. Create `releases/python/kitaru/<version>.toml`:

```toml
schema-version = 1
kitaru-version = "<python-version>"
ui-tag = "<kitaru-ui-v-tag>"
```

4. Run `uv lock`.

Do not add repository, archive, checksum, or prerelease fields to the frontend declaration. The repository and archive names are fixed, the workflow downloads the published checksum, and prerelease status is inferred from the tag.

## Edit selected plugin releases

Use the distribution identities from `release/release-units.toml`.

For each selected plugin:

1. Update its version with:

```bash
uv version --project plugins --package <distribution> <version> --no-sync
```

2. Set its `kitaru` dependency range in `plugins/packages/<slug>/pyproject.toml`.
3. Add the version entry to `plugins/packages/<slug>/CHANGELOG.md`. Create that file only when the package does not have one.
4. Leave every unselected plugin version unchanged unless this is the explicitly confirmed first coordinated publication of all plugins.

After all selected plugin edits, run:

```bash
uv lock --project plugins
```

## Align default plugin pins

Only these default distributions belong in `plugins/default-requirements.txt` and `DEFAULT_PLUGIN_DEFINITIONS`:

- `kitaru-braintrust-importer`
- `kitaru-evaluator`
- `kitaru-jsonl-importer`
- `kitaru-langfuse-importer`
- `kitaru-langsmith-importer`
- `kitaru-opentelemetry-importer`

When adopting a new default version:

1. Set its exact requirement in `plugins/default-requirements.txt`.
2. Update every matching `requirement` in `src/kitaru/server/api/bootstrap.py`.
3. Update every matching `display_version`.
4. Keep adapter packages out of the default list and server catalog.

The default file and server catalog must match exactly.

## Validate the preparation

Run the checks that match the edited scope. Do not require a separate no-write release rehearsal by default. The tag workflows build and test again before publishing.

Always run:

```bash
git diff --check
just check
uv run --no-project --with packaging==26.2 python scripts/release_units.py validate
```

For a core frontend declaration, run:

```bash
uv run --no-project python scripts/release_ui.py --version <core-version>
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

If a check fails, fix it and rerun the affected checks. Do not open the PR with an unexplained release-preparation failure.

## Review the diff

Before committing, show the release owner:

- selected package versions;
- dependency ranges;
- exact default pins;
- frontend tag;
- changelog entries;
- changed file list;
- validation results.

Ask for confirmation if the final diff differs materially from the approved plan.

## Commit and open the PR

Stage only release-preparation files. Use a concise commit and PR title, such as:

```text
Prepare Kitaru 0.22.0rc0
Prepare kitaru-langfuse-importer 0.2.0rc1
```

Push the branch and open a draft PR to `develop`. Include:

- release shape and versions;
- dependency and default-pin decisions;
- frontend tag for a core release;
- validations run;
- publication order for coordinated releases;
- a `## Reviewer Notes` section with exact files and commands to inspect.

Request the reviewer named by the user. If the user names no reviewer, leave reviewers unset.

Return the PR URL and state that no release tag was created and nothing was published.

## Publication order to record

For a coordinated release, put this order in the PR description:

1. Merge the preparation PR to `develop`.
2. Create the core Python tag and approve PyPI publication.
3. Confirm the core version exists on PyPI.
4. Create one tag per selected plugin and approve each PyPI publication.
5. Confirm required exact default versions exist on PyPI.
6. Create the bundle tag.

RC bundle publication must not move Docker `latest` or `main`.

## Recovery rules

- Never move or reuse a published package or bundle tag.
- Never reuse a version after different bytes were built.
- If a tag run fails before any external write, fix the source and use the next RC version unless the team explicitly decides the unused tag may remain only as failed history.
- If a run fails after an external write, inspect the remote artifacts. Retry only when the workflow safely reconciles matching identities.
- Keep credentials, private endpoints, account IDs, and internal infrastructure names out of committed skill content and PR descriptions.
