---
name: kitaru-release
description: Guide a Kitaru v2 release from branch and changelog review through the current GitHub release workflow, artifact verification, and recovery. Use only for release preparation or execution.
---

# Kitaru v2 release

The authoritative implementation is `.github/workflows/release.yml`. Read it before every release because publishing targets, validation gates, inputs, and recovery behavior can change.

This workflow publishes packages and images and advances protected branches. Never dispatch a non-dry-run release or approve a publishing environment without explicit user confirmation.

## Release boundary

A normal `workflow_dispatch` release checks out `develop`, regardless of the branch from which the workflow is dispatched. A v2 feature or integration branch is not releasable through the normal path until its intended history is present on `develop`. An existing `v<VERSION>` tag activates recovery mode and checks out that immutable tag.

If the requested v2 commit is not on the workflow's release source, stop and report the branch mismatch. Do not work around it by pushing directly to `develop` or `main`.

## 1. Refresh and establish the release range

```bash
git fetch origin develop main --tags --prune
git status --short
git log -1 --oneline origin/develop
git describe --tags --abbrev=0 origin/main
```

Use the last immutable release tag as the comparison base. Do not use a potentially force-updated `origin/main` commit as the release range boundary.

```bash
LAST_TAG=$(git describe --tags --abbrev=0 origin/main)
git log "$LAST_TAG"..origin/develop --oneline
git diff "$LAST_TAG"..origin/develop --stat
```

Stop if the working tree contains changes that are not part of release preparation, another release run is active, or `develop` does not contain the intended release work.

## 2. Review version and changelog

Read the current version from `pyproject.toml`; the workflow updates it during a dispatch release. Review `CHANGELOG.md` under `[Unreleased]` against the commits in the release range.

Use semantic versioning:

- patch: compatible fixes and maintenance
- minor: new backward-compatible public capability
- major: intentional breaking public contract

Check every issue or PR reference against the actual release range. Exclude changes that belong only to the sibling marketing repository or that do not affect the released Kitaru package, docs, images, or operational contract.

If the changelog needs editing, make that a focused pull request into the release source branch. Do not silently commit or push release-preparation edits.

## 3. Select relevant validation

Run the smallest complete set that matches the changed surfaces. The release workflow will rerun its authoritative gates against the release checkout and built artifact.

### General source checks

```bash
just check
just test
just build
```

### CLI changes

```bash
uv sync --frozen --extra cli --extra worker
just test tests/cli
just cli-artifact-smoke
```

### MCP changes

```bash
uv sync --frozen --extra mcp
just test tests/mcp
just mcp-schema-check
just build
just mcp-wheel-smoke
```

Treat any change to the MCP registry, schemas, descriptions, annotations, capability modes, or committed snapshots as public API and security review work. Read the current inventory from `tests/mcp/snapshots/metrics.json`; do not rely on copied tool counts.

### Server, database, task, or worker changes

```bash
docker compose up -d db
uv sync --frozen --extra server --extra worker --extra otel
just migration-check
just test tests/server tests/task tests/worker
```

### Docs changes

```bash
just docs-build
just docs-validate
```

The v2 checkout currently lacks `scripts/generate_sdk_docs.py`, so SDK-reference generation and docs CI are blocked until a reviewed v2 generator lands. Do not use the deleted v1 generator as release evidence.

### UI packaging or Docker changes

Read `FRONTEND-TESTING.md` and `docker/CLAUDE.md`. Use the current CI and release workflow steps as the validation recipe. Do not use `scripts/smoke-test.sh`, `just ui-smoke`, or `just release-smoke`; the script was removed from v2.

V2 has no tracked `tests/live/` provider suite or `live_llm` marker contract. Do not use inherited v1 adapters, provider-area flags, local ZenML flows, or remote-stack smoke as release evidence.

## 4. Review the workflow plan

Before dispatch, summarize:

- proposed version and release type
- last release tag and exact `origin/develop` SHA
- important user-facing changes
- validation run and results
- any skipped gate and the reason
- whether the stable UI tag should be automatic or explicitly pinned
- whether this is a dry run, normal release, or recovery of an existing tag

Ask for explicit confirmation before any push of release-preparation changes and again before a non-dry-run dispatch.

## 5. Dispatch

Dry run:

```bash
gh workflow run release.yml --ref develop \
  -f version=<VERSION> \
  -f dry-run=true
```

Normal release, only after explicit confirmation:

```bash
gh workflow run release.yml --ref develop \
  -f version=<VERSION> \
  -f dry-run=false
```

Add `-f kitaru-ui-tag=<TAG>` only when an exact stable UI release was explicitly selected. Otherwise let the workflow resolve its documented stable default.

Capture the run ID immediately and monitor the exact run rather than whichever release run happens to be latest:

```bash
gh run list --workflow=release.yml --limit 5
gh run view <RUN_ID>
gh run view <RUN_ID> --log-failed
```

The workflow validates source, CLI, MCP, migrations, package artifacts, the bundled UI wheel, and installed-wheel contracts before publishing. A real release may pause at its configured GitHub environment approval gate. Leave that approval to the user unless they explicitly ask you to perform it.

## 6. Verify published state

After success, verify the exact version across the surfaces the workflow publishes:

- Git tag and GitHub Release
- PyPI metadata and attached wheel/sdist assets
- public client and server container tags
- managed image result reported by the workflow
- Helm chart publication
- `release/<VERSION>`, `main`, and `develop` state described by the workflow summary
- SDK docs deployment when the `main` update triggers it

Use the workflow run and attempt as provenance. Do not infer success for one surface from another surface being available.

## Recovery

The workflow is designed to recover from an existing matching `v<VERSION>` tag. A recovery dispatch must reproduce that tagged commit and skips version/changelog/lockfile mutation.

Before retrying:

1. Read the failed step and determine which side effects already completed.
2. Confirm the existing tag and published artifacts match the workflow's recorded release SHA.
3. Fix the workflow on a reviewed branch when the workflow itself is defective; do not advance `develop` casually during a partial-release recovery.
4. Re-dispatch only after explaining the recovery path and receiving explicit approval.

Never delete or move a published release tag, overwrite a divergent release branch, or retry a partially published version as though no side effects occurred.
