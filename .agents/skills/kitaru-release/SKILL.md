---
name: kitaru-release
description: Discover dependencies and prepare or execute Kitaru core and plugin releases, including version proposals, Kitaru UI selection, release PRs, ordered tag commands, artifact verification, and recovery. Use when a user asks what a release depends on or wants to prepare, cut, publish, verify, or recover a Kitaru, Kitaru UI, or Kitaru plugin release.
---

# Release Kitaru

Use the current workflows as the source of truth:

- Core: `.github/workflows/release.yml`
- Plugins: `.github/workflows/release-plugins.yml`
- Python package inventory: `release/release-units.toml`
- TypeScript release set: `release/typescript.md` and `.github/workflows/release-typescript.yml`
- Frontend: `zenml-io/zenml-frontend-monorepo/.github/workflows/release-kitaru-ui.yml`
- Follow-up skills release: [skills-release](https://github.com/zenml-io/kitaru-skills/blob/develop/.claude/skills/skills-release/SKILL.md)

Read `AGENTS.md`. Read `plugins/AGENTS.md` and `plugins/DEVELOPMENT.md` for plugin changes. Read `FRONTEND-TESTING.md` for core or UI work.

Do not create a tag, dispatch a publishing workflow, approve an environment, or publish an artifact without explicit user confirmation. A request to prepare a release authorizes a release PR only.

## Select the release shape

Ask only for choices that cannot be derived from the repository and registries.

1. **Core:** Release `kitaru`, the selected UI, public images, the managed image, and Helm from one core tag.
2. **One plugin:** Release one selected Python plugin distribution. Do not release the UI or core.
3. **All plugins:** Inspect and prepare every plugin release unit independently.
4. **Coordinated:** Release core with its selected UI, then publish plugins that depend on that core version.

There is no separate bundle tag. The core tag publishes the Python package and deployables in one workflow.

Confirm the selected distributions, versions, frontend tag, publication order, and release commit before editing.

## Discover release impact

Fetch current remote state and load the release inventory:

```bash
git fetch origin --prune --tags
uv run --no-project --with packaging==26.2 \
  python scripts/release_units.py list --format json
```

For every selected unit, resolve its latest published tag. Compare that tag with `origin/develop` and inspect merged PRs in the range with `git log`, `git diff`, and `gh pr view`. Derive directly changed core and plugin units from `impact-paths` in `release/release-units.toml`. Read `requires:*` labels as release follow-up metadata, together with `Release context`, linked work, and existing changelog entries. A directly changed unit does not need a matching label when it will be published in the next applicable release. If its publication is intentionally deferred past that release, attach its exact `release-label` and record the intended timing in `Release context`.

For one plugin, start at that plugin's previous tag. For all plugins, calculate a separate range for every plugin release unit. `requires:plugins` means every unit is expected; report a unit with no implementation change as a red flag and require an explanation in the release PR.

For core, collect requirements for frontend, plugins, skills, ZenML docs, website, examples, and additional context from source PRs. Read linked repositories with `gh` or existing local checkouts. Do not write to them.

Compare declared follow-ups with the actual diff and PR context. Report unknown, conflicting, or stale signals. The diff identifies direct units; labels record deferred publication or other work that the Kitaru diff cannot show. Repository state decides what can be released.

For core, collect every merged PR label in the range and use the latest published stable core version as the input to the deterministic version rule:

```bash
uv run --no-project --with packaging==26.2 \
  python scripts/release_units.py propose-core-version \
  --latest-version <latest-stable-version> \
  --label <first-merged-pr-label> \
  --label <second-merged-pr-label> ...
```

Pass every label occurrence; do not summarize or discard labels before running the command. A `Breaking Change` label advances a pre-1.0 core to the next minor version and a post-1.0 core to the next major version. Without that label, the command advances the patch version. The version in `pyproject.toml` may contain `+dev`; it is a development placeholder and is never the input or release proposal. PR prose about an expected dependency floor does not override the command result.

Before editing, rerun the command with `--candidate <proposed-version>` and require it to pass. Do not prepare a different core version.

Propose explicit versions and changelog entries after discovery. Check PyPI versions and Git tags before proposing a version, and never reuse a published version. Get the user's acceptance before editing release metadata.

## Separate implementation from release preparation

Feature PRs leave existing plugin package versions and server default requirements/display versions unchanged. Put behavior changes in the package's `Unreleased` changelog section. The release-preparation PR selects package versions and updates all matching default-catalog metadata together. New packages require initial metadata, but becoming a server default remains an explicit release decision.

Follow the development dependency policy in `plugins/DEVELOPMENT.md`. Plugins compatible with published core keep their supported minimum. A plugin requiring unreleased core uses an exact dependency on the current root development version, such as `kitaru==0.25.0+dev`, and records the needed core change in `Release context`.

Before changing core to its selected release version, inspect every plugin's core dependency. Replace each resolved development pin with the selected compatible release floor, such as `kitaru>=0.26.0`, preserving extras and compatible upper bounds. Do not guess the future version in the feature PR. An unresolved development pin blocks release preparation; account for every affected plugin and its publication timing. A plugin-only release can replace the pin only when a published core contains the required change.

Regenerate `plugins/uv.lock` after these conversions. Verify the selected wheels' `Requires-Dist` metadata contains no development placeholder or local checkout dependency before handing over publication commands.

Inspect the TypeScript packages separately from the Python inventory. Include their lockstep release when their changes require publication. Inspect the Kitaru skills repository for a pending release and record its own version and compatibility requirements.

## Apply version rules

- Use canonical PEP 440 for Python: `X.Y.Z` or `X.Y.ZrcN`.
- The core workflow converts `X.Y.ZrcN` to `X.Y.Z-rc.N` for Docker and Helm.
- Use `vX.Y.Z` or `vX.Y.Z-rc.N` as the frontend workflow input.
- The frontend release tag is `kitaru-ui-vX.Y.Z` or `kitaru-ui-vX.Y.Z-rc.N`.
- Never reuse a published version or move a published tag.
- Increment the RC number for another candidate of the same target.
- Remove the RC suffix to accept a candidate without changing `X.Y.Z`.

For pre-1.0 plugins, use a patch when the public API and observable output stay compatible for existing supported inputs. Use a minor for new capabilities or breaking behavior. Importer output includes session grouping, IDs, node structure, field mappings, normalized values, and incomplete or failed trace handling.

## Release or select the frontend

Perform this section before the core preparation PR when the core needs a new UI release.

1. Inspect `zenml-io/zenml-frontend-monorepo` and select an exact commit on `main`.
2. Confirm its Kitaru UI checks pass.
3. Select a stable frontend version input, such as `v0.4.0`, for an official core release.
4. Ask for explicit confirmation before dispatch because the workflow creates a tag and GitHub Release.
5. Dispatch the workflow at the selected ref:

```bash
gh workflow run release-kitaru-ui.yml \
  --repo zenml-io/zenml-frontend-monorepo \
  --ref main \
  -f version=v0.4.0
```

6. Capture and monitor the exact run:

```bash
gh run list --repo zenml-io/zenml-frontend-monorepo \
  --workflow release-kitaru-ui.yml --limit 5
gh run view <run-id> --repo zenml-io/zenml-frontend-monorepo
```

7. Verify the release tag and both assets:

```bash
gh release view kitaru-ui-v0.4.0 \
  --repo zenml-io/zenml-frontend-monorepo
```

Require `kitaru-ui.tar.gz` and `kitaru-ui.tar.gz.sha256`. Verify the workflow ran at the reviewed commit and the release is stable. The frontend workflow derives its release channel from whether the selected commit is on `main`; official core releases require a stable bundle.

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

5. Resolve plugin development dependencies to the selected core version, then run `uv lock` and `uv lock --project plugins`. Keep unrelated `exclude-newer` timestamp churn out of the diff.
6. Run `uv run python scripts/generate_openapi.py` and commit `openapi/openapi.json`.
7. If a default plugin version changes, update every matching server catalog requirement and display version. The release inventory validates these values against the package manifest.

Read default membership from `release/release-units.toml`. Do not copy a fixed plugin count or a retired plugin name into the skill.

Check the standalone quickstart dependency floor and lockfile under `examples/python/pydantic_ai_ticket_resolver/`. Its README uses `uv sync --frozen`, so changing only the dependency floor does not update the installed version. Refresh both files to a published compatible core version, retaining unrelated dependency pins:

```bash
uv add --project examples/python/pydantic_ai_ticket_resolver --no-sync \
  --upgrade-package "kitaru==<published-version>" \
  "kitaru[cli,mcp,server,worker]>=<published-version>"
```

With the local test PostgreSQL available, run `uv sync --frozen` and `uv run --frozen python scripts/run_ci_e2e.py` from the example directory before installing candidate wheels. The published-dependency run and the candidate-wheel run verify different installation paths. Preserve the existing lockfile cutoff when no unrelated dependency update is needed.

Do not put an unpublished version or a local wheel path into the public quickstart lockfile. If the example requires the upcoming release, record its dependency refresh and frozen end-to-end check as a post-publication follow-up; candidate-wheel success alone does not complete it.

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

For a default-catalog plugin, update matching server requirements and display versions in this release-preparation PR. The current inventory validator requires them to match the selected package version. Feature PRs avoid that coupling by leaving both versions and default pins unchanged.

For a coordinated release, prepare core and plugin versions together. Tag core first. After the core `publish-python` job succeeds and the exact version is available on PyPI, tag dependent plugins. Core deployables and other jobs can continue while plugins publish. A default pin may reference a queued plugin version when the repository's pending-default behavior is available and verified. An independent plugin release uses an already-published compatible core.

## Patch an existing plugin release line

Use the unit's maintenance branch from `release/release-units.toml`, named `release/<plugin>/<major.minor>`. Confirm the branch exists and the requested version is the next unused patch version. The plugin workflow accepts a tag whose commit belongs to `develop` or that exact maintenance branch.

When the same bug exists on `develop`, merge the implementation there first. Then cherry-pick only its implementation commit onto a fix branch based on the maintenance line:

```bash
git fetch origin --prune --tags
git switch --track origin/release/langfuse/0.4
git switch -c fix/langfuse-0.4.1
git cherry-pick <implementation-commit>
```

When the bug only affects a superseded line, create the fix branch from that maintenance line and implement the fix there. Forward-port it only if the same bug exists on `develop`.

On the fix branch:

1. Bump only the selected plugin to the patch version.
2. Update its changelog and `plugins/uv.lock`.
3. For a default-catalog plugin, update matching bootstrap requirements and display versions only when that maintenance line uses the patched version.
4. Run the selected plugin's focused tests, release inventory validation, and artifact smoke.
5. Open the PR against the maintenance branch, not `develop`.

After the maintenance PR merges, resolve its exact merge commit and prepare the namespaced tag command. Do not push the tag without explicit authorization:

```bash
PATCH_SHA="$(gh pr view <patch-pr> \
  --repo zenml-io/kitaru \
  --json mergeCommit \
  --jq '.mergeCommit.oid')"
git tag -a python/kitaru-langfuse-importer/v0.4.1 "$PATCH_SHA" \
  -m python/kitaru-langfuse-importer/v0.4.1
git push origin python/kitaru-langfuse-importer/v0.4.1
```

Leave a newer package version and default-catalog declaration on `develop` unchanged when patching a superseded line.

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
- source PRs included for each release unit
- frontend tag and asset status for core
- dependency and default-pin decisions
- plugin coverage and explanations for missing units in an all-plugin release
- linked skills, ZenML docs, website, examples, and other follow-ups
- validations run
- exact post-merge tag and follow-up order
- `## Reviewer Notes` with a concrete review path

Stop after the PR unless the user explicitly asks to publish.

After the PR merges, resolve its exact merge commit and write tag commands against that SHA:

```bash
RELEASE_SHA="$(gh pr view <release-pr> \
  --repo zenml-io/kitaru \
  --json mergeCommit \
  --jq '.mergeCommit.oid')"
```

Emit the complete runnable command handoff in the PR and final response, using [publication-handoff.md](references/publication-handoff.md). Group commands by repository and source branch. Include every selected Python plugin, the TypeScript release set when selected, any new frontend release, manual `main` promotion, and the Kitaru skills release handoff. Use accepted versions and exact reviewed commits; do not leave a selected plugin as “repeat for the other plugins.” Do not execute publication commands during preparation.

Push every release tag in its own `git push origin <tag>` command. GitHub does not create push events when more than three tags are pushed at once, which leaves immutable tags without release workflow runs. Do not batch release tags into one push, even when several point to the same commit. After each push, confirm the matching workflow run exists before pushing the next tag.

## Rehearse before publication

Manual dispatch builds and validates without publishing.

Core:

```bash
gh workflow run release.yml --repo zenml-io/kitaru --ref <reviewed-branch-or-tag> \
  -f package-tag=python/kitaru/v<python-version>
```

Plugin:

```bash
gh workflow run release-plugins.yml --repo zenml-io/kitaru --ref <reviewed-branch-or-tag> \
  -f package-tag=python/<distribution>/v<python-version>
```

Use an existing branch or tag that resolves to the reviewed eligible commit. Confirm the run's `headSha` matches that commit, inspect its artifacts, and confirm no publishing job ran.

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

For a stable release, the workflow creates or fast-forwards the unit's maintenance branch after the GitHub Release exists. A maintenance-line patch uses the exact reviewed maintenance-branch commit prepared above instead of a `develop` commit.

## Publish core and deployables

Before tagging, verify the selected stable frontend bundle exists and release metadata is complete. Coordinated plugin versions can remain queued when pending-default behavior has been verified.

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
8. creates or fast-forwards the stable maintenance branch
9. creates a draft post-release PR that restores `## [Unreleased]`, sets core to `<version>+dev`, and updates both lockfiles and the generated OpenAPI version

Dependent plugin tags can be pushed after step 3 succeeds and the exact core package is available on PyPI. They do not wait for the remaining jobs.

For a stable release, fast-forward `main` to the tagged release commit before merging the generated development-reset PR. The reset PR must leave `main` at the clean release version and change only `pyproject.toml`, `uv.lock`, `plugins/uv.lock`, `openapi/openapi.json`, and `CHANGELOG.md` on `develop`.

Approve required environments only after checking the candidate evidence. A managed-image failure is reported as a warning and does not block public deployables.

Verify each published surface independently. Report core PyPI availability, public artifacts, managed-image status, installer smoke, maintenance-branch state, and development-reset status separately. A reset-job failure can make the workflow red after successful artifact publication. Diagnose and report that follow-up failure without describing the already-published artifacts as unpublished.

After the required core and plugin versions are available on PyPI, complete any pending quickstart dependency refresh through a reviewed follow-up PR and rerun its frozen end-to-end test. Report which branch contains that update: the public README links to `main`, and merging a follow-up into `develop` alone does not update the public example. The development-reset PR does not currently refresh the quickstart lockfile.

The workflow does not update `main`. After the newest stable core's public artifacts and GitHub Release succeed, inspect any remaining workflow failure and tell the release owner to fast-forward `main` to the immutable core tag:

```bash
git fetch origin main --tags
TAG="python/kitaru/v<python-version>"
RELEASE_SHA="$(git rev-list -n 1 "$TAG")"
git merge-base --is-ancestor origin/main "$RELEASE_SHA"
gh api --method PATCH repos/zenml-io/kitaru/git/refs/heads/main \
  -f sha="$RELEASE_SHA" -F force=false
```

The release owner runs this command manually. Do not execute it on their behalf. The fast-forward updates `main` to the tagged commit without a merge commit and triggers the existing docs workflow. Skip this step for prereleases and older maintenance-line core releases.

After core and required plugins are available, hand off pending `zenml-io/kitaru-skills` changes to its [skills-release skill](https://github.com/zenml-io/kitaru-skills/blob/develop/.claude/skills/skills-release/SKILL.md). Read the current skill before emitting that repository's commands. It owns the independent skills version, `develop` release commit, tag, `main` promotion, and GitHub Release. Core publication does not itself authorize publishing skills.

## Curate GitHub Release Notes

After every selected release workflow has succeeded and its published artifacts have been verified, curate the GitHub Release body for every release created in this release flow. This is a post-publication metadata step, not a replacement for the publication workflows: their autogenerated notes remain the fallback until curation is complete.

Tags, versions, uploaded assets, registry artifacts, release targets, and prerelease/latest state are immutable. A release body may be deliberately updated after publication only with explicit user confirmation. Do not curate releases that are outside the selected release set, and do not rewrite an older UI release merely because a core release references it.

Build the exact release set before drafting. It can include:

- the core tag `python/kitaru/v<version>`;
- each selected Python plugin tag from `release/release-units.toml`;
- the TypeScript release-set tag `typescript/kitaru/v<version>`, which covers `@zenml-io/kitaru`, `@zenml-io/kitaru-mastra`, and `@zenml-io/kitaru-vercel-ai` together;
- a new `kitaru-ui-v<version>` release in `zenml-io/zenml-frontend-monorepo` only when this release flow published that UI release.

For every selected release, first capture its current state with `gh release view <tag> --repo <repo> --json name,tagName,targetCommitish,isPrerelease,assets,body` and record its latest state separately with `gh release list --repo <repo> --json tagName,isLatest`. Resolve the previous tag from the same tag family, never from GitHub's repository-wide previous-release selection. The comparison families are `python/kitaru/v*` for core, the selected unit's `tag-prefix` for a Python plugin, `typescript/kitaru/v*` for TypeScript, and `kitaru-ui-v*` for the UI. A stable release compares with the previous stable tag in its family, never its last release candidate. A prerelease compares with the nearest preceding prerelease in its release line, falling back to the latest stable tag in its family. For a maintenance-line plugin patch, choose a predecessor from the same `<major>.<minor>` release line whose tag is an ancestor of the current tag, and verify that ancestry with `git merge-base --is-ancestor <previous-tag> <tag>`. If no eligible predecessor exists, mark it as the first release in that comparison family rather than inventing one. This avoids cross-package and cross-maintenance-line comparison links.

Use reviewed release evidence, not PR titles alone:

- Core: the released `CHANGELOG.md`, merged PR context, and the core-tag-to-core-tag diff.
- Python plugin: that distribution's changelog, its `impact-paths`, and the package-tag-to-package-tag diff.
- TypeScript: `release/typescript.md`, the three package manifests and changelog or PR context, distinguishing client, Mastra, and Vercel AI changes when they differ.
- UI: the path-scoped source and existing note-generation contract in `zenml-frontend-monorepo`, plus the UI-tag-to-UI-tag diff.

Replace the autogenerated PR list with a proportionate reader-facing body. Start with `## Highlights`: one to three short paragraphs explaining observable effects relative to the previous release, or introduce the initial capability for a first release. A patch release can say that it is focused maintenance; a minor release should foreground the most consequential capability. Follow with nonempty `## Added`, `## Changed`, `## Fixed`, or `## Infrastructure` sections when they make the release clearer. For TypeScript, identify which of the three published packages each meaningful item affects. Use code formatting for identifiers, include the same-family `Full Changelog` comparison link only when an eligible predecessor exists, and omit empty sections.

Do not include site-only work, Dependabot-only bumps, internal refactors without an observable effect, reverted or no-op change pairs, unverified claims, or private operational details. Do not use `--generate-notes` for the curated body. Keep each paragraph and list item on one physical line, and do not use em dashes or en dashes.

Show every complete draft and its tag-to-previous-tag mapping to the user before editing GitHub. If four or more independent releases need curation, the release coordinator may delegate evidence gathering and drafts in small batches. The coordinator owns the exact release set, checks every draft against its source evidence, collects one approval, and performs all GitHub edits and verification. Delegates do not edit GitHub Releases.

Only after explicit approval, write each approved body to a temporary file and update only the body:

```bash
gh release edit <tag> --repo <repo> --notes-file <notes-file>
```

Do not pass title, asset, target, prerelease, or latest flags. Re-fetch each release with the same JSON fields, record its latest state again with `gh release list --repo <repo> --json tagName,isLatest`, and verify that its tag, target, assets, prerelease/latest state are unchanged and its body exactly matches the approved draft. Report every curated release and any release that remains pending or was intentionally excluded.

## Recover a failed release

Do not delete or move a release tag. Do not reuse a version for different bytes.

1. Inspect the failed step and completed external writes.
2. Confirm existing artifacts match the immutable tag.
3. Determine whether retrying the original job can succeed. A workflow fix on a newer branch does not change the tagged source.
4. Ask for explicit confirmation before retrying publication.
5. Rerun the failed jobs of the original tag-triggered run to preserve its event and immutable artifacts:

```bash
gh run rerun <original-run-id> --repo zenml-io/kitaru --failed
```

Manual dispatch is a non-publishing rehearsal and cannot finish a failed publication. If recovery needs different package bytes, prepare a new version through a reviewed PR.

Keep credentials, private endpoints, account IDs, and internal infrastructure names out of committed content and PR descriptions.
