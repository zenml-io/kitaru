# Publication command handoff

Include minimal, copy-pasteable `git` commands in both the release PR and the final user-facing response. Name the repository and prerequisite above each block. Fill in the selected branch and every tag; never leave selected plugins as “repeat for the others.”

The publication block should only check out the source branch, tag the reviewed commit, and push that tag. Do discovery and safety checks before emitting it; do not include `gh`, API calls, shell variables, or validation boilerplate. These are commands for the release owner, not permission to change their checkout or publish.

## Ordered release steps

1. **Frontend, when a new bundle is needed.** In `zenml-io/zenml-frontend-monorepo`, verify the reviewed `main` commit and emit checkout, `kitaru-ui-v<version>` tag, and push commands. The tag push starts `release-kitaru-ui.yml`. Verify the resulting run and assets before tagging core. An existing selected bundle needs verification only.
2. **Core.** In `zenml-io/kitaru`, verify the release PR's merge commit on `develop` (or the exact core maintenance branch). Emit the core tag and push commands.
3. **Core PyPI gate.** Identify the exact core run. Wait for its `publish-python` job to succeed and verify `https://pypi.org/pypi/kitaru/<version>/json`. Dependent plugin publication can then proceed while images, Helm, and other core jobs continue.
4. **Every selected Python plugin.** Emit each full namespaced tag and its individual push command at the reviewed release commit. Independent plugin releases use an already-published compatible core and need no new core tag.
5. **TypeScript, when selected.** Read `release/typescript.md`. Complete the required rehearsal separately, then emit one `typescript/kitaru/v<version>` tag for the entire lockstep set on the reviewed `develop` commit. Identify any core API availability prerequisite. Do not create separate tags for the three npm packages.
6. **Stable core completion.** Verify the core's public artifacts and GitHub Release. Emit the manual, non-forced `main` fast-forward to the immutable core tag. Report maintenance/reset failures separately and resolve outstanding release failures before calling the release complete.
7. **Development reset and quickstart.** Link the generated reset PR and state that `main` must contain the stable tag before it merges. Identify the reviewed quickstart dependency/lockfile follow-up and its frozen E2E check. Report whether it has reached the public example on `main`.
8. **Kitaru skills.** After the required core/plugin versions are published, link and read the current [skills-release skill](https://github.com/zenml-io/kitaru-skills/blob/develop/.claude/skills/skills-release/SKILL.md). Provide minimal git tag commands only after the independent skills version and reviewed release commit are selected; link the skill for its other release steps. Its source is `develop`; its distribution branch is `main`. Do not reuse the core version or SHA. If its version or preparation is unresolved, give the exact handoff and missing choice instead of inventing a tag.
9. **Other follow-ups.** Link selected docs, website, examples, and release-note work with their remaining action.

## Tag command block

Run in the named repository. Use this structure with concrete values:

```bash
git checkout develop
git tag <exact-tag> HEAD
git push origin <exact-tag>
```

Use `main` for the frontend and the matching maintenance branch for a maintenance release. Prefer `HEAD` when the checked-out branch points to the reviewed release commit. Verify that condition first. If the local branch is behind, include `git pull --ff-only origin <source-branch>` after checkout only when it brings the branch to that commit. If the branch has advanced, replace `HEAD` with the literal reviewed SHA. If the release PR is still open, state that it must merge and its release commit must be verified before these commands run; never tag the preparation branch's pre-merge HEAD.

For multiple tags in one repository and branch, check out the branch once, then emit one tag/push pair for each selected release. Separate dependent plugin pairs from the core pair with the PyPI gate.

A maintenance release uses its matching `release/<unit>/<major.minor>` branch and maintenance PR. Check that the tag is unused before offering its creation command. For an existing tag, give its status and the recovery handoff; do not recreate or move it.

Push one tag per command. Confirm the matching workflow run exists after each push. For plugins in the same release set, subsequent tag pushes need not wait for the preceding plugin workflow to finish.

## Core publication status

Separate the two decision points:

- **Dependent plugins may publish:** the core PyPI job succeeded and the exact core version is available.
- **Stable core may be promoted to main:** required public publication jobs and GitHub Release succeeded; inspect remaining workflow failures explicitly.

The managed-image step is warning-only. Installer smoke can fail without preventing artifact publication. Development-reset failure also occurs after publication; it needs repair but does not undo existing artifacts. After verifying that local and remote `main` can fast-forward to the immutable stable core tag, emit:

```bash
git checkout main
git merge --ff-only <exact-core-tag>
git push origin main
```

Use the literal core tag. Do not force-push or reset away unrelated work.
