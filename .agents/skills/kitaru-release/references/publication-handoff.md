# Publication command handoff

Include the complete command set in both the release PR and the final user-facing response. Fill in all known repository paths, versions, PR numbers, and tag names. Resolve merge SHAs at execution time when the PR is still open; never tag the preparation branch's pre-merge HEAD.

For each block, state the repository, source branch, target tag, and prerequisite. Use `git -C <verified-checkout>` or an explicit `cd` so the commands cannot silently target another repository. Use `--repo` on every `gh` command. Preserve active checkouts; tag an explicit SHA without checking out a different branch.

## Ordered release steps

1. **Frontend, when a new bundle is needed.** In `zenml-io/zenml-frontend-monorepo`, verify the reviewed `main` commit and emit the actual `release-kitaru-ui.yml` dispatch command with the selected version. The workflow creates the UI tag; do not also emit a manual tag push. Include run and asset verification. An existing selected bundle needs verification only.
2. **Core.** In `zenml-io/kitaru`, resolve the release PR's merge SHA on `develop` (or the exact core maintenance branch). Emit the core tag and push commands.
3. **Core PyPI gate.** Identify the exact core run. Wait for its `publish-python` job to succeed and verify `https://pypi.org/pypi/kitaru/<version>/json`. Dependent plugin publication can then proceed while images, Helm, and other core jobs continue.
4. **Every selected Python plugin.** Emit each full namespaced tag and its individual push command at the reviewed release SHA. Independent plugin releases use an already-published compatible core and need no new core tag.
5. **TypeScript, when selected.** Read `release/typescript.md`. Emit the rehearsal and one `typescript/kitaru/v<version>` tag for the entire lockstep set on the exact reviewed `develop` commit. Identify any core API availability prerequisite. Do not create separate tags for the three npm packages.
6. **Stable core completion.** Verify the core's public artifacts and GitHub Release. Emit the manual, non-forced `main` fast-forward to the immutable core tag. Report maintenance/reset failures separately and resolve outstanding release failures before calling the release complete.
7. **Development reset and quickstart.** Link the generated reset PR and state that `main` must contain the stable tag before it merges. Identify the reviewed quickstart dependency/lockfile follow-up and its frozen E2E check. Report whether it has reached the public example on `main`.
8. **Kitaru skills.** After the required core/plugin versions are published, link and read the current [skills-release skill](https://github.com/zenml-io/kitaru-skills/blob/develop/.claude/skills/skills-release/SKILL.md). Emit its commands only after the independent skills version and reviewed release commit are selected. Its source is `develop`; its distribution branch is `main`. Do not reuse the core version or SHA. If its version or preparation is unresolved, give the exact handoff and missing choice instead of inventing a tag.
9. **Other follow-ups.** Link selected docs, website, examples, and release-note work with their remaining action.

## Python tag block

Use this structure with concrete values for every selected tag:

```bash
git -C <kitaru-checkout> fetch origin <source-branch> --tags
RELEASE_SHA="$(gh pr view <release-pr> --repo zenml-io/kitaru --json mergeCommit --jq '.mergeCommit.oid')"
test -n "$RELEASE_SHA" && test "$RELEASE_SHA" != null
git -C <kitaru-checkout> merge-base --is-ancestor "$RELEASE_SHA" origin/<source-branch>
git -C <kitaru-checkout> tag -a <exact-tag> "$RELEASE_SHA" -m <exact-tag>
git -C <kitaru-checkout> push origin <exact-tag>
```

A maintenance release uses its matching `release/<unit>/<major.minor>` branch and maintenance PR. Check that the tag is unused before offering its creation command. For an existing tag, emit verification or recovery commands instead.

Push one tag per command. Confirm the matching workflow run exists after each push. For plugins in the same release set, subsequent tag pushes need not wait for the preceding plugin workflow to finish.

## Core publication status

Separate the two decision points:

- **Dependent plugins may publish:** the core PyPI job succeeded and the exact core version is available.
- **Stable core may be promoted to main:** required public publication jobs and GitHub Release succeeded; inspect remaining workflow failures explicitly.

The managed-image step is warning-only. Installer smoke can fail without preventing artifact publication. Development-reset failure also occurs after publication; it needs repair but does not undo existing artifacts. Emit commands against the immutable core tag for `main` promotion, with an ancestry check and `force=false`, as specified in the release skill.

