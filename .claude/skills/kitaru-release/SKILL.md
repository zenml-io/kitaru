---
name: kitaru-release
description: >-
  Guide the Kitaru release process end-to-end — diff develop against the
  last tag, classify commits (src / docs content / site / infra),
  filter site-only PRs out of the Python library CHANGELOG, check
  zenml-io/kitaru-ui for a new UI release that will be bundled into the
  server image, suggest a version bump, update CHANGELOG.md, run the
  smoke test, trigger the release workflow via gh, and rewrite the
  auto-generated GitHub Release notes into structured
  Highlights / Changed / Fixed sections. Interactive — pauses for user
  confirmation at version choice, CHANGELOG diff, smoke-test result,
  and release-notes draft. Use when the user invokes
  /kitaru-release, or says "cut a release", "make a release",
  "release kitaru", "new kitaru version", "ship a release",
  "prepare a release", "what would be in the next release",
  "bump kitaru version".
---

# Kitaru Release

End-to-end runbook for cutting a new Kitaru release. Every step has exact commands; never substitute or invent alternatives.

## Interaction contract

This workflow is **interactive with mandatory pauses**. Do not run multiple phases back-to-back without user confirmation. The four pauses are marked ★ in the checklist. Never skip them — releases publish to PyPI + Docker Hub + ECR and force-push `main`, so silent errors compound.

There is also a **fifth pause enforced by GitHub itself**: the `pypi` environment has required reviewers (`kitaru-admins` team). Mid-workflow, the release job pauses at the environment gate until a `kitaru-admins` member approves the deployment. This is a feature, not a bug — treat it as a built-in safety net even if the user who triggered the run is the same person who approves.

## Checklist

Copy and track progress in your todo / task list:

```
- [ ] Step 1: Fetch + gather state
- [ ] Step 2: Classify commits by scope
- [ ] Step 3: Check kitaru-ui for a new release since last Kitaru release
- [ ] Step 4: ★ Pause — show summary, suggest version, await user confirmation
- [ ] Step 5: Update CHANGELOG [Unreleased] block
- [ ] Step 6: ★ Pause — show CHANGELOG diff, await confirmation, then commit + push
- [ ] Step 7: Run smoke test
- [ ] Step 8: ★ Pause — verify smoke test green, await confirmation to trigger release
- [ ] Step 9: Trigger release workflow via gh, watch to completion
- [ ] Step 10: Draft structured release notes
- [ ] Step 11: ★ Pause — show drafted notes, await confirmation
- [ ] Step 12: Apply notes via gh release edit
- [ ] Step 13: Final summary with all URLs
```

---

## Step 1: Fetch + gather state

Always fetch first — `main` gets force-pushed during releases and stale local refs produce the wrong diff.

```bash
git fetch origin main develop --tags --prune
git checkout develop
git pull --ff-only
```

Identify the last release tag (do NOT use `origin/main` as a base — always use the tag, since tags are immutable and main is force-pushed):

```bash
LAST_TAG=$(git describe --tags --abbrev=0 origin/main)
echo "Last release: $LAST_TAG"
```

List commits since last release:

```bash
git log "$LAST_TAG"..origin/develop --oneline
git diff "$LAST_TAG"..origin/develop --stat | tail -30
```

## Step 2: Classify commits by scope

For each commit between `$LAST_TAG` and `origin/develop`, determine its scope from the file paths it touched:

| Scope | Paths | CHANGELOG? |
|---|---|---|
| **Library** | `src/kitaru/**` | Yes |
| **Docs content** | `docs/content/**.mdx` | Yes |
| **Scripts / build** | `scripts/**`, `pyproject.toml` version-adjacent | Sometimes (judgement call) |
| **Docs site infra** | `docs/app/**`, `docs/scripts/**`, `docs/package.json` | No (unless user-visible) |
| **Landing site** | `site/**` | **No** — site has its own deploy cadence |
| **CI / dependabot** | `.github/workflows/**`, dependabot bumps | No |
| **Release infra** | `docker/**`, `helm/**` | No unless user-facing |

Per-commit inspection:

```bash
git show --stat <sha> | head -30
```

Treat no-op pairs (add X / revert X in same unreleased window) as excluded — they net to nothing.

## Step 3: Check kitaru-ui for new release

Kitaru's production Docker image bundles the `kitaru-ui` dashboard. The release workflow's `kitaru-ui-tag` input defaults to the latest kitaru-ui release, so if a new UI ships between Kitaru releases, the next Kitaru release bundles it automatically.

Fetch the last Kitaru release timestamp and the kitaru-ui releases:

```bash
LAST_KITARU_TS=$(gh release view "$LAST_TAG" -R zenml-io/kitaru --json publishedAt --jq .publishedAt)
gh release list -R zenml-io/kitaru-ui --limit 10 \
  --json tagName,publishedAt,isLatest,isDraft,isPrerelease
```

From the JSON, find the latest non-draft, non-prerelease UI release and compare its `publishedAt` to `$LAST_KITARU_TS`:

- If UI `publishedAt > $LAST_KITARU_TS` → a new UI will ship. **Remember the UI tag name** for release notes step 10.
- If UI `publishedAt <= $LAST_KITARU_TS` → same UI as last release. Don't mention it.

Do **not** fetch or summarize what's in the UI release — just note the tag if it's newer.

## Step 4: ★ Pause — summary + version suggestion

Present a summary table to the user covering:

1. Commits since last release with scope classification
2. Whether a new kitaru-ui ships (tag only, no contents)
3. File-level diff stats
4. Version bump suggestion with reasoning

Version semantics:

| Bump | When |
|---|---|
| **Major** (`X.0.0`) | Breaking public API change, primitive removed, config file format breaks |
| **Minor** (`0.X.0`) | New user-facing SDK primitive, new CLI command group, new public surface |
| **Patch** (`0.0.X`) | Bug fix, doc improvement, internal refactor, small-surface CLI tweak |

Default to patch unless the diff clearly warrants minor. A single new CLI flag is usually patch. A whole new command group (e.g. `kitaru memory`) is minor.

**Wait for user to confirm or override the version.** Do not proceed until they've agreed on a version number.

## Step 5: Update CHANGELOG [Unreleased] block

Read `CHANGELOG.md` and locate the `## [Unreleased]` heading. Under it, organize entries into:

```markdown
## [Unreleased]

### Added
- [new user-facing capabilities]

### Changed
- [modifications to existing behavior]

### Fixed
- [bug fixes]
```

Rules:

- **One bullet per logical change**, not one bullet per commit.
- **Always verify PR references** — cross-check every `(#N)` in existing `[Unreleased]` bullets against `git log --oneline $LAST_TAG..origin/develop`. A common failure mode: the bullet is written with a draft PR number that changed when rebased. Correct any mismatches.
- **Include** library changes (`src/`) and docs content changes (`docs/content/**.mdx`) that materially help readers.
- **Exclude** site-only PRs, dependabot action bumps, docs-infra PRs (sitemap, llms.txt, redirects), and no-op revert pairs.
- Each bullet should be scannable. Lead with the effect (what users see), then mechanism if non-obvious.
- If a change touches the CLI, use backticks for command names and flags: `` `kitaru executions list --size 20` ``.

## Step 6: ★ Pause — show diff + commit

```bash
git diff CHANGELOG.md
```

Show the diff to the user. **Wait for confirmation.** Only then:

```bash
git add CHANGELOG.md
git commit -m "$(cat <<'EOF'
Update CHANGELOG for upcoming release

[1-2 sentences summarising what was added to the Unreleased block
 and what was intentionally excluded]
EOF
)"
```

Ask the user to confirm the push:

```bash
git push origin develop
```

Never push without that explicit confirmation — the release workflow reads `CHANGELOG.md` from `develop` at runtime, so this push is load-bearing for the release step.

## Step 7: Run smoke test

```bash
./scripts/smoke-test.sh
```

Expected runtime: 3-5 minutes. The script:

- Does a full `uv sync --python 3.12 --extra local --extra llm --extra mcp`
- Starts a local Kitaru server on `http://127.0.0.1:8383`
- Exercises CLI, SDK flows (including replay), MCP tools, and (if `OPENAI_API_KEY` is set) an end-to-end LLM flow
- Tears down the server

If `OPENAI_API_KEY` is not set, LLM tests will be marked SKIPPED — surface this to the user so they can decide whether to re-run with the key set.

The script uses `set -uo pipefail` **without `-e`** deliberately — it continues past failures to collect all results and prints a final `Passed: N  Failed: M  Skipped: K` summary.

Prefer running in the background with `run_in_background: true` and tail the log afterwards — the full output is verbose and not useful in conversation context.

## Step 8: ★ Pause — verify smoke test

Parse the final summary. **Any non-zero `Failed:` count = STOP.**

- Surface the failing check names to the user
- Do NOT proceed to the release trigger
- Offer to investigate individual failures

Only when `Failed: 0` and the user confirms, proceed.

## Step 9: Trigger release workflow

```bash
gh workflow run release.yml --ref develop \
  -f version=<AGREED_VERSION> \
  [-f kitaru-ui-tag=<UI_TAG>]   # only if pinning a specific UI version
  [-f dry-run=true]             # only if user requested
```

Confirm the trigger succeeded:

```bash
sleep 5
gh run list --workflow=release.yml --limit 1 \
  --json databaseId,status,conclusion,displayTitle,createdAt
```

Capture the `databaseId` and watch:

```bash
gh run watch <RUN_ID> --exit-status
```

Run this in the background (`run_in_background: true`) with a generous timeout (600000ms / 10min). Typical runtime is 4-8 minutes for success paths (plus a few seconds for the approval gate — see below).

### Approving the pypi deployment gate

For **non-dry-run** releases, the `release` job pauses at `environment: pypi` until a `kitaru-admins` team member approves. `gh run watch` will show the run in `waiting` state while this is pending. The user triggering the run can approve their own deployment (`prevent_self_review: false` is set on the environment).

Check for pending approvals:

```bash
gh api repos/zenml-io/kitaru/actions/runs/<RUN_ID>/pending_deployments \
  --jq '.[] | {env: .environment.name, state: .current_user_can_approve}'
```

**Option A — approve in the web UI (recommended for one-off):** Open the Actions run page, click "Review deployments", tick the `pypi` box, click "Approve and deploy".

**Option B — approve via CLI:**

```bash
# Look up the pypi environment ID dynamically (it's stable but better not to hard-code)
ENV_ID=$(gh api repos/zenml-io/kitaru/environments/pypi --jq .id)
gh api -X POST repos/zenml-io/kitaru/actions/runs/<RUN_ID>/pending_deployments \
  -F "environment_ids[]=$ENV_ID" \
  -f state=approved \
  -f comment='Approved via kitaru-release skill'
```

Dry-runs (`-f dry-run=true`) skip the gate entirely because the workflow sets `environment: ''` when dry-run is true.

Never approve a release on someone else's behalf without their confirmation. If the user triggering the release is not a `kitaru-admins` member, ask them to ping an admin to approve, or pause the skill until an admin has done so.

### After approval (or immediately for dry-run)

On completion, verify release artifact exists:

```bash
gh release view v<VERSION> --json name,tagName,isDraft,url,publishedAt
```

If `isDraft: false` and `publishedAt` is populated, the release succeeded. If the workflow failed, inspect job logs with `gh run view <RUN_ID> --log-failed` and stop — do not attempt the notes-editing step.

## Step 10: Draft release notes

Fetch the auto-generated notes so you can see what to strip:

```bash
gh release view v<VERSION> --json body --jq .body
```

Auto-notes list every merged PR including site-only ones. Rewrite into:

```markdown
## Highlights

[1-2 sentence summary framed relative to the previous release. For a patch, say "A small maintenance release on top of v<prev>". For a minor with a flagship feature, foreground that feature. Mention the new kitaru-ui only if step 3 found a newer one: "This release also bundles the latest Kitaru UI (<ui-tag>)." — do not describe UI changes.]

## Added
- [if any new user-facing capability — use bullet text from CHANGELOG]

## Changed
- [use bullet text from CHANGELOG, expand where helpful for non-experts]

## Fixed
- [use bullet text from CHANGELOG]

**Full Changelog**: https://github.com/zenml-io/kitaru/compare/v<prev>...v<VERSION>
```

Rules:

- **Skip empty sections.** If there's nothing Fixed, omit the Fixed heading entirely.
- **Keep it proportional.** Patch releases get a short Highlights paragraph; minor/major releases can have richer Highlights with subsections + code samples (see the `v0.4.0` release for the flagship-feature pattern).
- **Do not include** site-only PRs (launch blog, lightbox, redirects, sitemap), dependabot action bumps, or no-op revert pairs. These were already filtered from CHANGELOG; the release notes should follow the same filter.
- **UI release line placement**: if mentioning the new UI, put it as the last sentence of the Highlights paragraph — not a separate section, not in a PR list.

## Step 11: ★ Pause — show drafted notes

Present the full drafted notes as a fenced code block to the user. **Wait for confirmation** before applying.

## Step 12: Apply notes

```bash
gh release edit v<VERSION> --notes "$(cat <<'EOF'
[drafted notes from step 10]
EOF
)"
```

Verify:

```bash
gh release view v<VERSION> --json body --jq .body | head -20
```

## Step 13: Final summary

Print a completion table with every artifact URL:

| Artifact | Link |
|---|---|
| GitHub Release | `https://github.com/zenml-io/kitaru/releases/tag/v<VERSION>` |
| PyPI | `https://pypi.org/project/kitaru/<VERSION>/` |
| Docker Hub | `zenmldocker/kitaru:<VERSION>` + `:latest` |
| CHANGELOG on main | `https://github.com/zenml-io/kitaru/blob/main/CHANGELOG.md` |

Mark any post-release follow-ups (social posts, docs sync) as user-driven. The skill is done at this point.

---

## Known gotchas

- **Main is force-pushed.** Always diff against the last tag, never against `origin/main`. `git fetch --tags` is mandatory before every invocation.
- **CHANGELOG PR references drift.** Draft PR numbers get renumbered at merge. Cross-check every `(#N)` against `git log`.
- **Site vs library changelog.** `site/` changes deploy on their own cadence via `site.yml`. They do not belong in the Python library CHANGELOG even when they land on the same `develop` branch.
- **UI tag default.** The release workflow defaults `kitaru-ui-tag` to the latest kitaru-ui release. Only pass `-f kitaru-ui-tag=v<X>` if the user explicitly wants to pin to an older UI.
- **Concurrency group.** `release.yml` has `concurrency: group: release, cancel-in-progress: false` — a second release trigger queues rather than cancels. If something goes wrong mid-release, do not trigger a second run; wait for the first to finish, then reset from the resulting state.
- **Dry-run environment.** Real publishes use the `pypi` GitHub environment (requires secrets + manual approval); dry-runs use no environment. If the user wants a dry-run first, pass `-f dry-run=true` and loop back through Step 9 again for the real run after they approve.
- **PyPI approval gate.** The `pypi` environment has required reviewers (`kitaru-admins` team, `prevent_self_review: false`). Every non-dry-run release pauses partway through awaiting approval. The triggering user can approve their own deployment if they're in `kitaru-admins`. If they're not, the release will sit waiting indefinitely until an admin approves — do not forget this step. `gh run watch` will show the run in `waiting` state while the gate is open; this is normal, not a hang.
- **The `prompt-exports/` directory** is commonly untracked in the working tree — ignore it when staging CHANGELOG commits.

## Inputs and outputs reference

Release workflow inputs (`release.yml`):

| Input | Required | Default | Notes |
|---|---|---|---|
| `version` | yes | — | Semver without `v` prefix, e.g. `0.4.1` |
| `kitaru-ui-tag` | no | latest | `v` prefix required, e.g. `v0.2.0` |
| `dry-run` | no | `false` | Skips PyPI/Docker/tag pushes |

Useful state-inspection commands:

```bash
# What's on develop not yet released
git log "$(git describe --tags --abbrev=0 origin/main)"..origin/develop --oneline

# Current [Unreleased] CHANGELOG block
sed -n '/## \[Unreleased\]/,/## \[/p' CHANGELOG.md | head -50

# Active release workflow runs
gh run list --workflow=release.yml --limit 5 \
  --json databaseId,status,conclusion,displayTitle,createdAt

# Latest kitaru-ui release tag
gh release list -R zenml-io/kitaru-ui --limit 1 \
  --json tagName,publishedAt --jq '.[0]'
```
