---
name: kitaru-release
description: >-
  Guide the Kitaru release process end-to-end — diff develop against the
  last tag, classify commits (src / docs content / docs infra / release infra),
  filter docs-infra-only PRs out of the Python library CHANGELOG, remember that
  marketing-site work now lives in sibling zenml-io-v2, check
  zenml-io/zenml-frontend-monorepo for the latest stable kitaru-ui-v*
  release that will be bundled into the Python package and then copied
  into the Docker image, suggest a version bump, update CHANGELOG.md, run the
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
- [ ] Step 3: Check monorepo Kitaru UI stable releases since last Kitaru release
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
| **Docs site infra** | `docs/app/**`, `docs/scripts/**`, `docs/package.json`, `wrangler.toml` | No (unless user-visible) |
| **Marketing site** | Lives in sibling `zenml-io-v2`, not this repo | No — handle in that repo |
| **CI / dependabot** | `.github/workflows/**`, dependabot bumps | No |
| **Release infra** | `docker/**`, `helm/**` | No unless user-facing |

Per-commit inspection:

```bash
git show --stat <sha> | head -30
```

Treat no-op pairs (add X / revert X in same unreleased window) as excluded — they net to nothing.

## Step 3: Check monorepo Kitaru UI stable releases

Official Kitaru releases bundle a Kitaru UI release from `zenml-io/zenml-frontend-monorepo` into the Python package. The Docker image then copies that already-packaged UI from the installed `kitaru` package. Docker does **not** download UI assets or choose a UI tag itself.

Before changing UI bundle selection, frontend smoke testing, Docker dashboard packaging, or release UI workflow behavior, read `FRONTEND-TESTING.md`. It is the canonical runbook for stable/prerelease `kitaru-ui-v*` testing and token/trusted-event boundaries.

The release workflow's `kitaru-ui-tag` input accepts only `kitaru-ui-v<semver>` tags. If the input is empty, `scripts/download-ui.sh` selects the highest stable/full `kitaru-ui-v*` release. Drafts and prereleases are excluded. Prerelease UI tags are only for local testing and `.github/workflows/ui-prerelease-smoke.yml`.

Fetch the last Kitaru release timestamp and the monorepo releases:

```bash
LAST_KITARU_TS=$(gh release view "$LAST_TAG" -R zenml-io/kitaru --json publishedAt --jq .publishedAt)
gh release list -R zenml-io/zenml-frontend-monorepo --limit 50 \
  --json tagName,publishedAt,isDraft,isPrerelease \
  --jq '[.[] | select(.tagName | startswith("kitaru-ui-v"))]'
```

From the JSON, find the highest/version-latest non-draft, non-prerelease `kitaru-ui-v*` release and compare its `publishedAt` to `$LAST_KITARU_TS`:

- If UI `publishedAt > $LAST_KITARU_TS` → a new UI will ship. **Remember the UI tag name** for release notes step 10.
- If UI `publishedAt <= $LAST_KITARU_TS` → same UI as last release. Don't mention it.
- If there is no full/non-prerelease `kitaru-ui-v*` release → stop and tell the user the official Kitaru release is blocked until frontend maintainers promote one.

Do **not** fetch or summarize what's in the UI release — just note the tag if it's newer.

## Step 4: ★ Pause — summary + version suggestion

Present a summary table to the user covering:

1. Commits since last release with scope classification
2. Whether a new Kitaru UI bundle ships (tag only, no contents)
3. File-level diff stats
4. Version bump suggestion with reasoning

Version semantics:

| Bump | When |
|---|---|
| **Major** (`X.0.0`) | Breaking public API change, primitive removed, config file format breaks |
| **Minor** (`0.X.0`) | New user-facing SDK primitive, new CLI command group, new public surface |
| **Patch** (`0.0.X`) | Bug fix, doc improvement, internal refactor, small-surface CLI tweak |

Default to patch unless the diff clearly warrants minor. A single new CLI flag is usually patch. A whole new command group (e.g. `kitaru auth`) is minor.

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

- Does a full `uv sync --python 3.12 --extra local --extra llm --extra mcp` plus the adapter extras (`pydantic-ai`, `openai-agents`, `claude-agent-sdk`, `gemini`, `langgraph`)
- Starts a local Kitaru server on `http://127.0.0.1:8383`
- Exercises CLI, SDK flows (including replay), MCP tools, the five adapter examples (PydanticAI, LangGraph, OpenAI Agents, Claude Agent SDK, Gemini Interactions), and an end-to-end LLM flow
- Tears down the server

**Set credentials before running, or most of the meaningful work is SKIPPED.** The five adapter examples are always present, but only the ones with a credential actually exercise a real model call — without keys they degrade to a `--help`/import smoke or are skipped outright. For a release-grade run, export the full set first:

```bash
export OPENAI_API_KEY=...        # OpenAI Agents real run, LangGraph `calls`, research bot
export ANTHROPIC_API_KEY=...     # Claude Agent SDK real run (or CLAUDE_CODE_USE_BEDROCK=1 / _VERTEX=1)
export GEMINI_API_KEY=...        # Gemini Interactions raw `--mode model` real run (GOOGLE_API_KEY also works)
export KITARU_SMOKE_RESEARCH_BOT=1     # opt in to the real web-search research-bot test
export KITARU_SMOKE_GEMINI_ANTIGRAVITY=1  # opt in to the Gemini `--mode antigravity` managed-agent run
./scripts/smoke-test.sh
```

**Gemini has two credential paths and they unlock different tests** — this is easy to get wrong. The smoke test checks for `GEMINI_API_KEY`/`GOOGLE_API_KEY` (the direct API path) *separately* from Vertex ADC config (`GOOGLE_GENAI_USE_VERTEXAI=true` + `GOOGLE_CLOUD_PROJECT` + `GOOGLE_CLOUD_LOCATION`):

| Gemini test | Direct API key (`GEMINI_API_KEY`/`GOOGLE_API_KEY`) | Vertex ADC (`GOOGLE_GENAI_USE_VERTEXAI` + project + location) |
|---|---|---|
| `--mode model` (raw response) | ✅ runs a real call automatically | ❌ **skipped** — Vertex ADC is *not* accepted on this path |
| `--mode antigravity` (managed-agent preset) | ✅ runs, but only with `KITARU_SMOKE_GEMINI_ANTIGRAVITY=1` | ✅ runs, but only with `KITARU_SMOKE_GEMINI_ANTIGRAVITY=1` |

So if the release machine authenticates Gemini through **Vertex** (common for `zenml-core`-style setups), `--mode model` will skip no matter what, and the *only* way to get a real Gemini round-trip is to set `KITARU_SMOKE_GEMINI_ANTIGRAVITY=1` so the antigravity test runs against Vertex ADC. Don't report "Gemini covered" off a Vertex run unless you opted into antigravity. (Vertex ADC must actually be available — `gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS` — or the antigravity test will fail rather than skip.)

Parse the final summary and **tell the user exactly which checks were SKIPPED and why** (which key was unset), so they can decide whether a partial run is good enough or they want to re-run with the missing key. A bare run with no keys is a weak release gate — flag that explicitly rather than reporting "all passed" when half the adapter suite was skipped.

**Watch for stale `VIRTUAL_ENV` contamination.** If the shell has a leftover `VIRTUAL_ENV` from a different worktree/venv, `uv` prints a "does not match the project environment path" warning **to stderr**, and the few smoke checks that capture `... 2>&1` and parse JSON (e.g. `analytics disabled in smoke test`, `executions get <latest>`) will choke on the warning glued to the front of the JSON and report a spurious `<parse error>` failure/skip. This is environment noise, not a regression — confirm by re-running the affected command with `unset VIRTUAL_ENV` before treating it as a release blocker.

The script uses `set -uo pipefail` **without `-e`** deliberately — it continues past failures to collect all results and prints a final `Passed: N  Failed: M  Skipped: K` summary.

Prefer running in the background with `run_in_background: true` and tail the log afterwards — the full output is verbose and not useful in conversation context.

**Verify the bundled UI too (recommended pre-release check).** The Python smoke test above does not exercise the dashboard. To click through the exact UI release that will ship, bundle it and run the UI smoke against it:

```bash
export KITARU_UI_RELEASE_TOKEN=<token-with-contents-read>  # the private monorepo needs auth
just UI_TAG=kitaru-ui-v<X.Y.Z> ui-bundle    # use the tag from Step 3; or bare `just ui-bundle` for highest stable
just UI_TAG=kitaru-ui-v<X.Y.Z> ui-smoke     # runs the smoke test with that UI and keeps the server up
```

`ui-smoke` runs `KITARU_UI_DIST_PATH=<prepared-dist> ./scripts/smoke-test.sh --keep-server`, so after it passes the server stays up and prints a dashboard URL for manual click-through. `KITARU_UI_RELEASE_TOKEN` is required because `ui-bundle` downloads from the **private** `zenml-io/zenml-frontend-monorepo`; without it you get a `curl: (22) ... 404`. Read **`FRONTEND-TESTING.md`** (repo root) for the full stable/prerelease bundle runbook.

**If running from a git worktree:** a fresh worktree may not have `src/kitaru/_ui/dist/` populated yet. The same `just ui-bundle` / `just ui-smoke` path above prepares it, or run `bash scripts/download-ui.sh` before `./scripts/smoke-test.sh`. The direct override path is `KITARU_UI_DIST_PATH=/path/to/dist ./scripts/smoke-test.sh --keep-server`.

## Step 8: ★ Pause — verify smoke test

Parse the final summary. **Any non-zero `Failed:` count = STOP and investigate — do not auto-proceed.**

- Surface the failing check names to the user
- Do NOT proceed to the release trigger on an unexplained failure
- Offer to investigate individual failures

Before treating a failure as a hard blocker, **rule out spurious environment noise** — most commonly the stale `VIRTUAL_ENV` contamination described in Step 7. If a failing check captures `2>&1` and parses JSON, re-run that exact command with `unset VIRTUAL_ENV` (and from a clean shell, not a worktree with a different venv). If it then passes cleanly, the failure is an environment artifact, not a regression: say so explicitly, show the clean re-run as evidence, and you may proceed once the user confirms. A genuine `Failed:` with no environmental explanation still blocks the release.

Only when every failure is either `Failed: 0` or provably spurious, and the user confirms, proceed.

## Step 9: Trigger release workflow

**Dry-run first when the release machinery itself changed.** A dry-run (`-f dry-run=true`) builds the wheel, downloads + bundles the UI, and builds the Docker image, but skips every publish/push/tag and the `pypi` approval gate — so it surfaces workflow bugs *before* any irreversible step. Strongly prefer a dry-run first whenever `release.yml`, `scripts/download-ui.sh`, the Docker/Helm packaging, or the UI-bundling path has changed since the last release, or when a new secret/credential is involved. (This is exactly what caught the missing `KITARU_UI_RELEASE_TOKEN` and the PyPI-verify bug before they could half-publish a release.) For a routine release with no machinery changes, a dry-run is optional but cheap.

```bash
gh workflow run release.yml --ref develop \
  -f version=<AGREED_VERSION> \
  [-f kitaru-ui-tag=kitaru-ui-v<X.Y.Z>]  # only if pinning a specific stable/full UI version
  [-f dry-run=true]                      # do a dry-run pass first; re-run without it after it's green
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

Dry-runs (`-f dry-run=true`) use the `dry-run` GitHub environment and skip the `pypi` approval gate entirely.

Never approve a release on someone else's behalf without their confirmation. If the user triggering the release is not a `kitaru-admins` member, ask them to ping an admin to approve, or pause the skill until an admin has done so.

### After approval (or immediately for dry-run)

On completion, verify release artifact exists:

```bash
gh release view v<VERSION> --json name,tagName,isDraft,url,publishedAt
```

For non-dry-run releases, the workflow also validates `CLOUD_PLUGINS_REPO_PAT` early, pins the current `zenml-io/zenml-cloud-plugins` `main` SHA, checks that `refs/tags/kitaru-<VERSION>` is either missing or already points at that SHA, then creates the tag after the Kitaru Helm chart has been pushed. That tag is the downstream trigger for the Kitaru Pro server image build; dry-runs skip it and should say so in the workflow summary.

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
| Cloud plugins trigger | `https://github.com/zenml-io/zenml-cloud-plugins/tree/kitaru-<VERSION>` |
| CHANGELOG on main | `https://github.com/zenml-io/kitaru/blob/main/CHANGELOG.md` |

Mark any post-release follow-ups (social posts, docs sync) as user-driven. The skill is done at this point.

---

## Known gotchas

- **Main is force-pushed.** Always diff against the last tag, never against `origin/main`. `git fetch --tags` is mandatory before every invocation.
- **CHANGELOG PR references drift.** Draft PR numbers get renumbered at merge. Cross-check every `(#N)` against `git log`.
- **Marketing vs library changelog.** Marketing-site changes now live in `zenml-io-v2`, not this repo. Docs infra changes usually do not belong in the Python library CHANGELOG unless they change user-visible docs behavior.
- **UI tag default.** The release workflow defaults `kitaru-ui-tag` to the highest stable/full `kitaru-ui-v*` release from `zenml-io/zenml-frontend-monorepo`. Only pass `-f kitaru-ui-tag=kitaru-ui-v<X.Y.Z>` if the user explicitly wants to pin to a specific stable UI. Official releases reject prerelease UI tags. Read `FRONTEND-TESTING.md` before touching this path.
- **Prerelease UI smoke.** To validate a prerelease UI, use Actions → `UI prerelease smoke` with a required `ui-tag` such as `kitaru-ui-v0.3.0-rc.1`. That workflow sets `KITARU_UI_ALLOW_PRERELEASE=true`, builds/verifies locally, and publishes nothing.
- **Concurrency group.** `release.yml` has `concurrency: group: release, cancel-in-progress: false` — a second release trigger queues rather than cancels. If something goes wrong mid-release, do not trigger a second run; wait for the first to finish, then reset from the resulting state.
- **Dry-run environment.** Real publishes use the `pypi` GitHub environment (requires secrets + manual approval); dry-runs use the `dry-run` GitHub environment and skip the `pypi` approval gate. If the user wants a dry-run first, pass `-f dry-run=true` and loop back through Step 9 again for the real run after they approve.
- **PyPI approval gate.** The `pypi` environment has required reviewers (`kitaru-admins` team, `prevent_self_review: false`). Every non-dry-run release pauses partway through awaiting approval. The triggering user can approve their own deployment if they're in `kitaru-admins`. If they're not, the release will sit waiting indefinitely until an admin approves — do not forget this step. `gh run watch` will show the run in `waiting` state while the gate is open; this is normal, not a hang.
- **Non-dry-run releases require `RELEASE_GIT_TOKEN` for protected branch pushes.** `release.yml` now fails early if `secrets.RELEASE_GIT_TOKEN` is missing on a real release, before any PyPI/Docker/Helm side effects. The secret is only used for the protected branch pushes to `develop`, `main`, and `release/*`; checkout, GitHub API reads, and the Kitaru repo tag push still use the default `GITHUB_TOKEN`. If a later push step still gets a 403/permission error, check that the token's identity is actually allowed to bypass the `develop`/`main` rulesets and create `release/*` branches. Dry-runs do not require this secret.
- **Non-dry-run releases require `CLOUD_PLUGINS_REPO_PAT` for the downstream Kitaru Pro trigger.** `release.yml` validates this secret before expensive publish work, pins the current `zenml-io/zenml-cloud-plugins` `main` SHA, and fails early if `refs/tags/kitaru-$VERSION` already exists somewhere else. After Docker and Helm have been published, the workflow creates that tag at the pinned SHA. The secret needs read access to `zenml-cloud-plugins/main` and permission to create Git tags in `zenml-io/zenml-cloud-plugins`. Existing matching tags are treated as recovery-safe and skipped; existing divergent tags fail the release and require manual investigation. Dry-runs do not require this secret and do not create the downstream tag.
- **All releases require `KITARU_UI_RELEASE_TOKEN` to fetch the UI.** The "Download stable Kitaru UI" step (`scripts/download-ui.sh`) pulls the bundle from the **private** `zenml-io/zenml-frontend-monorepo`, and only sends an auth header when the token is set. A missing/empty secret resolves to an empty string (not a hard error), so the request goes out unauthenticated and the private repo answers `404` → `curl: (22)` → the step dies *before* any publish. The token is a fine-grained PAT with **Contents: read** on the monorepo — and fine-grained PATs **expire**, so a release that worked months ago can fail here later. If you see the 404 at "Download stable Kitaru UI", check the secret exists (`gh secret list -R zenml-io/kitaru`) and that the PAT hasn't expired or hit org pending-approval. This step runs on dry-runs too, so a dry-run catches a missing/expired token safely.
- **Recovery dispatch skips file mutations.** When `v$VERSION` already exists on origin, the workflow detects this pre-checkout, checks out the tag itself, and skips the "Bump version" / "Update CHANGELOG" / "Update lockfile" / "Commit release changes" steps. This is intentional: `uv lock` is not stable across time (it regenerates `exclude-newer` timestamps and may re-resolve transitive deps if newer versions have been released between the original tag push and the recovery dispatch), so running it would create a commit on top of the tagged SHA and fail the consistency check. Do not re-enable those steps for recovery — the tag is the authoritative identity anchor.
- **Recovering when the fix is in `release.yml` itself.** If a release fails partway (e.g. after PyPI publish + tag push but before Docker/Helm/main/GitHub-Release) and the fix lives in the workflow file, do **not** commit the fix to `develop`. The "Push release commit to develop" step does a *plain, non-force* push of the bump commit and only succeeds as a fast-forward; advancing `develop` breaks that and the recovery fails one step later. Instead: `git checkout -b fix-branch v$VERSION` (off the tag), commit the workflow fix, push the branch, and dispatch `gh workflow run release.yml --ref fix-branch -f version=$VERSION`. GitHub runs the workflow YAML from the dispatched ref (so it gets your fix) while the recovery logic still checks out the *tag* for the build (so `develop` stays put and the bump commit fast-forwards). All downstream ref/publish steps are idempotent (skip-existing / fast-forward-only / create-or-match), so the recovery picks up exactly where it died. Afterwards, open a normal PR from `fix-branch` into `develop` so the fix lands for future releases.
- **Editing `release.yml` triggers `zizmor`.** Any change under `.github/workflows/**` fires the path-filtered `zizmor.yml` security scan, which runs `uvx zizmor` **unpinned** (latest). Because it drifts stricter over time and only re-scans on workflow edits, a recovery/fix PR can inherit a *pre-existing* finding it didn't cause (e.g. a floating `# v7` action comment that now needs the exact `# v7.1.0`). Run `just zizmor` locally before pushing any workflow change — `just check` does **not** include zizmor (it runs actionlint, a different tool).
- **The `prompt-exports/` directory** is commonly untracked in the working tree — ignore it when staging CHANGELOG commits.

## Inputs and outputs reference

Release workflow inputs (`release.yml`):

| Input | Required | Default | Notes |
|---|---|---|---|
| `version` | yes | — | Semver without `v` prefix, e.g. `0.4.1` |
| `kitaru-ui-tag` | no | latest stable/full `kitaru-ui-v*` | Optional stable UI pin, e.g. `kitaru-ui-v0.2.0`; prereleases are rejected |
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

# Latest Kitaru UI releases in the frontend monorepo
gh release list -R zenml-io/zenml-frontend-monorepo --limit 50 \
  --json tagName,publishedAt,isDraft,isPrerelease \
  --jq '[.[] | select(.tagName | startswith("kitaru-ui-v"))]'
```
