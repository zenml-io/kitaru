# Frontend testing guide for Kitaru maintainers

This is the internal runbook for testing the Kitaru UI from the Kitaru repo.
It covers two different jobs that are easy to mix up:

1. **Official Kitaru releases** bundle only stable/full Kitaru UI releases from
   `zenml-io/zenml-frontend-monorepo`.
2. **Local testing** can choose a UI bundle explicitly, including prerelease UI
   tags, by pointing a local Kitaru server at a downloaded `dist/` directory.

The safety rule is simple: Docker does not pick a UI release anymore. The Kitaru
package already contains the UI, and Docker copies that packaged UI into the
ZenML dashboard directory during image build.

## Mental model

Think of the UI like a printed booklet that gets put inside the Kitaru box.

- `scripts/download-ui.sh` chooses the booklet and puts it into
  `src/kitaru/_ui/dist/`.
- `uv build` packs that booklet into the Python wheel.
- `docker/Dockerfile` installs Kitaru and copies the booklet from the installed
  package into the server dashboard.

That means there is one official UI choice per Kitaru build. There is no second
hidden UI download during Docker build.

## Stable local UI bundle

Use this when you want to test the same class of UI bundle that an official
Kitaru release is allowed to use.

```bash
just ui-bundle
```

What happens:

- Kitaru resolves the highest stable/full `kitaru-ui-v*` GitHub release from
  `zenml-io/zenml-frontend-monorepo`.
- The archive checksum is verified.
- Files are extracted to `.kitaru-ui-bundles/current/dist/`.
- A `bundle_manifest.json` is written next to the dist directory.

If the monorepo release assets require authentication, make sure your shell has
access to a read token first:

```bash
export KITARU_UI_RELEASE_TOKEN=<token-with-contents-read>
just ui-bundle
```

## Stable pinned UI bundle

Use this when you need to test one specific stable UI release.

```bash
just UI_TAG=kitaru-ui-v0.2.0 ui-bundle
```

The tag must use the monorepo Kitaru UI shape: `kitaru-ui-v<semver>`.
Old bare tags like `v0.2.0` are intentionally rejected.

If that tag is a prerelease, this command fails. That failure is the release
safety rail: the normal stable lane should not accidentally consume prerelease
UI.

## Prerelease UI bundle

Use this when Bart/frontend maintainers have published a prerelease UI and want
Kitaru maintainers to validate it before promoting it to a full GitHub release.

```bash
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-bundle-prerelease
```

This is deliberately more explicit than `ui-bundle` because it sets
`KITARU_UI_ALLOW_PRERELEASE=true` under the hood. That opt-in belongs in local
or smoke testing only, never in the official Kitaru release workflow.

## Start local Kitaru with the prepared UI

After preparing a bundle, start the local server with:

```bash
just ui-login
```

For a pinned bundle:

```bash
just UI_TAG=kitaru-ui-v0.2.0 ui-login
```

For a prerelease bundle:

```bash
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-login
```

`ui-login` looks for the prepared bundle under `.kitaru-ui-bundles/.../dist` and
runs `kitaru login` with `KITARU_UI_DIST_PATH` set.

Important: `KITARU_UI_DIST_PATH` only matters when the local server starts. If a
local server is already running, restart it before testing a different UI:

```bash
kitaru logout
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-login
```

## Direct local override without Just

If you already have a UI `dist/` directory, point Kitaru at it directly:

```bash
KITARU_UI_DIST_PATH=/absolute/path/to/dist kitaru login
```

When working from this source checkout, the equivalent is usually:

```bash
KITARU_UI_DIST_PATH=/absolute/path/to/dist uv run kitaru login
```

The directory must contain `index.html`. If it does not, Kitaru raises a
user-facing error instead of silently falling back to the stock ZenML dashboard.

## Run the Kitaru smoke test against a selected UI

Use this before asking someone to click around manually:

```bash
just ui-smoke
```

For a prerelease:

```bash
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-bundle-prerelease
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-smoke
```

`ui-smoke` runs:

```bash
KITARU_UI_DIST_PATH=<prepared-dist> ./scripts/smoke-test.sh --keep-server
```

The `--keep-server` flag leaves the local server running after the automated
checks finish, so you can open the dashboard and inspect the selected UI in a
browser.

## Prerelease smoke workflow in GitHub Actions

Use `.github/workflows/ui-prerelease-smoke.yml` when you want automation to test
a prerelease UI without publishing anything.

From GitHub:

1. Open **Actions → UI prerelease smoke → Run workflow**.
2. Set `ui-tag` to a prerelease tag, for example `kitaru-ui-v0.3.0-rc.1`.
3. Set `kitaru-ref` if you need a branch other than `develop`.
4. Leave `docker-smoke` enabled unless you only need wheel validation.

What the workflow does:

- checks out the requested Kitaru ref;
- downloads the selected UI with `KITARU_UI_ALLOW_PRERELEASE=true`;
- builds the Kitaru wheel and verifies the UI files are inside it;
- optionally builds the Docker image from local source;
- checks that the Docker image contains both the packaged Kitaru UI and the
  copied ZenML dashboard files;
- publishes nothing: no PyPI package, no Docker image, no Helm chart, no Git tag,
  and no GitHub Release.

## Docker testing notes

For release-like Docker testing from this repo, use:

```bash
just server-image
```

This downloads a stable UI into `src/kitaru/_ui/dist/` first, installs Kitaru in
the image, and then Docker copies the installed package UI into the dashboard.

For a specific stable UI:

```bash
just UI_TAG=kitaru-ui-v0.2.0 server-image
```

For an unarchived local frontend build, use the separate dev-server image path:

```bash
cp -r /path/to/zenml-frontend-monorepo/apps/kitaru-ui/dist/ docker/kitaru-ui-dist/
just server-dev-image
```

Keep these two Docker paths separate:

- `server-image` tests packaged Kitaru UI, like release builds.
- `server-dev-image` tests a raw local frontend `dist/` copied into the Docker
  build context.

## Quick checklist for prerelease validation

```bash
export KITARU_UI_RELEASE_TOKEN=<token-if-needed>
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-bundle-prerelease
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-smoke
```

Then open the dashboard URL printed by `kitaru login` / the smoke test and click
through the UI. If the UI is good, frontend maintainers can promote the
`kitaru-ui-v*` release from prerelease to a full GitHub release. Only after that
can an official Kitaru release bundle it by default.
