# Frontend testing guide for Kitaru maintainers

This is the internal runbook for testing the Kitaru UI from the Kitaru repo.
It covers two different jobs that are easy to mix up:

1. **Official Kitaru releases** bundle only stable/full Kitaru UI releases from
   `zenml-io/zenml-frontend-monorepo`.
2. **Local testing** can choose a UI bundle explicitly, including prerelease UI
   tags, by downloading it into the packaged location and running the server
   from source against it.

The safety rule is simple: official Kitaru builds only ever bundle a stable UI
release. Prerelease UI testing is explicit, local, and never touches the
release pipeline.

## Mental model

Think of the UI like a printed booklet that gets put inside the Kitaru box.

- `scripts/download-ui.sh` chooses the booklet and puts it into
  `src/kitaru/_ui/dist/`.
- `uv build` packs that booklet into the Python wheel.
- The Kitaru server serves the booklet at its own root URL, straight out of
  `src/kitaru/_ui/dist/`.
- Release Docker images get the booklet by installing the published wheel.
  There is no separate ZenML dashboard and no Docker copy step.

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
  `zenml-io/zenml-frontend-monorepo`, searching across paginated GitHub release
  results instead of trusting only the first page.
- The archive checksum is verified.
- Files are extracted to `src/kitaru/_ui/dist/`.
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
UI. Tags like `kitaru-ui-v0.3.0-rc.1` are treated as prereleases from their
semver shape even if GitHub release metadata incorrectly says they are full
releases.

## Prerelease UI bundle

Use this when Bart/frontend maintainers have published a prerelease UI and want
Kitaru maintainers to validate it before promoting it to a full GitHub release.

```bash
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-bundle-prerelease
```

This is deliberately more explicit than `ui-bundle` because it sets
`KITARU_UI_ALLOW_PRERELEASE=true` under the hood. That opt-in belongs in local
or smoke testing only, never in the official Kitaru release workflow.
Tokened UI bundle jobs in CI must run only on trusted events such as `push`, not
on `pull_request` code.

## Serve the downloaded UI locally

After preparing a bundle, start the database and run the server from source:

```bash
docker compose up -d db
just ui-serve
```

`ui-serve` checks that `src/kitaru/_ui/dist/index.html` exists, then runs the
API server against the local compose database with `KITARU_SERVER_DB_NAME` set
to a dedicated database so UI testing does not share the default one.

Open `http://localhost:8000` to see the served UI. If you download a different
bundle with `just ui-bundle` or `just ui-bundle-prerelease`, restart `ui-serve`
to pick it up.

To confirm which bundle is live, check `GET /v1/info`. Its `ui_version` field
reports the tag from the served bundle's `bundle_manifest.json`.

## External dashboard redirect

Setting `KITARU_SERVER_EXTERNAL_UI=true` switches the server out of file-serving
mode. It then serves no UI files itself and redirects every non-API path to
`KITARU_SERVER_DASHBOARD_URL` instead.

## Prerelease smoke workflow in GitHub Actions

Use `.github/workflows/ui-prerelease-smoke.yml` when you want automation to test
a prerelease UI without publishing anything.

From GitHub:

1. Open **Actions → UI prerelease smoke → Run workflow**.
2. Set `ui-tag` to a prerelease tag, for example `kitaru-ui-v0.3.0-rc.1`.
3. Set `kitaru-ref` if you need a branch other than `develop`.
4. Leave `docker-smoke` enabled unless you only need wheel validation.

What the workflow does:

- checks out the workflow's own ref and the requested Kitaru ref separately, so
  the trusted download script always runs from the trusted ref;
- downloads the selected UI with `KITARU_UI_ALLOW_PRERELEASE=true`;
- builds the Kitaru wheel and verifies the UI files are inside it;
- records the UI tag, repo, and checksum in the job summary;
- optionally builds the server image from local source with
  `docker/dev-server.Dockerfile`;
- starts a PostgreSQL container and the server image against it;
- checks that the running container's installed package contains the packaged
  Kitaru UI files;
- checks that the root route returns the UI's HTML shell;
- checks that the device verification route renders without a generic error or
  a `TemplateNotFound` failure;
- publishes nothing: no PyPI package, no Docker image, no Helm chart, no Git tag,
  and no GitHub Release.

## Quick checklist for prerelease validation

```bash
export KITARU_UI_RELEASE_TOKEN=<token-if-needed>
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-bundle-prerelease
docker compose up -d db
just ui-serve
```

Then open `http://localhost:8000` and click through the UI. If the UI is good,
frontend maintainers can promote the `kitaru-ui-v*` release from prerelease to a
full GitHub release. Only after that can an official Kitaru release bundle it by
default.
