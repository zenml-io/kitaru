# Plugin development and releases

This guide explains how to test and publish the plugin distributions in `plugins/packages/`. Run repository commands from the Kitaru repository root.

## Understand the package model

Each adapter and importer is an independent Python distribution. All built-in evaluators share the `kitaru-evaluator` distribution. Adapter distributions are installed directly in agent environments and are not registered in the server's default plugin catalog.

The default catalog lives in `src/kitaru/server/api/bootstrap.py`. Server startup stores one package source for each definition. The source contains an exact requirement and a `module:callable` entrypoint. The server does not install or import the plugin package.

The root `pyproject.toml` and `uv.lock` manage Kitaru. `plugins/pyproject.toml` and `plugins/uv.lock` form a separate workspace for the plugin distributions.

Registration stores package metadata. It does not upload wheel bytes. A worker resolves the exact requirement when it executes a plugin task.

## Feature PRs and core dependencies

Feature PRs change implementation, tests, and package `Unreleased` changelog entries. Leave existing plugin package versions and server default requirements/display versions unchanged. The release-preparation PR selects the package versions and updates all matching default-catalog metadata together. This also preserves the current inventory validator's requirement that package and default versions match.

The plugin workspace already uses the local Kitaru checkout through `[tool.uv.sources]`. Preserve the published minimum dependency when the plugin still supports that core version.

When a plugin needs unreleased core APIs, use an exact dependency on the current development version from the root manifest. For example, use `kitaru==0.25.0+dev` while core is `0.25.0+dev`. Record the required core commit or PR in `Release context`. If the development reset has not landed, resolve that first rather than changing core's version in a plugin feature PR.

The release-preparation PR replaces this placeholder with the selected compatible release floor, for example `kitaru>=0.26.0`, and regenerates `plugins/uv.lock`. Preserve extras and compatible upper bounds. Verify the candidate wheel's `Requires-Dist` metadata before publication. Never publish a plugin with the development placeholder still present. A plugin-only release needs a published core that contains the required APIs.

Do not use `>=0.25.0+dev`: ordered comparisons with a local version are invalid. Do not substitute `>=0.25.0.dev0`: that range also accepts the old stable `0.25.0`. The exact development pin marks pending compatibility work; it does not prove that every checkout with that version contains the needed API. See the [Python version specification](https://packaging.python.org/en/latest/specifications/version-specifiers/).

New packages require initial manifest versions and inventory entries. Adding a server default remains an explicit release decision.

## Prepare the checkout

1. Change to the repository root.
2. Confirm the branch.
3. Install all required development extras.

```bash
cd /path/to/kitaru
export KITARU_REPO="$PWD"
git branch --show-current
uv sync --frozen --extra cli --extra worker --extra server --extra otel
uv sync --project plugins --frozen --all-packages
```

## Run the artifact test

Test all plugin distributions:

```bash
just plugin-artifact-smoke
```

Test one distribution:

```bash
uv run --no-sync python scripts/smoke_plugin_artifacts.py \
  --package plugins/packages/langfuse-importer
```

The smoke test performs these actions:

1. Build the Kitaru wheel.
2. Build each selected plugin wheel.
3. Install the wheels in a clean environment.
4. Run `uv pip check`.
5. Load each configured package entrypoint.
6. Register every selected default definition twice.
7. Verify registration idempotency.

## Build candidate wheels

Create a clean candidate directory and retain the artifacts:

```bash
find plugins/candidate-wheels -maxdepth 1 -type f -name '*.whl' -delete
uv run --no-sync python scripts/smoke_plugin_artifacts.py \
  --candidate-dir plugins/candidate-wheels
ls -lh plugins/candidate-wheels/*.whl
```

The directory must contain the Kitaru wheel plus one wheel per directory under `plugins/packages/`.

Git ignores generated files under `plugins/candidate-wheels/`. Commit changes to `plugins/candidate.Dockerfile` and `plugins/docker-compose.candidate.yml`, but do not commit wheel files.

## Build the candidate server image

Build the plugin-owned candidate server image. Production release Dockerfiles remain PyPI-only:

```bash
export KITARU_VERSION="$(uv version --short)"

docker build \
  -f plugins/candidate.Dockerfile \
  --target server \
  --build-arg KITARU_VERSION="$KITARU_VERSION" \
  --build-arg "KITARU_EXTRAS=--extra server --extra otel" \
  -t kitaru-plugin-e2e:local \
  .
```

The candidate Dockerfile installs Kitaru from `plugins/candidate-wheels`. Plugin wheels remain separate and are resolved by workers when tasks execute.

The same Dockerfile provides `client` and `worker` targets for CI. Build the worker with `--build-arg "KITARU_EXTRAS=--extra worker"`; its `UV_FIND_LINKS` points to the candidate wheel directory copied into the development image.

Confirm the default catalog inside the image:

```bash
docker run --rm kitaru-plugin-e2e:local \
  python -c 'from kitaru.server.api.bootstrap import DEFAULT_PLUGIN_DEFINITIONS; print(f"definitions={len(DEFAULT_PLUGIN_DEFINITIONS)}"); [print(d.kind.value, d.name, d.requirement, d.entrypoint) for d in DEFAULT_PLUGIN_DEFINITIONS]'
```

The current catalog contains six importers and thirteen evaluators. Adapter distributions are installed directly by agent projects and are not registered in this catalog.

## Start the candidate server

Start a PostgreSQL database and the candidate server image:

```bash
export KITARU_PORT=8000
export KITARU_API_URL="http://localhost:${KITARU_PORT}"
unset KITARU_API_KEY KITARU_API_TOKEN KITARU_TASK_TOKEN

docker compose -f plugins/docker-compose.candidate.yml up -d --wait
docker compose -f plugins/docker-compose.candidate.yml ps
curl -fsS "$KITARU_API_URL/health/live"
curl -fsS "$KITARU_API_URL/api/v1/info"
uv run --no-sync kitaru login "$KITARU_API_URL"
```

The info response must report `auth_scheme` as `none`. The Compose service must report a healthy state.

If port 8000 is occupied, set `KITARU_PORT` before you start Compose. Use the same URL in all later commands.

## Verify default registration

List all default plugins:

```bash
uv run --no-sync kitaru importer list --server "$KITARU_API_URL" --size 100
uv run --no-sync kitaru evaluator list --server "$KITARU_API_URL" --size 100
```

Inspect exact package sources:

```bash
uv run --no-sync kitaru importer version get \
  'kitaru/langfuse@1' \
  --server "$KITARU_API_URL" \
  --output json

uv run --no-sync kitaru evaluator version get \
  'kitaru/cost@1' \
  --server "$KITARU_API_URL" \
  --output json
```

Restart the server and verify idempotency:

```bash
docker compose -f plugins/docker-compose.candidate.yml restart server
docker compose -f plugins/docker-compose.candidate.yml up -d --wait

uv run --no-sync kitaru importer version list \
  'kitaru/langfuse' \
  --server "$KITARU_API_URL" \
  --size 100
```

An unchanged package requirement and entrypoint must remain at one database version after restart.

## Register an in-progress script

Register one source file while you develop a plugin. This path does not require a wheel or a PyPI release:

```bash
uv run --no-sync kitaru importer register dev-langfuse \
  --server "$KITARU_API_URL" \
  --provider langfuse \
  --script plugins/packages/langfuse-importer/src/kitaru_langfuse_importer/importer.py \
  --entrypoint parse \
  --display-version dev
```

Register a new version after you change the file:

```bash
uv run --no-sync kitaru importer version register dev-langfuse \
  --server "$KITARU_API_URL" \
  --script plugins/packages/langfuse-importer/src/kitaru_langfuse_importer/importer.py \
  --entrypoint parse \
  --display-version dev
```

The CLI uploads only the selected file. Keep the script self-contained because sibling package files are not uploaded. A worker downloads the stored script when it executes a task. Use package registration when you need to test wheel installation or package imports.

## Register a package source manually

Built-in names use the reserved `kitaru/` prefix. Use an unreserved alias for manual registration:

```bash
LANGFUSE_VERSION="$(uv version --project plugins --package kitaru-langfuse-importer --short)"
LANGFUSE_REQUIREMENT="kitaru-langfuse-importer==$LANGFUSE_VERSION"

uv run --no-sync kitaru importer register local-langfuse-wheel \
  --server "$KITARU_API_URL" \
  --provider langfuse \
  --package "$LANGFUSE_REQUIREMENT" \
  --entrypoint kitaru_langfuse_importer.importer:parse \
  --display-version "${LANGFUSE_REQUIREMENT#*==}"

uv run --no-sync kitaru importer version get \
  local-langfuse-wheel@1 \
  --server "$KITARU_API_URL" \
  --output json
```

The API validates the exact pin and entrypoint format. The artifact test validates entrypoint importability.

## Start a clean worker

Use a separate environment so the worker does not inherit editable workspace packages:

```bash
export KITARU_CANDIDATES="$KITARU_REPO/plugins/candidate-wheels"
KITARU_WHEEL="$(find "$KITARU_CANDIDATES" -maxdepth 1 -type f -name 'kitaru-[0-9]*.whl' -print -quit)"
test -n "$KITARU_WHEEL"

uv venv --clear /tmp/kitaru-plugin-worker-venv
uv pip install \
  --python /tmp/kitaru-plugin-worker-venv/bin/python \
  "${KITARU_WHEEL}[cli,worker]"

mkdir -p /tmp/kitaru-plugin-worker-run
cd /tmp/kitaru-plugin-worker-run

export UV_FIND_LINKS="$KITARU_CANDIDATES"
export PATH="/tmp/kitaru-plugin-worker-venv/bin:$PATH"

kitaru worker start \
  --server "$KITARU_API_URL" \
  --name local-wheel-worker \
  --claim importer \
  --claim evaluator
```

Keep the worker active while you create an import or evaluation job. Task subprocesses inherit `UV_FIND_LINKS` and resolve the candidate wheels.

Stop the worker with Ctrl-C before you remove the candidate server.

## Stop the candidate server

Retain the PostgreSQL data:

```bash
docker compose -f "$KITARU_REPO/plugins/docker-compose.candidate.yml" down
```

Remove the database volume before a clean registration test:

```bash
docker compose -f "$KITARU_REPO/plugins/docker-compose.candidate.yml" down -v
```

The second command deletes only the volume owned by the `kitaru-plugin-e2e` Compose project.

## Select a package release

Use the package directory and distribution name from this table:

| Package input | Distribution | Tag format |
|---|---|---|
| `braintrust-importer` | `kitaru-braintrust-importer` | `python/kitaru-braintrust-importer/vX.Y.Z` |
| `evaluator` | `kitaru-evaluator` | `python/kitaru-evaluator/vX.Y.Z` |
| `jsonl-importer` | `kitaru-jsonl-importer` | `python/kitaru-jsonl-importer/vX.Y.Z` |
| `langfuse-importer` | `kitaru-langfuse-importer` | `python/kitaru-langfuse-importer/vX.Y.Z` |
| `langgraph` | `kitaru-langgraph` | `python/kitaru-langgraph/vX.Y.Z` |
| `logfire-importer` | `kitaru-logfire-importer` | `python/kitaru-logfire-importer/vX.Y.Z` |
| `phoenix-importer` | `kitaru-phoenix-importer` | `python/kitaru-phoenix-importer/vX.Y.Z` |
| `langsmith-importer` | `kitaru-langsmith-importer` | `python/kitaru-langsmith-importer/vX.Y.Z` |
| `openai-agents` | `kitaru-openai-agents` | `python/kitaru-openai-agents/vX.Y.Z` |
| `pydantic-ai` | `kitaru-pydantic-ai` | `python/kitaru-pydantic-ai/vX.Y.Z` |

Release only the distribution that contains the change. A change to any built-in evaluator releases the shared `kitaru-evaluator` distribution.

## Prepare a release commit

Use a release-preparation PR after the implementation merges. The examples below release `kitaru-langfuse-importer` as version `0.2.0`. Select an unused version from the current registry and tags.

1. Update the selected workspace package version and lockfile.

```bash
uv version --project plugins --package kitaru-langfuse-importer 0.2.0 --no-sync
```

2. For an importer or evaluator in the default catalog, change the matching requirement and display version in `DEFAULT_PLUGIN_DEFINITIONS`. The release inventory derives the expected requirement from the package version. Skip this catalog step for an adapter distribution.
3. Resolve any core development dependency to the selected release floor and run `uv lock --project plugins`.
4. Verify that no unrelated plugin package version changed.

```bash
git diff -- \
  plugins/packages/langfuse-importer/pyproject.toml \
  plugins/uv.lock \
  src/kitaru/server/api/bootstrap.py
```

5. Run the release gates.

```bash
uv sync --project plugins --frozen --all-packages
uv run --project plugins ruff format --config plugins/pyproject.toml --check plugins
uv run --project plugins ruff check --config plugins/pyproject.toml plugins
uv run --project plugins ty check --project plugins
uv run --project plugins pytest -q -c plugins/pyproject.toml plugins/tests tests/server/test_default_plugins.py
uv run --no-sync python scripts/smoke_plugin_artifacts.py \
  --package plugins/packages/langfuse-importer
just plugin-artifact-smoke
```

6. Run the candidate server procedure when the change affects default definitions, package installation, registration, or task execution.
7. Commit the selected version, changelog, core dependency, server default metadata, and plugin lockfile in the release-preparation PR. The implementation and its tests normally arrive through earlier feature PRs.

## Configure the first PyPI release

Each distribution needs a PyPI Trusted Publisher. If the PyPI project does not exist, create a pending publisher with these values:

| Field | Value |
|---|---|
| PyPI project name | Distribution name from the package table |
| GitHub owner | `zenml-io` |
| GitHub repository | `kitaru` |
| Workflow filename | `release-plugins.yml` |
| Environment | `pypi-<distribution>`; for example, `pypi-kitaru-langfuse-importer` |

A pending publisher creates the PyPI project during its first successful upload. It does not reserve the project name before that upload.

Create one GitHub environment named `pypi-<distribution>` for each distribution. The workflow selects that environment from the release inventory, which gives every PyPI project a distinct OIDC identity while retaining one workflow file. The workflow uses GitHub OIDC and does not require a stored PyPI API token.

## Run a release dry-run

The `Release Kitaru plugins` workflow must exist on the repository default branch before GitHub accepts a manual dispatch. Rehearse the reviewed release commit after it is reachable from `develop` or the matching maintenance branch:

```bash
PACKAGE=langfuse-importer
VERSION=0.2.0
RELEASE_REF="<branch-or-tag-at-reviewed-commit>"

gh workflow run release-plugins.yml \
  --repo zenml-io/kitaru \
  --ref "$RELEASE_REF" \
  -f package-tag="python/kitaru-$PACKAGE/v$VERSION"

gh run list \
  --repo zenml-io/kitaru \
  --workflow release-plugins.yml \
  --event workflow_dispatch \
  --limit 5
```

Verify the new run's `headSha` matches the reviewed commit. Select its run ID and watch it:

```bash
gh run watch RUN_ID --repo zenml-io/kitaru --exit-status
```

Every manual dispatch is a dry-run. It validates the selected source and package version, builds the wheel, and checks installation metadata. It does not publish or create a tag. Run behavioral and default-catalog tests through the preparation checks above.

## Publish a plugin

Publish the reviewed release commit on `develop` or the unit's matching maintenance branch. For a coordinated release, first require the core `publish-python` job to succeed and verify the exact core version on PyPI. The other core jobs can continue while plugins publish. An independent plugin release uses an already-published compatible core.

```bash
git fetch origin develop --tags

PACKAGE=langfuse-importer
VERSION=0.2.0
TAG="python/kitaru-$PACKAGE/v$VERSION"
RELEASE_SHA="$(gh pr view <release-pr> --repo zenml-io/kitaru --json mergeCommit --jq '.mergeCommit.oid')"
test -n "$RELEASE_SHA" && test "$RELEASE_SHA" != null
git merge-base --is-ancestor "$RELEASE_SHA" origin/develop

git tag -a "$TAG" "$RELEASE_SHA" -m "$TAG"
git push origin "$TAG"
```

The tag push starts the `Release Kitaru plugins` workflow. The workflow resolves the package and version from the tag and accepts commits reachable from `develop` or the matching maintenance branch. It builds and verifies the distribution, publishes through PyPI Trusted Publishing, and creates a GitHub Release with the wheel, source distribution, and checksums. Stable publication then creates or advances the maintenance branch. For a maintenance release, use that branch and its exact reviewed merge commit in the commands above.

If publication stops after PyPI accepts one or more files, rerun the failed jobs from the same workflow run. The rerun downloads the original build artifact, skips files that PyPI already accepted, finishes any remaining artifacts, and creates the GitHub Release if it does not exist. Do not move or recreate the release tag.

Watch the run and require a successful conclusion:

```bash
gh run list \
  --repo zenml-io/kitaru \
  --workflow release-plugins.yml \
  --event push \
  --limit 5

gh run watch RUN_ID --repo zenml-io/kitaru --exit-status
```

## Verify a published plugin

Verify PyPI and the GitHub Release:

```bash
curl -fsS \
  https://pypi.org/pypi/kitaru-langfuse-importer/0.2.0/json \
  >/dev/null

gh release view python/kitaru-langfuse-importer/v0.2.0 \
  --repo zenml-io/kitaru
```

Install the published artifact without workspace sources:

```bash
uv run --no-project \
  --with kitaru-langfuse-importer==0.2.0 \
  python -c 'from kitaru_langfuse_importer.importer import parse; assert callable(parse)'
```

PyPI versions and Git tags are immutable. If PyPI publishing succeeds and a later tag or GitHub Release step fails, do not publish another file under the same version. Preserve the original release commit and repair only the missing GitHub metadata.
