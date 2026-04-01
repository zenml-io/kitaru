# Pinned ZenML server image version — bump here when upgrading.
# Must match the ARG ZENML_SERVER_TAG default in docker/Dockerfile and
# docker/Dockerfile.server-dev (those two are enforced by contract tests;
# this Justfile value and the CI/release workflow values are not).
ZENML_SERVER_TAG := "0.94.1"
DOCKER_REPO := "zenmldocker/kitaru"
DOCKER_TAG := "latest"
UI_TAG := "latest"
# Set to "linux/amd64,linux/arm64" for multi-arch builds (requires QEMU + buildx).
# Leave empty (default) to build for the native platform only.
DOCKER_PLATFORM := ""

# List available recipes
default:
    @just --list

# Run all checks (format, lint, typecheck, typos, yaml, actions, links)
check:
    @printf '─── Format Check ───────────────────────────────\n'
    @just format-check
    @printf '\n─── Lint ───────────────────────────────────────\n'
    @just lint
    @printf '\n─── Type Check ─────────────────────────────────\n'
    @just typecheck
    @printf '\n─── Typos ──────────────────────────────────────\n'
    @just typos
    @printf '\n─── YAML Check ─────────────────────────────────\n'
    @just yaml-check
    @printf '\n─── Actions Lint ───────────────────────────────\n'
    @just actions-lint
    @printf '\n─── Links ──────────────────────────────────────\n'
    @just links
    @printf '\n─────────────────────────────────────────────────\n'
    @printf 'All checks passed!\n'

# Check code formatting without modifying files
format-check:
    uv run ruff format --check .

# Run linter
lint:
    uv run ruff check .

# Run type checker
typecheck:
    uv run ty check

# Check for typos in source code
typos:
    uvx typos

# Check YAML formatting
yaml-check:
    uv run yamlfix --check .github/

# Lint GitHub Actions workflows (requires actionlint: brew install actionlint)
actions-lint:
    actionlint

# Check links in markdown files — offline only (requires lychee: brew install lychee)
links:
    lychee --offline --root-dir . --exclude-path '.venv' --exclude-path 'docs/node_modules' --exclude-path 'site/node_modules' --exclude-path 'design' './**/*.md'

# Check links including external URLs (slow, used in CI)
links-external:
    lychee --root-dir . --exclude-path '.venv' --exclude-path 'docs/node_modules' --exclude-path 'site/node_modules' --exclude-path 'design' './**/*.md'

# Auto-fix formatting, lint issues, and YAML
fix:
    uv run ruff format .
    uv run ruff check . --fix
    uv run yamlfix .github/

# Run tests (e.g., `just test`, `just test -x`, `just test tests/test_foo.py`)
test *ARGS:
    uv run pytest {{ ARGS }}

# Build the package locally (does not publish)
build:
    uv build

# Build dev base image for remote stack testing (K8s, etc.)
# The image bakes in kitaru from local source + ZenML from PyPI.
# Pass REPO to override the target registry/image.
dev-image REPO="strickvl/kitaru-dev":
    docker build -f docker/Dockerfile.dev -t kitaru-dev .
    docker tag kitaru-dev {{ REPO }}:latest
    docker push {{ REPO }}:latest
    @printf 'Dev image pushed to {{ REPO }}:latest\n'

# Build production server image (ZenML server base + Kitaru + Kitaru UI).
# Override variables on the command line:
#   just server-image                          # defaults
#   just UI_TAG=v0.1.0 server-image            # specific UI release
#   just DOCKER_TAG=v0.2.0 server-image        # specific image tag
server-image:
    #!/usr/bin/env bash
    set -euo pipefail
    platform="{{ DOCKER_PLATFORM }}"
    if [ -n "$platform" ]; then
        docker buildx build -f docker/Dockerfile --target server \
            --platform "$platform" \
            --build-arg ZENML_SERVER_TAG={{ ZENML_SERVER_TAG }} \
            --build-arg KITARU_UI_TAG={{ UI_TAG }} \
            -t {{ DOCKER_REPO }}:{{ DOCKER_TAG }} \
            --push .
        printf 'Multi-arch server image pushed: {{ DOCKER_REPO }}:{{ DOCKER_TAG }} (%s)\n' "$platform"
    else
        docker build -f docker/Dockerfile --target server \
            --build-arg ZENML_SERVER_TAG={{ ZENML_SERVER_TAG }} \
            --build-arg KITARU_UI_TAG={{ UI_TAG }} \
            -t kitaru-server .
        docker tag kitaru-server {{ DOCKER_REPO }}:{{ DOCKER_TAG }}
        printf 'Server image built: {{ DOCKER_REPO }}:{{ DOCKER_TAG }}\n'
    fi

# Build and push production server image.
# Multi-arch builds (DOCKER_PLATFORM set) push during build; this is a no-op then.
server-image-push: server-image
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "{{ DOCKER_PLATFORM }}" ]; then
        docker push {{ DOCKER_REPO }}:{{ DOCKER_TAG }}
        printf 'Server image pushed: {{ DOCKER_REPO }}:{{ DOCKER_TAG }}\n'
    else
        printf 'Multi-arch image already pushed during build.\n'
    fi

# Build dev server image for local UI testing.
# Requires docker/kitaru-ui-dist/ to exist (copy from kitaru-ui/dist/).
server-dev-image:
    @test -f docker/kitaru-ui-dist/index.html || { printf 'Error: docker/kitaru-ui-dist/index.html not found.\nBuild kitaru-ui first: cd kitaru-ui && pnpm build\nThen: cp -r dist/ /path/to/kitaru/docker/kitaru-ui-dist/\n' >&2; exit 1; }
    docker build -f docker/Dockerfile.server-dev --target server \
        --build-arg ZENML_SERVER_TAG={{ ZENML_SERVER_TAG }} \
        -t kitaru-server-dev .
    @printf 'Server dev image built: kitaru-server-dev\n'

# Generate all docs content from Python source (CLI reference + changelog + SDK reference)
generate-docs:
    uv run python scripts/generate_cli_docs.py
    uv run python scripts/generate_changelog_docs.py
    @# fumapy is bundled in the fumadocs-python npm package, not on PyPI.
    @# Auto-install it if docs/node_modules exists (requires prior pnpm install in docs/).
    @test -d docs/node_modules/fumadocs-python && uv pip install -q docs/node_modules/fumadocs-python || true
    uv run python scripts/generate_sdk_docs.py
    cd docs && node scripts/convert-sdk-docs.mjs

# Preview docs locally (run generate-docs first if CLI pages needed)
docs:
    cd docs && pnpm run dev

# Build docs (full static export)
docs-build:
    cd docs && pnpm run build

# Preview landing page locally
site:
    cd site && pnpm run dev

# Build landing page only (no docs merge)
site-build-only:
    cd site && pnpm run build

# Build full unified site: generate docs → build docs → build site → merge
site-build:
    @printf '─── Generate Docs ──────────────────────────────\n'
    @just generate-docs
    @printf '\n─── Build Docs ─────────────────────────────────\n'
    @just docs-build
    @printf '\n─── Build Site ─────────────────────────────────\n'
    @just site-build-only
    @printf '\n─── Merge Docs into Site ────────────────────────\n'
    bash scripts/merge_site.sh
    @printf '\n─── Check Internal Links ───────────────────────\n'
    lychee --offline --root-dir site/dist 'site/dist/**/*.html'
    @printf '\n─────────────────────────────────────────────────\n'
    @printf 'Unified site built at site/dist/\n'
