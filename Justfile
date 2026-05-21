# Pinned ZenML server image version — bump here when upgrading.
# Must match pyproject.toml, uv.lock, the server Dockerfiles, CI/release
# workflow pins, and helm/Chart.yaml; contract tests enforce alignment.
ZENML_SERVER_TAG := "0.94.4"
DOCKER_REPO := "zenmldocker/kitaru"
DOCKER_TAG := "latest"
UI_TAG := "latest"
UI_BUNDLE_ROOT := ".kitaru-ui-bundles"

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

# Check YAML formatting (skips dependabot.yml — yamlfix unquotes its `time:` value, which Dependabot then rejects as an integer)
yaml-check:
    find .github -type f \( -name '*.yml' -o -name '*.yaml' \) ! -name dependabot.yml -print0 | xargs -0 uv run yamlfix --check

# Lint GitHub Actions workflows (requires actionlint: brew install actionlint)
actions-lint:
    actionlint

# Audit GitHub Actions workflows with zizmor. For richer online checks, run:
#   GH_TOKEN=$(gh auth token) just zizmor
zizmor:
    uvx zizmor --config=.github/zizmor.yml .github/workflows/ .github/dependabot.yml

# Audit Python dependencies for known vulnerabilities (honors .github/pip-audit-ignored.txt)
audit:
    awk '/^(CVE|GHSA|PYSEC)-/ {printf "--ignore-vuln %s ", $1}' .github/pip-audit-ignored.txt | xargs uv run pip-audit

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
    find .github -type f \( -name '*.yml' -o -name '*.yaml' \) ! -name dependabot.yml -print0 | xargs -0 uv run yamlfix

# Run tests (e.g., `just test`, `just test -x`, `just test tests/test_foo.py`)
test *ARGS:
    uv run pytest {{ ARGS }}

# Build the package locally (does not publish)
build:
    uv build

# Download/extract a local Kitaru UI bundle for manual login/smoke testing.
# Defaults to the latest stable kitaru-ui-v* release.
# Pass UI_TAG=kitaru-ui-v0.2.0 to pin a stable release.
ui-bundle:
    @set -e; \
    if [ "{{ UI_TAG }}" = "latest" ]; then \
        dest="{{ UI_BUNDLE_ROOT }}/current"; \
        printf 'Downloading latest stable Kitaru UI bundle into %s/dist\n' "$dest"; \
        KITARU_UI_INSTALL_DIR="$dest" bash scripts/download-ui.sh; \
    else \
        dest="{{ UI_BUNDLE_ROOT }}/{{ UI_TAG }}"; \
        printf 'Downloading Kitaru UI bundle {{ UI_TAG }} into %s/dist\n' "$dest"; \
        KITARU_UI_INSTALL_DIR="$dest" TAG="{{ UI_TAG }}" bash scripts/download-ui.sh; \
    fi; \
    printf '\nUse with: KITARU_UI_DIST_PATH=%s/dist uv run kitaru login\n' "$dest"

# Download/extract an explicit prerelease Kitaru UI bundle for local testing.
ui-bundle-prerelease:
    @set -e; \
    if [ "{{ UI_TAG }}" = "latest" ]; then \
        printf 'Error: pass an explicit prerelease tag, e.g. UI_TAG=kitaru-ui-v0.3.0-rc.1\n' >&2; \
        exit 1; \
    fi; \
    dest="{{ UI_BUNDLE_ROOT }}/{{ UI_TAG }}"; \
    printf 'Downloading prerelease Kitaru UI bundle {{ UI_TAG }} into %s/dist\n' "$dest"; \
    KITARU_UI_ALLOW_PRERELEASE=true KITARU_UI_INSTALL_DIR="$dest" TAG="{{ UI_TAG }}" bash scripts/download-ui.sh; \
    printf '\nUse with: KITARU_UI_DIST_PATH=%s/dist uv run kitaru login\n' "$dest"

# Start local Kitaru with a prepared UI bundle.
ui-login:
    @set -e; \
    if [ "{{ UI_TAG }}" = "latest" ]; then \
        dist="{{ UI_BUNDLE_ROOT }}/current/dist"; \
    else \
        dist="{{ UI_BUNDLE_ROOT }}/{{ UI_TAG }}/dist"; \
    fi; \
    test -f "$dist/index.html" || { printf 'Error: %s/index.html not found. Run just ui-bundle first.\n' "$dist" >&2; exit 1; }; \
    KITARU_UI_DIST_PATH="$dist" uv run kitaru login

# Run smoke tests against a prepared UI bundle and keep the server for inspection.
ui-smoke:
    @set -e; \
    if [ "{{ UI_TAG }}" = "latest" ]; then \
        dist="{{ UI_BUNDLE_ROOT }}/current/dist"; \
    else \
        dist="{{ UI_BUNDLE_ROOT }}/{{ UI_TAG }}/dist"; \
    fi; \
    test -f "$dist/index.html" || { printf 'Error: %s/index.html not found. Run just ui-bundle first.\n' "$dist" >&2; exit 1; }; \
    KITARU_UI_DIST_PATH="$dist" ./scripts/smoke-test.sh --keep-server

# Build dev base image for remote stack testing (K8s, etc.)
# The image bakes in kitaru from local source + ZenML from PyPI.
# Pass REPO to override the target registry/image.
dev-image REPO="strickvl/kitaru-dev":
    docker build -f docker/Dockerfile.dev -t kitaru-dev .
    docker tag kitaru-dev {{ REPO }}:latest
    docker push {{ REPO }}:latest
    @printf 'Dev image pushed to {{ REPO }}:latest\n'

# Build production server image (ZenML server base + Kitaru + packaged Kitaru UI).
# Override variables on the command line:
#   just server-image                                  # bundle latest stable UI
#   just UI_TAG=kitaru-ui-v0.2.0 server-image          # bundle specific stable UI
#   just DOCKER_TAG=v0.2.0 server-image                # specific image tag
server-image:
    @set -e; \
    if [ "{{ UI_TAG }}" = "latest" ]; then \
        bash scripts/download-ui.sh; \
    else \
        TAG="{{ UI_TAG }}" bash scripts/download-ui.sh; \
    fi
    docker build -f docker/Dockerfile --target server \
        --build-arg ZENML_SERVER_TAG={{ ZENML_SERVER_TAG }} \
        -t kitaru-server .
    docker tag kitaru-server {{ DOCKER_REPO }}:{{ DOCKER_TAG }}
    @printf 'Server image built: {{ DOCKER_REPO }}:{{ DOCKER_TAG }}\n'

# Build and push production server image
server-image-push: server-image
    docker push {{ DOCKER_REPO }}:{{ DOCKER_TAG }}
    @printf 'Server image pushed: {{ DOCKER_REPO }}:{{ DOCKER_TAG }}\n'

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
    @printf '\n─── Validate SEO Output ────────────────────────\n'
    node scripts/validate_seo_build.mjs
    @printf '\n─── Check Internal Links ───────────────────────\n'
    lychee --offline --root-dir site/dist --index-files index.html 'site/dist/**/*.html'
    @printf '\n─────────────────────────────────────────────────\n'
    @printf 'Unified site built at site/dist/\n'
