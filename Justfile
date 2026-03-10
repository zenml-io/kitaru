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

# Build and push dev base image for remote stack testing (K8s, etc.)
# The image bakes in kitaru + its zenml git dependency from local source.
# Pass REPO to override the target registry/image.
dev-image REPO="strickvl/kitaru-dev":
    docker build -f docker/Dockerfile.dev -t kitaru-dev .
    docker tag kitaru-dev {{ REPO }}:latest
    docker push {{ REPO }}:latest
    @printf 'Dev image pushed to {{ REPO }}:latest\n'

# Build production server image (ZenML server + cloud plugins + Kitaru).
# Pass REPO to override the target registry/image.
server-image REPO="zenmldocker/kitaru" TAG="latest":
    docker build -f docker/Dockerfile --target server -t kitaru-server .
    docker tag kitaru-server {{ REPO }}:{{ TAG }}
    @printf 'Server image built: {{ REPO }}:{{ TAG }}\n'

# Build and push production server image
server-image-push REPO="zenmldocker/kitaru" TAG="latest": (server-image REPO TAG)
    docker push {{ REPO }}:{{ TAG }}
    @printf 'Server image pushed: {{ REPO }}:{{ TAG }}\n'

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
    @printf '\n─────────────────────────────────────────────────\n'
    @printf 'Unified site built at site/dist/\n'
