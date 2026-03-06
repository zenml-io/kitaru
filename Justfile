# List available recipes
default:
    @just --list

# Run all checks (format, lint, typecheck, typos, yaml, links)
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

# Check links in markdown files — offline only (requires lychee: brew install lychee)
links:
    lychee --offline --root-dir . --exclude-path 'docs/node_modules' --exclude-path 'design' './**/*.md'

# Check links including external URLs (slow, used in CI)
links-external:
    lychee --root-dir . --exclude-path 'docs/node_modules' --exclude-path 'design' './**/*.md'

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

# Generate all docs content from Python source (CLI reference + changelog)
generate-docs:
    uv run python scripts/generate_cli_docs.py
    uv run python scripts/generate_changelog_docs.py

# Preview docs locally (run generate-docs first if CLI pages needed)
docs:
    cd docs && pnpm run dev

# Build docs (full static export)
docs-build:
    cd docs && pnpm run build

# Check docs build (used in CI-like local validation)
docs-check:
    cd docs && pnpm run build
