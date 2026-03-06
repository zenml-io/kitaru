# List available recipes
default:
    @just --list

# Run all checks (format, lint, typecheck, typos, yaml, links)
check: format-check lint typecheck typos yaml-check links

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

# Check links in markdown files (requires lychee: brew install lychee)
links:
    lychee --root-dir . './**/*.md'

# Auto-fix formatting, lint issues, and YAML
fix:
    uv run ruff format .
    uv run ruff check . --fix
    uv run yamlfix .github/

# Run tests (e.g., `just test`, `just test -x`, `just test tests/test_foo.py`)
test *ARGS:
    uv run pytest {{ ARGS }}
