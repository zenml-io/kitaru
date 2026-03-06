# Contributing to Kitaru

Thank you for your interest in contributing to Kitaru!

## Development Setup

### Python SDK

```bash
# Clone the repo
git clone https://github.com/zenml-io/kitaru.git
cd kitaru

# Install dependencies
uv sync

# Run all checks
just check

# Run tests
just test
```

### Documentation

The docs site lives in `docs/` and requires Node.js 22+ and pnpm.

```bash
cd docs && pnpm install
just generate-docs  # Generate CLI reference + changelog from source
just docs           # Start dev server at localhost:3000
just docs-build     # Full static build
```

See `docs/README.md` for detailed documentation authoring guidelines.

## Branch Model

- **`develop`** is the default branch. All PRs target `develop`.
- **`main`** contains only released versions. Never push directly to `main`.
- Feature branches should be created from `develop`.

## Code Style

- Python: `ruff` for formatting and linting, `ty` for type checking
- US English spelling everywhere
- Type hint all function parameters and return values
- Google-style docstrings

## Running Checks

```bash
just check  # Format, lint, typecheck, typos, YAML, links
just test   # Run all tests
just fix    # Auto-fix formatting and lint issues
```

## Commits

- Imperative mood, concise summary (50 chars or less)
- Explain *why* in the body, reference issues when applicable
- Bug fixes should include a regression test
