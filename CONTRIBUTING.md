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

## Releasing the TypeScript Packages

The three public packages, `@zenml-io/kitaru`, `@zenml-io/kitaru-mastra`, and `@zenml-io/kitaru-vercel-ai`, release in lockstep. Set the same stable or release-candidate version in all three manifests, including the adapters' workspace dependency on `@zenml-io/kitaru`.

From the merged release commit on `develop`, manually run the **Release TypeScript packages** workflow with `package-tag` set to `typescript/kitaru/v<version>`. This rehearsal validates, tests, packs, and uploads the three tarballs and their checksums without publishing them. Inspect those artifacts, then push that exact tag at the rehearsed commit to publish the packages and create the GitHub release. The workflow rejects commits that are not contained in `develop`.

Tags and npm versions are immutable. Never move or reuse `typescript/kitaru/v<version>`. Versions ending in `-rc.N` publish under the npm `rc` dist-tag and create a prerelease; stable versions publish under `latest`.

## Code Style

- Python: `ruff` for formatting and linting, `ty` for type checking
- US English spelling everywhere
- Type hint all function parameters and return values
- Google-style docstrings

## Running Checks

```bash
just check  # Format, lint, OpenAPI, typecheck, typos, YAML, actions, links
just test   # Run all tests
just fix    # Auto-fix formatting and lint issues
```

## Commits and PRs

- Imperative mood, concise summary (50 chars or less)
- Explain *why* in the body, reference issues when applicable
- Bug fixes should include a regression test
- Write clear PR titles (no `feat:`/`fix:` prefixes) and describe what the
  changes do and why

## Outside Contributor Flow

If you are not able to open a pull request directly, please open an issue first
and include the change you want to make. If you already have a branch ready,
link to it in the issue so maintainers can review it.

Maintainers may invite issue authors as repository collaborators when the change
looks appropriate for the project and direct PR access would make review easier.
Opening an issue or branch does not guarantee collaborator access; it gives the
team the context needed to decide.

## Reporting Issues

- **Bugs:** use the [bug report template](https://github.com/zenml-io/kitaru/issues/new?template=bug_report.md)
- **Features:** use the [feature request template](https://github.com/zenml-io/kitaru/issues/new?template=feature_request.md)
- **Security vulnerabilities:** see [SECURITY.md](SECURITY.md)
