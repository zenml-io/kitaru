# Contributing to Kitaru

Thank you for your interest in contributing to Kitaru!

## How contributions work

We're happy to take pull requests from people outside the core team. We do restrict direct PRs to collaborators, though, because we were getting a lot of drive-by PRs that didn't fit the project and each one costs review time. So the flow is: talk to us first, then write the code.

1. Find an issue that covers what you want to change and comment on it saying you'd like to take it. If there isn't one, open a new issue (the [bug report](https://github.com/zenml-io/kitaru/issues/new?template=bug_report.md) and [feature request](https://github.com/zenml-io/kitaru/issues/new?template=feature_request.md) templates both have a box for this) and describe the problem and what you'd change.
2. A maintainer replies on the issue. Sometimes that's a quick "yes, go ahead", sometimes it's a longer back and forth about scope or design. Either way, wait for that before you start.
3. Once we've agreed, a maintainer adds you as a collaborator and you open your PR against `develop`. If you already have a branch on a fork, link it in the issue and we'll look at it there.

If a PR shows up without an issue behind it, we'll probably close it and ask you to open one, even if the change looks fine. That's not a judgment on the code. We just can't tell from a cold PR whether it fits where the project is going, and it's a waste of your evening if it doesn't.

Typo and broken link fixes go through the same route. The conversation will be about one sentence long.

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

## Reporting Issues

- **Bugs:** use the [bug report template](https://github.com/zenml-io/kitaru/issues/new?template=bug_report.md)
- **Features:** use the [feature request template](https://github.com/zenml-io/kitaru/issues/new?template=feature_request.md)
- **Security vulnerabilities:** see [SECURITY.md](SECURITY.md)
