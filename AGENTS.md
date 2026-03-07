# Repository Guidelines

## Project Structure

Kitaru is a **mixed Python + web repo** that produces three things: a Python SDK package, a documentation site, and a marketing landing page — all deployed together.

```
src/kitaru/           # Python SDK package (src layout)
  cli.py              # CLI entry point (cyclopts)
  adapters/           # Framework adapter stubs (not yet implemented)
tests/                # pytest tests
examples/             # Runnable SDK examples (Phase 5/7/8/10 milestones)
docs/                 # FumaDocs Next.js app — documentation at kitaru.ai/docs
  content/docs/       # Documentation content (MDX files)
  scripts/            # Node-side doc generation (convert-sdk-docs.mjs)
  app/                # Next.js app routes, layout, metadata
site/                 # Astro landing page + runtime shell at kitaru.ai/
  src/pages/api/      # Server-side API routes (e.g. /api/waitlist with KV)
scripts/              # Doc generation + site merge scripts (includes SDK reference extraction)
spec/                 # SDK design specifications (planning material, not shipped code)
design/               # Design docs, meeting notes (gitignored, never commit)
wrangler.toml         # Unified Cloudflare Worker deployment config
```

### Unified deployment model

The docs and landing page deploy as **one Cloudflare Worker**:

1. Python scripts generate docs content (`scripts/generate_cli_docs.py`, `scripts/generate_changelog_docs.py`, `scripts/generate_sdk_docs.py`)
2. `docs/` builds a static export into `docs/out/` (Next.js with `basePath: '/docs'`)
3. `site/` builds the Astro app into `site/dist/`
4. `scripts/merge_site.sh` copies `docs/out/*` into `site/dist/docs/`
5. Root `wrangler.toml` deploys the merged output as one Worker

Astro is the runtime shell because it owns the `/api/waitlist` endpoint (backed by Cloudflare KV). The docs are pure static files mounted under `/docs`.

## Build, Test, and Development Commands

Use `uv` for Python dependency management and `just` as the command runner.

### Python workflows

- `uv sync`: install and sync dependencies
- `uv sync --extra local`: install with local ZenML runtime components
- `just check`: run all checks (format, lint, typecheck, typos, yaml, links)
- **Always run `just check` after finishing any chunk of implementation work, and fix every reported issue before pausing or handing work off.**
- `just test`: run full test suite
- `just test tests/test_file.py::test_name`: run one test
- `just fix`: auto-fix formatting, lint issues, and yaml
- `just lint`: lint only
- `just typecheck`: type check only
- `just typos`: typo check only
- `just format-check`: check formatting without modifying
- `just yaml-check`: check YAML formatting
- `just links`: check markdown links offline (requires `lychee`: `brew install lychee`)
- `just links-external`: check links including external URLs (slow)
- `just build`: build wheel and sdist locally

### Docs/site workflows

These require Node 22+ and pnpm.

- `just generate-docs`: generate CLI reference + changelog + SDK reference docs
- `just docs`: preview docs locally (dev server at localhost:3000)
- `just docs-build`: build docs static export
- `just site`: preview landing page locally (dev server at localhost:4321)
- `just site-build-only`: build landing page only (no docs merge)
- `just site-build`: full unified build (generate docs, build docs, build site, merge)

## Coding Style & Naming Conventions

- Follow US English spelling in code and docs (`initialize`, `serialize`, `color`).
- Use type hints on all public functions and return values.
- Prefer modern annotations (`list[str]`, `str | None`) over legacy `typing` aliases.
- Follow Google Python style for docstrings.
- Keep comments focused on *why* (intent/trade-offs), not line-by-line narration.
- Treat leading underscore names as private to module/class boundaries.
- Prefer Protocols/ABCs or `isinstance` over `getattr`/`hasattr` for capability checks.
- Put helpers on the class when tied to its behavior; use standalone utils only for generic cross-module functions.
- Prefer Pydantic models for data structures; checkpoint return values must be serializable.

## CLI

The `kitaru` console script is defined in `pyproject.toml` under `[project.scripts]` and implemented with cyclopts in `src/kitaru/cli.py`. Add subcommands via `@app.command`. When testing CLI commands, always pass an explicit arg list (`app(["--help"])`, not bare `app()`). CLI invocations raise `SystemExit(0)` on success.

## Testing Guidelines

Use `pytest` for unit and integration tests. Name files `test_*.py` and test functions `test_*`. Mirror source paths (example: `src/kitaru/runtime.py` -> `tests/test_runtime.py`). Every bug fix should include a regression test that fails before the fix and passes after it.

## Commit & Pull Request Guidelines

Use short, imperative subjects (for example: `Add ...`, `Update ...`, `Create ...`). Keep commit titles concise (about 50 chars), and explain the why in the body when needed.

For pull requests, use a clear human-readable title and include:
- what changed
- why it was needed
- key implementation decisions
- reviewer focus areas

Link related issues (for example `Fixes #123`) when applicable.

## CI/CD

### Python CI (`ci.yml`)

Runs on push/PR to `develop`. Jobs: lint + format check + yaml check, typos, type check, link check, and tests (Python 3.12 + 3.13).

### Site CI (`site.yml`)

Runs on push to `main` (production deploy) and PRs touching `docs/`, `site/`, `scripts/`, `src/kitaru/`, `CHANGELOG.md`, or `wrangler.toml`. Generates docs content, builds both apps, merges output, and deploys:
- **Production:** deploys unified Worker to `kitaru.ai` on `main` push
- **PR previews:** deploys a preview Worker for same-repo PRs; cleans up on PR close

### Other workflows

- `release.yml`: release automation (version bump, PyPI publish, GitHub Release)
- `spellcheck.yml`: separate typo/spell checking on `develop` PRs/pushes
- `image-optimiser.yml`: PR-only image compression for docs assets

## Branching and Release Strategy

- Default branch is `develop`. All PRs target `develop`.
- `main` tracks the latest released version only; do not push directly.
- Releases are cut via the Release workflow (`workflow_dispatch` on `develop` or `v*` tag push).
- Release branches (`release/X.Y.Z`) and tags (`vX.Y.Z`) are created automatically.
- Version is maintained in `pyproject.toml` and bumped by the release workflow. Never hardcode it — use `importlib.metadata.version("kitaru")`.
- Update `CHANGELOG.md` under `[Unreleased]` when making user-facing changes.
- The site deploys on `main` pushes, so the site goes live at release time.

## Docs Content Rules

- **Only document shipped features.** No "Coming Soon" sections.
- **ZenML invisibility:** users should never need to know Kitaru is built on ZenML. Use Kitaru terminology (workflow, checkpoint, storage), not ZenML terms (orchestrator, artifact store, pipeline).
- **Generated content is gitignored:** CLI docs (`cli.mdx` or `cli/`), `changelog.mdx`, and `reference/` are created by generation scripts and must not be hand-edited or committed. SDK reference uses a two-step pipeline: `scripts/generate_sdk_docs.py` (Python → JSON) then `docs/scripts/convert-sdk-docs.mjs` (JSON → MDX via fumadocs-python).
- **Frontmatter required:** every `.mdx` page needs `title` and `description`.

## Security & Configuration Notes

Do not commit local secrets, `.env` files, or anything in `design/`. Use `uv` (not raw `pip`) for dependency management to keep environments reproducible.
