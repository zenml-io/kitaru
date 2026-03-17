# Repository Guidelines

## Project Structure

Kitaru is a **mixed Python + web repo** that produces three things: a Python SDK package, a documentation site, and a marketing landing page — all deployed together.

```
src/kitaru/           # Python SDK package (src layout)
  cli.py              # CLI facade / console entrypoint (cyclopts)
  _cli/               # Internal command modules + shared CLI helpers
  adapters/           # Framework adapters (includes PydanticAI)
  mcp/                # MCP server tools (optional `kitaru[mcp]` extra)
tests/                # pytest tests
tests/mcp/            # MCP-specific tests (runs in `[mcp]` CI path)
examples/             # Runnable SDK examples
docs/                 # FumaDocs Next.js app — documentation at kitaru.ai/docs
  content/docs/       # Documentation content (MDX files)
  scripts/            # Node-side doc generation (convert-sdk-docs.mjs)
  app/                # Next.js app routes, layout, metadata
site/                 # Astro landing page + runtime shell at kitaru.ai/
  src/pages/api/      # Server-side API routes (e.g. /api/waitlist with KV)
scripts/              # Doc generation + site merge scripts (includes SDK reference extraction)
docker/               # Dockerfiles (Dockerfile = production server, Dockerfile.dev = dev/testing stack)
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

Use `uv` for Python dependency management and `just` as the command stack.

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

The `kitaru` console script is defined in `pyproject.toml` under `[project.scripts]`. `src/kitaru/cli.py` is the thin facade / entrypoint, while command implementations live in `src/kitaru/_cli/`. Add new subcommands in the appropriate `src/kitaru/_cli/_*.py` module and register them on the shared Cyclopts app there. When testing CLI commands, always pass an explicit arg list (`app(["--help"])`, not bare `app()`). CLI invocations raise `SystemExit(0)` on success.

Agent-facing commands should keep the shared `--output json` / `-o json` contract consistent:
- single-item commands emit `{command, item}`
- list commands emit `{command, items, count}`
- `kitaru executions logs --follow --output json` emits JSONL event objects instead of one final document

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

Runs on push/PR to `develop`. Jobs: lint + format check + yaml check, typos, type check, link check, base tests (Python 3.11 + 3.12 + 3.13), and additional test lanes with `kitaru[mcp]` installed (3.11 + 3.12).

### Site CI (`site.yml`)

Runs on push to `main` (production deploy) and PRs touching `docs/`, `site/`, `scripts/`, `src/kitaru/`, `CHANGELOG.md`, or `wrangler.toml`. Generates docs content, builds both apps, merges output, and deploys:
- **Production:** deploys unified Worker to `kitaru.ai` on `main` push
- **PR previews:** deploys a preview Worker for same-repo PRs; cleans up on PR close

### Other workflows

- `release.yml`: release automation (version bump, PyPI publish, Docker image publish, GitHub Release)
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
- **Generated vs static docs:** generated CLI reference content, changelog output, and SDK reference pages come from generation scripts and should not be hand-edited. Static hand-written MDX pages under `docs/content/docs/` (for example `getting-started/*.mdx` or `cli/login.mdx`) are tracked and may be edited directly when the feature behavior changes. SDK reference still uses a two-step pipeline: `scripts/generate_sdk_docs.py` (Python → JSON) then `docs/scripts/convert-sdk-docs.mjs` (JSON → MDX via fumadocs-python).
- **Secret docs accuracy:** only `kitaru.llm()` auto-resolves alias-linked secrets today. If you need to document non-LLM secret access, label it clearly as the current low-level pattern instead of implying there is already a dedicated Kitaru secret getter.
- **CLI docs source of truth:** if generated CLI reference syntax is wrong, fix `scripts/generate_cli_docs.py` and/or the relevant `src/kitaru/_cli/_*.py` module (use `src/kitaru/cli.py` only for facade/bootstrap issues), never the generated `docs/content/docs/cli/*` output.
- **Environment-variable docs:** document `KITARU_*` env vars as the public surface. Mention `ZENML_*` only as a compatibility note when necessary to explain migration or interop.
- **Model-registry docs:** `kitaru model register` still writes aliases to local config, but submitted/replayed runs automatically receive a transported registry snapshot via `KITARU_MODEL_REGISTRY`. `kitaru model list` should be described as listing aliases available in the current environment, not just aliases stored locally.
- **Frontmatter required:** every `.mdx` page needs `title` and `description`.

## Security & Configuration Notes

Do not commit local secrets, `.env` files, or anything in `design/`. Use `uv` (not raw `pip`) for dependency management to keep environments reproducible.
