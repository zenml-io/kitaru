# docs/CLAUDE.md

FumaDocs-specific instructions for AI-assisted development in the docs app.

## Where the docs live now (read this first)

Kitaru documentation is split across three surfaces:

- **Hand-written docs** (concepts, guides, adapters, getting-started, etc.) live
  in **GitBook** at **`docs.zenml.io/kitaru`**, sourced from **`docs/book/`**
  (GitBook Git Sync, plain Markdown). Edit those `.md` files directly.
- **Generated SDK reference** is served by **this FumaDocs app** at
  **`sdkdocs.kitaru.ai`** (mirrors `sdkdocs.zenml.io`).
- **`kitaru.ai/docs/*`** is now a **redirect** to those new homes
  (`docs/worker/redirect.mjs` + `wrangler.redirect.toml`, worker `kitaru-site`).

So **this app is reference-only** — its content is the generated
`content/docs/reference/python/` plus a landing `index.mdx`.
Do not add hand-written pages here; those belong in `docs/book/` (GitBook).

## Architecture

This is a **self-contained Next.js/FumaDocs app** that builds the Kitaru SDK
reference site, served at **`sdkdocs.kitaru.ai`**.

It lives entirely within `docs/` and has no dependency on the root repo's
Python tooling except for the generated reference content. The static export in
`docs/out/` is deployed to the `kitaru-sdkdocs` Cloudflare Worker (root
`wrangler.toml`, bound to the `sdkdocs.kitaru.ai` custom domain).

- **Framework:** FumaDocs (fumadocs-ui + fumadocs-mdx + fumadocs-core)
- **Runtime:** Next.js with static export (`output: 'export'`, served at domain root — no basePath)
- **Domain:** `sdkdocs.kitaru.ai` (custom domain on the `kitaru-sdkdocs` worker)
- **Package manager:** pnpm (lockfile committed)
- **Node version:** 22+ (pinned in `.node-version`)
- **Styling:** Tailwind CSS v4 (CSS-based config, not JS config file)
- **Search:** Orama client-side (static index built at build time)
- **Linter:** Biome

## Deploying sdkdocs.kitaru.ai

**Automatic (the normal path):** the `SDK Reference Docs` workflow
(`.github/workflows/docs.yml`) regenerates the SDK reference, builds the
static export, and tests the redirect worker on every run. It `wrangler deploy`s
the reference site to `kitaru-sdkdocs`, then deploys the `kitaru-site` redirect
worker, only on:

- **push to `main`** (i.e. at release time), and
- **manual `workflow_dispatch`** (Actions → "SDK Reference Docs" → Run workflow)
  — deploys whichever branch it runs against, so you can ship from `develop`.

PRs build and test only; they do not deploy or create preview Workers. The workflow does not run `scripts/generate_changelog_docs.py`; public changelog traffic is handled by the redirect worker.

**Manual (from a clone, needs Cloudflare creds via `wrangler login`):**

```bash
# from repo root — regenerate reference, build, deploy
uv run python scripts/generate_sdk_docs.py
cd docs && node scripts/convert-sdk-docs.mjs && pnpm run build && cd ..
npx wrangler deploy                                   # SDK site -> sdkdocs.kitaru.ai
npx wrangler deploy --config wrangler.redirect.toml   # kitaru.ai/docs redirect worker
```

The redirect worker (`wrangler.redirect.toml`) has no build output and only
changes when redirect rules in `docs/worker/redirect.mjs` change.

## Key Rules

- **Never add Node.js tooling to the repo root.** No root `package.json`,
  no root `node_modules`, no workspace config.
- **Never hand-edit generated files:** `content/docs/changelog.mdx` and
  `content/docs/reference/` are created by
  generation scripts and gitignored. The public changelog is hosted at
  `docs.zenml.io/changelog`; the local `changelog.mdx` is only generated for
  local/reference builds. SDK reference uses a two-step pipeline:
  `scripts/generate_sdk_docs.py` (Python extraction) + `docs/scripts/convert-sdk-docs.mjs`
  (Node MDX conversion via fumadocs-python).
- **CLI contracts remain offline:** command metadata lives under `src/kitaru/cli/` and is exposed through `kitaru schema`; user-facing CLI reference publishing is deferred.
- **Respect static export constraints:** No server-side features (middleware,
  rewrites, cookies, ISR). All content must be buildable at build time.
- **Only document shipped features.** No "Coming Soon" sections for unimplemented
  features. Every page must describe something a user can actually use today.
- **ZenML invisibility:** Users should never need to know Kitaru is built on
  ZenML underneath. Never say "orchestrator", "artifact store", or "pipeline"
  in user-facing docs — use Kitaru terminology (workflow, checkpoint, storage).
- **Secret docs must stay honest:** only `kitaru.llm()` auto-resolves
  alias-linked secrets today. If you need to document non-LLM secret access,
  keep it in a clearly marked advanced or low-level note instead of implying a
  first-class Kitaru helper exists.
- **Frontmatter required:** Every `.mdx` page needs `title` and `description`.

## Content Structure

```
content/docs/
  meta.json              # Top-level sidebar ordering
  index.mdx              # Reference-site landing page
  changelog.mdx          # AUTO-GENERATED, gitignored local/reference changelog page
  reference/python/      # AUTO-GENERATED, gitignored SDK reference via fumadocs-python
```

## Available MDX Components

These are registered globally in `mdx-components.tsx`:

- `<Tabs>` / `<Tab>` — variant content (uv/pip, local/production)
- `<Callout>` — warnings, tips, notes, important info
- `<Steps>` / `<Step>` — numbered procedure walkthroughs
- `<Cards>` / `<Card>` — linking to related pages
- `<Accordions>` / `<Accordion>` — collapsible FAQ items
- `<PyFunction>`, `<PyAttribute>`, `<PySourceCode>`, `<PyFunctionReturn>` — Python SDK
  reference components (from `fumadocs-python/components`, used in generated pages only)

## Development

```bash
# From repo root:
just generate-docs  # Generate changelog + SDK reference docs (run first on fresh clone)
just docs           # Start dev server at localhost:3000
just docs-build     # Full static build
just docs-validate  # Validate the static export as served under /docs

# Or from docs/:
pnpm run dev        # Dev server
pnpm run build      # Static build
pnpm run types:check # TypeScript type checking
pnpm run lint       # Biome lint
pnpm run format     # Biome format
```

**Important:** Generated content (the local/reference changelog page and SDK reference) is gitignored.
On a fresh clone, run `just generate-docs` before `just docs` or `just docs-build`,
otherwise generated pages will be missing from the sidebar. The deployed public
changelog still lives at `docs.zenml.io/changelog`; the generated
`changelog.mdx` here is not the public changelog source. SDK reference
generation requires `fumapy` — `just generate-docs` auto-installs it from
`docs/node_modules/fumadocs-python` (requires `pnpm install` in `docs/` first).

## File Responsibilities

| File | Owner |
|---|---|
| `content/docs/index.mdx`, `content/docs/meta.json` | Python/docs developers (reference-site landing + top-level navigation) |
| `content/docs/changelog.mdx`, `content/docs/reference/**` | Generation scripts — do not hand-edit or commit generated output |
| `app/`, `components/`, `lib/` | Designer / frontend (layout, theme, routes, metadata) |
| `global.css` | Designer (branding) |
| `mdx-components.tsx` | Shared (component registration) |

## Cross-surface docs guidance

These rules apply to Kitaru documentation on any surface (GitBook `docs/book/`,
this FumaDocs reference app, and generated output).

### Authoring conventions

- Hand-written docs are **GitBook Markdown under `docs/book/`** (not MDX). Edit those `.md` files directly and add new pages to `docs/book/toc.md`. GitBook conventions live in `docs/book/AGENTS.md`.
- Links **within the GitBook space** use relative `.md` paths (e.g. `../concepts/checkpoints.md`, `flows.md#runtime-options`). Link to the **SDK reference** with `https://sdkdocs.kitaru.ai` (the separate reference site, not in the GitBook space). Link to **other ZenML docs** with absolute `https://docs.zenml.io/...`. Diagrams are static PNG images hosted on Cloudflare R2 and referenced as `https://assets.kitaru.ai/docs/diagrams/<slug>.png` (regenerate via the diagram pipeline, not committed to the repo).
- Do not commit temporary agent planning/review files such as `docs/plans/*`, `docs/reviews/*`, or prompt exports unless the user explicitly asks for a durable tracked document. Treat them as coordination scratchpads, not product docs.
- Generated reference output should still come from the existing generation scripts rather than manual edits.

### Accuracy rules for what we describe

- Treat `KITARU_*` environment variables as the public configuration surface in docs and examples. Mention `ZENML_*` only as a compatibility note when needed.
- `kitaru model register` still writes aliases to local config, but submitted/replayed runs automatically receive a transported registry snapshot via `KITARU_MODEL_REGISTRY`. Describe `kitaru model list` as listing aliases available in the current environment, not just aliases stored locally.
- Agent-facing CLI docs should describe the version-1 structured contract: success documents include `schema_version`, `command`, `ok`, `warnings`, `links`, and `next_actions`, plus `item` or `items`, `count`, and `page`; streaming commands emit JSONL events.
- Login docs/guidance should treat `kitaru login SERVER` as managed or self-hosted login and `kitaru login --local` as targeting an already-running server at `http://localhost:8000`; login never starts a server.
- Only `kitaru.llm()` auto-resolves alias-linked secrets today. If you need to document non-LLM secret access, present it as the current low-level pattern rather than implying a public Kitaru helper already exists.
- Current shipped stack-create types on the CLI/MCP surface are `local`, `kubernetes`, `vertex`, `sagemaker`, and `azureml`. Advanced CLI/MCP stack creation also supports `--extra` / structured `extra` plus the remote-only `--async` / `async_mode` convenience flag. The public Python SDK `kitaru.create_stack(...)` still provisions local stacks only, so docs should keep that distinction explicit.
