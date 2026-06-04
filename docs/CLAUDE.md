# docs/CLAUDE.md

FumaDocs-specific instructions for AI-assisted development in the docs app.

## Where the docs live now (read this first)

Kitaru documentation is split across three surfaces:

- **Hand-written docs** (concepts, guides, adapters, getting-started, etc.) live
  in **GitBook** at **`docs.zenml.io/kitaru`**, sourced from **`docs/book/`**
  (GitBook Git Sync, plain Markdown). Edit those `.md` files directly.
- **Generated SDK + CLI reference** is served by **this FumaDocs app** at
  **`sdkdocs.kitaru.ai`** (mirrors `sdkdocs.zenml.io`).
- **`kitaru.ai/docs/*`** is now a **redirect** to those new homes
  (`docs/worker/redirect.mjs` + `wrangler.redirect.toml`, worker `kitaru-site`).

So **this app is reference-only** — its content is just the generated
`content/docs/cli/` + `content/docs/reference/python/` + a landing `index.mdx`.
Do not add hand-written pages here; those belong in `docs/book/` (GitBook).

## Architecture

This is a **self-contained Next.js/FumaDocs app** that builds the Kitaru SDK +
CLI reference site, served at **`sdkdocs.kitaru.ai`**.

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
(`.github/workflows/docs.yml`) regenerates the CLI + SDK reference, builds the
static export, and `wrangler deploy`s it to `kitaru-sdkdocs`, then deploys the
`kitaru-site` redirect worker. It runs on:

- **push to `main`** (i.e. at release time), and
- **manual `workflow_dispatch`** (Actions → "SDK Reference Docs" → Run workflow)
  — deploys whichever branch it runs against, so you can ship from `develop`.

PRs get an ephemeral preview Worker (`kitaru-sdkdocs-preview-<PR#>`).

**Manual (from a clone, needs Cloudflare creds via `wrangler login`):**

```bash
# from repo root — regenerate reference, build, deploy
uv run python scripts/generate_cli_docs.py
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
- **Never hand-edit generated files:** `content/docs/cli.mdx` (or `cli/`),
  `content/docs/changelog.mdx`, and `content/docs/reference/` are created by
  generation scripts and gitignored. SDK reference uses a two-step pipeline:
  `scripts/generate_sdk_docs.py` (Python extraction) + `docs/scripts/convert-sdk-docs.mjs`
  (Node MDX conversion via fumadocs-python).
- **CLI reference fixes belong in the generator/source:** if command syntax is
  wrong in generated CLI docs, fix `scripts/generate_cli_docs.py` and/or
  `src/kitaru/cli.py`, then regenerate. Never hand-edit generated CLI pages.
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
  index.mdx              # "What is Kitaru?" overview
  getting-started/       # Installation + quickstart
  cli.mdx                # AUTO-GENERATED (flat; becomes cli/ when subcommands exist)
  contributing.mdx       # Links to repo CONTRIBUTING.md
  changelog.mdx          # AUTO-GENERATED from CHANGELOG.md
  reference/             # AUTO-GENERATED (gitignored, SDK reference via fumadocs-python)
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
just generate-docs  # Generate CLI + changelog + SDK reference docs (run first on fresh clone)
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

**Important:** Generated content (CLI reference, changelog, SDK reference) is gitignored.
On a fresh clone, run `just generate-docs` before `just docs` or `just docs-build`,
otherwise those pages will be missing from the sidebar. SDK reference generation
requires `fumapy` — `just generate-docs` auto-installs it from
`docs/node_modules/fumadocs-python` (requires `pnpm install` in `docs/` first).

## File Responsibilities

| File | Owner |
|---|---|
| `content/docs/**/*.mdx` | Python developers (content) |
| `content/docs/**/meta.json` | Python developers (navigation) |
| `app/`, `components/`, `lib/` | Designer / frontend (layout, theme, routes, metadata) |
| `global.css` | Designer (branding) |
| `mdx-components.tsx` | Shared (component registration) |
