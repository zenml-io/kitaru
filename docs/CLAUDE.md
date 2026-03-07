# docs/CLAUDE.md

FumaDocs-specific instructions for AI-assisted development in the docs app.

## Architecture

This is a **self-contained Next.js/FumaDocs app** for Kitaru's documentation,
served at **`kitaru.ai/docs`** as part of the unified site deployment.

It lives entirely within `docs/` and has no dependency on the root repo's
Python tooling (except for generated content). The static export is merged
into the Astro landing page build (`site/dist/docs/`) and deployed as a
single Cloudflare Worker.

- **Framework:** FumaDocs (fumadocs-ui + fumadocs-mdx + fumadocs-core)
- **Runtime:** Next.js with static export (`output: 'export'`, `basePath: '/docs'`)
- **Domain:** `kitaru.ai/docs` (subpath of the unified site, not a subdomain)
- **Package manager:** pnpm (lockfile committed)
- **Node version:** 22+ (pinned in `.node-version`)
- **Styling:** Tailwind CSS v4 (CSS-based config, not JS config file)
- **Search:** Orama client-side (static index built at build time)
- **Linter:** Biome

## Key Rules

- **Never add Node.js tooling to the repo root.** No root `package.json`,
  no root `node_modules`, no workspace config.
- **Never hand-edit generated files:** `content/docs/cli.mdx` (or `cli/`),
  `content/docs/changelog.mdx`, and `content/docs/reference/` are created by
  generation scripts and gitignored. SDK reference uses a two-step pipeline:
  `scripts/generate_sdk_docs.py` (Python extraction) + `docs/scripts/convert-sdk-docs.mjs`
  (Node MDX conversion via fumadocs-python).
- **Respect static export constraints:** No server-side features (middleware,
  rewrites, cookies, ISR). All content must be buildable at build time.
- **Only document shipped features.** No "Coming Soon" sections for unimplemented
  features. Every page must describe something a user can actually use today.
- **ZenML invisibility:** Users should never need to know Kitaru is built on
  ZenML underneath. Never say "orchestrator", "artifact store", or "pipeline"
  in user-facing docs — use Kitaru terminology (workflow, checkpoint, storage).
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
just site-build     # Full unified build (generate + docs + site + merge)

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
requires `fumapy` to be installed: `uv pip install ./docs/node_modules/fumadocs-python`

## File Responsibilities

| File | Owner |
|---|---|
| `content/docs/**/*.mdx` | Python developers (content) |
| `content/docs/**/meta.json` | Python developers (navigation) |
| `app/`, `components/`, `lib/` | Designer / frontend (layout, theme, routes, metadata) |
| `global.css` | Designer (branding) |
| `mdx-components.tsx` | Shared (component registration) |
