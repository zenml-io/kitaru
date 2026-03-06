# docs/CLAUDE.md

FumaDocs-specific instructions for AI-assisted development in the docs app.

## Architecture

This is a **self-contained Next.js/FumaDocs app** for Kitaru's documentation.
It lives entirely within `docs/` and has no dependency on the root repo's
Python tooling (except for generated content).

- **Framework:** FumaDocs (fumadocs-ui + fumadocs-mdx + fumadocs-core)
- **Runtime:** Next.js with static export (`output: 'export'`)
- **Package manager:** pnpm (lockfile committed)
- **Node version:** 22+ (pinned in `.node-version`)
- **Styling:** Tailwind CSS v4 (CSS-based config, not JS config file)
- **Search:** Orama client-side (static index built at build time)
- **Linter:** Biome

## Key Rules

- **Never add Node.js tooling to the repo root.** No root `package.json`,
  no root `node_modules`, no workspace config.
- **Never hand-edit generated directories:** `content/docs/cli/` and
  `content/docs/reference/python/` are created by Python scripts and gitignored.
- **Respect static export constraints:** No server-side features (middleware,
  rewrites, cookies, ISR). All content must be buildable at build time.
- **Only document shipped features.** No "Coming Soon" sections for unimplemented
  features. Every page must describe something a user can actually use today.
- **ZenML invisibility:** Users should never need to know Kitaru is built on
  ZenML underneath. Never say "orchestrator", "artifact store", or "pipeline"
  in user-facing docs — use Kitaru terminology (saga, checkpoint, storage).
- **Frontmatter required:** Every `.mdx` page needs `title` and `description`.

## Content Structure

```
content/docs/
  meta.json              # Top-level sidebar ordering (hand-written sections only)
  index.mdx              # "What is Kitaru?" overview
  getting-started/       # Installation + quickstart
  cli/                   # AUTO-GENERATED (gitignored)
  contributing/          # Links to repo CONTRIBUTING.md
  changelog/             # Release history
  reference/python/      # AUTO-GENERATED (gitignored, deferred)
```

## Available MDX Components

These are registered globally in `mdx-components.tsx`:

- `<Tabs>` / `<Tab>` — variant content (uv/pip, local/production)
- `<Callout>` — warnings, tips, notes, important info
- `<Steps>` / `<Step>` — numbered procedure walkthroughs
- `<Cards>` / `<Card>` — linking to related pages
- `<Accordions>` / `<Accordion>` — collapsible FAQ items

## Development

```bash
# From repo root:
just docs           # Start dev server at localhost:3000
just docs-build     # Full static build
just generate-docs  # Generate CLI docs from Python source (Phase 2)

# Or from docs/:
pnpm run dev        # Dev server
pnpm run build      # Static build
```

## File Responsibilities

| File | Owner |
|---|---|
| `content/docs/**/*.mdx` | Python developers (content) |
| `content/docs/**/meta.json` | Python developers (navigation) |
| `app/`, `components/`, `lib/` | Designer / frontend (layout, theme) |
| `global.css` | Designer (branding) |
| `mdx-components.tsx` | Shared (component registration) |
