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
`content/docs/reference/python/` and `content/docs/cli/` plus a landing
`index.mdx`.
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

**Automatic path:** the `SDK Reference Docs` workflow (`.github/workflows/docs.yml`) regenerates the SDK reference via `scripts/generate_sdk_docs.py` (the v2 generator: a `PUBLIC_API` allowlist over `kitaru.client` and `kitaru.task`), builds the static export, and tests the redirect worker.

Deployment is limited to:

- **push to `main`** (i.e. at release time), and
- **manual `workflow_dispatch`** (Actions → "SDK Reference Docs" → Run workflow)
  — deploys whichever branch it runs against, so you can ship from `develop`.

PRs build and test only; they do not deploy or create preview Workers. The workflow does not run `scripts/generate_changelog_docs.py`; public changelog traffic is handled by the redirect worker.

**Manual redirect-only deployment** requires Cloudflare credentials through `wrangler login`.

```bash
# from repo root
npx wrangler deploy --config wrangler.redirect.toml   # kitaru.ai/docs redirect worker
```

The redirect worker (`wrangler.redirect.toml`) has no build output and only
changes when redirect rules in `docs/worker/redirect.mjs` change.

## Key Rules

- **Never add Node.js tooling to the repo root.** No root `package.json`,
  no root `node_modules`, no workspace config.
- **Never hand-edit generated files:** `content/docs/changelog.mdx` and `content/docs/reference/` are generated and gitignored. The public changelog is hosted at `docs.zenml.io/changelog`; the local `changelog.mdx` is only for local/reference builds. The reference pipeline is `scripts/generate_sdk_docs.py` (Python, griffe extraction filtered to the `PUBLIC_API` allowlist) followed by `docs/scripts/convert-sdk-docs.mjs` (Node, JSON to MDX); run it via `just generate-docs`.
- **CLI reference is schema-driven:** command metadata lives under `src/kitaru/cli/` and is exposed through `kitaru schema`; `scripts/generate_cli_docs.py` consumes that schema to generate `content/docs/cli/` (run via `just generate-docs`). The generator hardcodes no command names — CLI changes flow through automatically.
- **Respect static export constraints:** No server-side features (middleware,
  rewrites, cookies, ISR). All content must be buildable at build time.
- **Only document shipped features.** No "Coming Soon" sections for unimplemented
  features. Every page must describe something a user can actually use today.
- **Use v2 product terminology:** describe sessions, nodes, agents, cohorts, experiments, evaluations, jobs, tasks, workers, and replays as the current source and API models define them. Do not import v1 flow, checkpoint, stack, model-alias, or ZenML runtime terminology into v2 docs.
- **Secret docs must stay honest:** derive secret behavior from the current v2 API models, client resource, routes, and authorization tests. Do not imply that a v1 model-alias or LLM helper exists in v2.
- **Frontmatter required:** Every `.mdx` page needs `title` and `description`.

## Content Structure

```
content/docs/
  meta.json              # Top-level sidebar ordering
  index.mdx              # Reference-site landing page
  changelog.mdx          # AUTO-GENERATED, gitignored local/reference changelog page
  reference/python/      # AUTO-GENERATED, gitignored SDK reference via fumadocs-python
  cli/                   # AUTO-GENERATED, gitignored CLI reference from `kitaru schema`
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
just docs           # Start dev server at localhost:3000
just docs-build     # Full static build
just docs-lint      # Biome lint + format check
just docs-validate  # Validate the static export as served under /docs

# Or from docs/:
pnpm run dev        # Dev server
pnpm run build      # Static build
pnpm run types:check # TypeScript type checking
pnpm run lint       # Biome lint + format check
pnpm run format     # Biome format
```

The `SDK Reference Docs` workflow runs `pnpm run lint` on every PR that touches `docs/`, and root `just check` does not cover it, so run `just docs-lint` before pushing. `pnpm exec biome check --write` applies the safe fixes (formatting, import order). Rule exceptions live in `biome.jsonc` with a comment explaining each one.

**Important:** Generated content (the local/reference changelog page and SDK reference) is gitignored. On a fresh clone, run `uv sync --extra cli` (the CLI generator runs `kitaru schema` in-process), `pnpm install` in `docs/`, then `uv pip install ./docs/node_modules/fumadocs-python`, then `just generate-docs` to materialize the reference before `just docs` shows the full sidebar. The deployed public changelog still lives at `docs.zenml.io/changelog`; the generated `changelog.mdx` here is not the public changelog source.

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
- Generated reference output must come from reviewed generation scripts rather than manual edits. The public surface it documents is the `PUBLIC_API` allowlist in `scripts/generate_sdk_docs.py`; change that allowlist (and its tests) rather than hand-editing output when the SDK surface changes.

### Accuracy rules for what we describe

- Treat `KITARU_*` environment variables as the public configuration surface in docs and examples. Mention `ZENML_*` only as a compatibility note when needed.
- Agent-facing CLI docs should describe the version-1 structured contract: success documents include `schema_version`, `command`, `ok`, `warnings`, `links`, and `next_actions`, plus `item` or `items`, `count`, and `page`; streaming commands emit JSONL events.
- Login docs/guidance should treat `kitaru login SERVER` as managed or self-hosted login and `kitaru login --local` as provisioning or reusing the CLI-owned Docker Compose deployment. The local deployment defaults to `http://localhost:8000`; `--port` takes precedence over `KITARU_LOCAL_PORT`, and the selected port persists with the deployment.
- Treat `src/kitaru/cli/app.py`, the offline `kitaru schema` output, and the generated OpenAPI document as the command and API authorities. Do not document v1 runtime commands such as `kitaru init`, `kitaru stack`, `kitaru model`, or `kitaru executions` unless they are reintroduced in v2 source and tests.
- Native MCP documentation must match `tests/mcp/snapshots/metrics.json` and `src/kitaru/mcp/registry.py`. Do not copy tool counts into prose; run `just mcp-schema-check` and describe the tools present in the current snapshot. The native v2 MCP server does not expose stack or model-alias management.
