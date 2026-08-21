---
name: kitaru-docs
description: Use for Kitaru docs.
---

# Kitaru Documentation Workflow

Use this when editing or reviewing Kitaru documentation, examples, generated reference docs, docs redirects, or docs CI behavior.

## Documentation Surfaces

Kitaru docs live on three surfaces:

1. Hand-written docs are plain Markdown in `docs/book/`, published to `docs.zenml.io/kitaru` through GitBook Git Sync. Navigation is `docs/book/toc.md`, configuration is `docs/book/.gitbook.yaml`, and authoring conventions live in `docs/book/AGENTS.md`.
2. Generated SDK and CLI reference docs live in the FumaDocs app under `docs/`, with output under `docs/content/docs/reference/` served from `sdkdocs.kitaru.ai`. `scripts/generate_sdk_docs.py` extracts the allowlisted Python API, and `scripts/generate_cli_docs.py` reads the offline `kitaru schema` contract. Run both through `just generate-docs`.
3. `kitaru.ai/docs/*` redirects are handled by `docs/worker/redirect.mjs` and `wrangler.redirect.toml`.

Do not add hand-written pages to `docs/content/docs/`. The public changelog is owned by the changelog repository at `docs.zenml.io/changelog`. This repository may generate a gitignored `docs/content/docs/changelog.mdx` for local reference builds, but agents should not hand-edit or commit it.

The public marketing/runtime site lives in `zenml-io-v2`. If a task involves Astro pages, public site assets, marketing Cloudflare deployment, or runtime website APIs, switch to that repository instead of adding the code here.

## Docs Content Rules

- Only document shipped features; do not add "Coming Soon" sections.
- Inside `docs/book/`, link to sibling pages with relative `.md` paths.
- Link to SDK reference with `https://sdkdocs.kitaru.ai`.
- Link to other ZenML docs with absolute `https://docs.zenml.io/...` URLs.
- Link to diagrams with `https://assets.kitaru.ai/docs/diagrams/<slug>.png`.
- Do not commit temporary planning/review files or prompt exports unless the user explicitly asks for a durable tracked document.
- Treat `KITARU_*` variables as the public configuration surface.
- CLI contracts live in `src/kitaru/cli/app.py` and are available offline through `kitaru schema`.
- The generated OpenAPI document and `src/kitaru/api_models/` are the API contract authorities.
- Native MCP documentation must match `src/kitaru/mcp/registry.py` and `tests/mcp/snapshots/metrics.json`. Run `just mcp-schema-check`; do not copy tool counts into prose.
- The native v2 MCP server does not expose stack or model-alias management.
- Do not document v1 runtime surfaces such as `kitaru init`, `kitaru stack`, `kitaru model`, `kitaru executions`, `kitaru.llm()`, or `kitaru.create_stack()` unless they are reintroduced in v2 source and tests.
- Agent-facing CLI docs should preserve the structured JSON/JSONL, `--machine`, `--non-interactive`, and `--no-browser` contracts.
- Every `.mdx` page needs `title` and `description` frontmatter.

Do not hand-edit generated reference output. Change the SDK `PUBLIC_API` allowlist or CLI registration metadata and their tests, then run `just generate-docs`. The SDK generator needs the fumapy bridge after the docs dependencies are installed.

## Example READMEs

Example READMEs are user-facing. They should teach new users what Kitaru does and walk them through the specific example.

Do not add maintainer-oriented sections such as "Testing", CI-only credential setup, or notes about stubbed or mocked runs. If a section would not help a first-time user understand Kitaru, it belongs in tests, contributor docs, or PR descriptions instead.
