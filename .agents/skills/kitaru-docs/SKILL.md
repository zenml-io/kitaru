---
name: kitaru-docs
description: Use for Kitaru docs.
---

# Kitaru Documentation Workflow

Use this when editing or reviewing Kitaru documentation, examples, generated
reference docs, docs redirects, or docs CI behavior.

## Documentation Surfaces

Kitaru docs live on three surfaces:

1. Hand-written docs are plain Markdown in `docs/book/`, published to
   `docs.zenml.io/kitaru` via GitBook Git Sync. Edit these `.md` files directly.
   Navigation is `docs/book/toc.md`, config is `docs/book/.gitbook.yaml`, and
   authoring conventions live in `docs/book/AGENTS.md`.
2. Generated SDK reference docs live in the FumaDocs app under `docs/`.
   Generated output is `docs/content/docs/reference/python/`, served from
   `sdkdocs.kitaru.ai`.
3. `kitaru.ai/docs/*` redirects are handled by `docs/worker/redirect.mjs` and
   `wrangler.redirect.toml`.

Do not add hand-written pages to the FumaDocs app under `docs/content/docs/`.
The public changelog is owned by the changelog repo at `docs.zenml.io/changelog`.
This repo may generate a gitignored `docs/content/docs/changelog.mdx` for local
reference builds, but agents should not hand-edit or commit it.

The public marketing/runtime site for Kitaru lives in `zenml-io-v2`. If a task
involves Astro pages, public site assets, marketing Cloudflare Pages deployment,
or runtime web APIs such as waitlist/get-started/newsletter endpoints, switch
to that repository instead of adding that code here.

## Docs Content Rules

- Only document shipped features; do not add "Coming Soon" sections.
- Keep ZenML invisible to users. Use Kitaru terminology such as workflow,
  checkpoint, stack, deployment, and storage. Avoid ZenML terms such as
  orchestrator, artifact store, pipeline, and step in user docs.
- Inside `docs/book/`, link to sibling pages with relative `.md` paths, such as
  `../concepts/checkpoints.md` or `flows.md#runtime-options`.
- Link to SDK reference with `https://sdkdocs.kitaru.ai`.
- Link to other ZenML docs with absolute `https://docs.zenml.io/...` URLs.
- Link to diagrams with
  `https://assets.kitaru.ai/docs/diagrams/<slug>.png`.
- Do not commit temporary planning/review files such as `docs/plans/*`,
  `docs/reviews/*`, or prompt exports unless the user explicitly asks for a
  durable tracked document.
- Only `kitaru.llm()` auto-resolves alias-linked secrets today. If documenting
  non-LLM secret access, label it as the current low-level pattern instead of
  implying there is a dedicated Kitaru secret getter.
- CLI command contracts live under `src/kitaru/cli/` and are available offline through `kitaru schema`; CLI reference publishing is deferred.
- Current shipped stack-create types on CLI/MCP are `local`, `kubernetes`,
  `vertex`, `sagemaker`, and `azureml`.
- Advanced CLI/MCP stack creation supports `--extra` / structured `extra` plus
  the remote-only `--async` / `async_mode` convenience flag.
- Public Python SDK `kitaru.create_stack(...)` remains local-only; keep that
  distinction explicit.
- Document `KITARU_*` env vars as the public surface. Mention `ZENML_*` only as
  a compatibility note when necessary for migration or interop.
- `kitaru model register` still writes aliases to local config, but
  submitted/replayed runs automatically receive a transported registry snapshot
  via `KITARU_MODEL_REGISTRY`.
- Describe `kitaru model list` as listing aliases available in the current
  environment, not only aliases stored locally.
- Every `.mdx` page needs `title` and `description` frontmatter.

## Example READMEs

Example READMEs are user-facing. They should teach new users what Kitaru does
and walk them through the specific example.

Do not add maintainer-oriented sections such as "Testing", CI-only credential
setup, or notes about stubbed/mocked test runs. If a section would not help a
first-time user understand Kitaru, it belongs in tests, contributor docs, or PR
descriptions instead.
