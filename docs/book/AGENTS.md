# Documentation Guidelines for AI Agents (Kitaru GitBook)

This directory is the **GitBook source** for the Kitaru docs, published to
**`docs.zenml.io/kitaru`** via GitBook Git Sync (plain Markdown, *not* MDX).
Edit these `.md` files directly — they are the source of truth for hand-written
docs.

Do **not** put hand-written docs in `docs/content/docs/` — that is the
reference-only FumaDocs app (generated CLI + SDK reference, served at
`sdkdocs.kitaru.ai`).

## Adding & editing pages

- Pages are CommonMark `.md`. Frontmatter is `description:` (required) plus an
  optional `icon:` (a FontAwesome name, e.g. `rocket`, `gears`, `terminal`). The
  first `# H1` is the page title — there is no `title:` frontmatter key.
- **Register every page in `toc.md`** (the GitBook SUMMARY). Page URLs follow the
  `toc.md` hierarchy, not the file layout.
- Section landing pages are `README.md` inside the section folder.

## GitBook components (Liquid, not JSX)

- Callouts: `{% hint style="info" %}` … `{% endhint %}` (`info` / `warning` /
  `success` / `danger`).
- Tabs: `{% tabs %}{% tab title="X" %}` … `{% endtab %}{% endtabs %}`.
- Steppers: `{% stepper %}{% step %}` … `{% endstep %}{% endstepper %}`.
- Card grids: `<table data-view="cards">…</table>` with a
  `data-card-target data-type="content-ref"` column.
- **No React/MDX components** (`<Callout>`, `<Tabs>`, `<Card .../>`, `<XDiagram />`).
  This is GitBook Markdown — those will render literally.

## Links

- **Within this space:** relative `.md` paths — `../concepts/checkpoints.md`,
  `flows.md#runtime-options`. Section index → `../concepts/README.md`.
- **SDK / CLI reference:** `https://docs.zenml.io/sdk-reference` (it lives on the
  separate `sdkdocs.kitaru.ai` site, not in this space).
- **Other ZenML docs:** absolute `https://docs.zenml.io/...`.
- **Changelog:** `https://docs.zenml.io/changelog` (owned by the changelog repo).
- Don't link to `kitaru.ai/docs/*` — those are legacy URLs the redirect worker
  forwards here.

## Redirects

When you move or rename a page, add the old → new mapping to
`.gitbook.yaml` under `redirects:`. Inbound legacy `kitaru.ai/docs/*` URLs are
handled separately by `docs/worker/redirect.mjs`.

## Diagrams

Diagrams are **static PNGs hosted on Cloudflare R2** and referenced as:

```
<figure><img src="https://assets.kitaru.ai/docs/diagrams/<slug>.png" alt="..."><figcaption></figcaption></figure>
```

The visual source of truth is the React components in
`docs/components/diagrams.tsx`. Updating or adding a diagram is a deliberate
render-and-upload step (render the component to PNG, then `wrangler r2 object
put` into the `kitaru-assets` bucket under `docs/diagrams/`). The one-off render
harness used during the migration was removed after the initial render; recover
it from git history (`docs/scripts/render-diagrams.mjs`) if you need to
regenerate.

## Style

- Keep **Kitaru product terminology** (flow, checkpoint, stack, deployment) — do
  not rewrite into ZenML internals (pipeline, step, orchestrator).
- US English. Only document shipped features.
