# Kitaru SEO Issue 323: Implementation Plan

## Goal

Fix the technical SEO issues from GitHub issue #323 and the Ahrefs-derived audit without making a broad site refactor. The site should tell crawlers one consistent story for each public URL: the redirect target, canonical tag, Open Graph URL, sitemap entry, and internal links should all point at the same final HTML URL.

## Background

The audit's main cluster is a slash/no-slash identity problem. In plain terms: Kitaru currently lets crawlers see two doors into the same room, then labels the doors inconsistently. That creates duplicate-page reports, canonical-to-redirect reports, redirecting sitemap entries, orphan no-slash pages, and broken links that are symptoms of the same underlying URL confusion.

Relevant current seams:

- Docs are exported by Next under `/docs` with `trailingSlash: true` (`docs/next.config.mjs:7-10`).
- The Astro shell currently says `trailingSlash: 'never'` (`site/astro.config.mjs:10-11`), even though live directory pages such as `/blog` currently redirect to `/blog/`.
- Astro canonical and OG URLs mirror the request pathname (`site/src/layouts/Base.astro:26-33`, `site/src/layouts/BlogPost.astro:48-59`). If both `/blog` and `/blog/` are reachable, each can claim itself as canonical.
- Docs pages set title, description, and OG image, but no explicit per-page canonical (`docs/app/(docs)/[[...slug]]/page.tsx:45-56`).
- The docs sitemap currently builds URLs as `https://kitaru.ai/docs${page.url}` (`docs/app/sitemap.ts:8-12`), which can emit no-slash URLs for directory-style exported pages.
- Generated CLI docs emit child links such as `./list` (`scripts/generate_cli_docs.py:427`). On `/docs/cli/executions` that resolves to `/docs/cli/list`; on `/docs/cli/executions/` it resolves correctly to `/docs/cli/executions/list`.
- Generated docs content is ignored (`docs/.gitignore:14-21`), so fixes must land in generators, layouts, config, or validation scripts, not in generated MDX output.
- The site workflow already generates docs, builds docs, builds Astro, merges the outputs, and runs offline Lychee (`.github/workflows/site.yml:37-82`). Build validation should extend that path.

External basis:

- Google treats redirects, `rel=canonical`, sitemap inclusion, and internal links as canonicalization signals; conflicting signals should be avoided.
- Google sitemap guidance says sitemaps should list the URLs wanted in search results.
- Google redirect guidance treats 301/308 as permanent canonical signals and 302/303/307 as temporary/weaker signals.

## Approach

Use **trailing slash as the canonical HTML URL shape** for directory pages, then make every SEO signal agree with that choice.

Canonical examples:

- `https://kitaru.ai/blog/`
- `https://kitaru.ai/blog/no-journal-replay/`
- `https://kitaru.ai/docs/`
- `https://kitaru.ai/docs/cli/executions/`

Do **not** slash-normalize file-like or API URLs:

- `.xml`, `.md`, `.js`, images, and API routes should keep their existing file/API shape.
- Docs markdown negotiation in `site/worker/index.js` should remain valid for both `/docs/foo` and `/docs/foo/` (`site/worker/index.js:17-36`, `site/worker/index.js:88-103`).

The safest implementation shape is four small layers:

1. **Baseline and guardrails first.** Record current live/preview behavior before changing URLs, including whether Cloudflare/Astro returns a real redirect or a duplicate `200` for no-slash HTML pages.
2. **Fix generated-link mechanics.** Stop generated CLI pages from relying on fragile `./child` links.
3. **Align metadata and sitemaps.** Add explicit canonical URLs and make sitemap URLs canonical. Exclude retired redirected URLs from sitemaps.
4. **Normalize public site links and redirects.** Update Astro trailing-slash policy, hard-coded internal links, and existing redirects. If Astro config alone does not enforce no-slash -> slash redirects in preview, add an HTML-only Worker redirect or split that redirect enforcement into its own follow-up.

This keeps the riskiest change — broad URL-shape normalization — behind validation and makes it revertible separately from the lower-risk generated-link and docs canonical fixes.

## Work Items

### 1. Capture a pre-change URL inventory

Before implementation, record a small CSV or Markdown table for representative URLs:

- `/`, `/blog`, `/blog/`
- `/blog/no-journal-replay`, `/blog/no-journal-replay/`
- `/compare`, `/compare/`
- `/docs`, `/docs/`
- `/docs/cli/executions`, `/docs/cli/executions/`
- `/docs/cli/executions.md`
- `/docs/sitemap.xml`, `/sitemap-index.xml`, `/sitemap-0.xml`

For each URL, record:

- status code and final URL after redirects
- whether no-slash HTML returns `301/308` or duplicate `200`
- canonical tag
- `og:url`
- whether the URL appears in a sitemap
- whether the URL is a file/API URL that must not be slash-normalized

Acceptance: the implementation PR has a baseline note in its description, so reviewers can compare before/after instead of guessing. If no-slash HTML still returns `200` in preview after `trailingSlash: 'always'`, the plan requires either an HTML-only Worker redirect or delaying the marketing-wide slash switch.

### 2. Fix generated CLI docs child links

Change `scripts/generate_cli_docs.py` so subcommand links are root-relative docs URLs with the canonical trailing slash, not `./{sub.slug}` (`scripts/generate_cli_docs.py:427`).

Target behavior:

- `kitaru executions` links to `/docs/cli/executions/`
- `kitaru executions list` links to `/docs/cli/executions/list/`
- `kitaru flow deployments logs` links to `/docs/cli/flow/deployments/logs/`

Implementation note: the generator needs a real nested docs path model, not just `sub.slug`. Either add a `docs_path`/`url_path` field to `CommandDoc`, or pass the parent docs path through the rendering step. Validate both a two-level command and a three-level command so `flow deployments logs` cannot regress.

Acceptance:

- Generated CLI docs no longer contain child-command table links like `](./list)`.
- Generated nested links are correct for `executions/list` and `flow/deployments/logs`.
- Offline link checking still passes after docs generation, docs build, site build, and merge.

### 3. Add docs canonical URL helpers and use them everywhere docs emit SEO URLs

Add a small docs-side helper, likely under `docs/lib/`, that converts FumaDocs page URLs into absolute canonical docs URLs.

Use it in:

- `docs/app/(docs)/[[...slug]]/page.tsx` to emit explicit `alternates.canonical` and `openGraph.url` (`docs/app/(docs)/[[...slug]]/page.tsx:45-56`).
- `docs/app/sitemap.ts` so every docs sitemap entry is the same canonical URL (`docs/app/sitemap.ts:8-12`).

Also review `docs/app/layout.tsx:19-37`. Prefer absolute canonical URLs for docs pages, or change `metadataBase` to the site origin before using relative `/docs/...` values. In either case, make `https://kitaru.ai/docs/docs` a hard validation failure.

Acceptance:

- Docs HTML pages have explicit slash-form canonicals.
- Docs sitemap entries use the same slash-form URLs.
- Retired redirected docs URLs, such as `/docs/concepts/memory` and `/docs/guides/memory`, do not appear in the generated docs sitemap unless the pages are intentionally restored.
- No built HTML contains `https://kitaru.ai/docs/docs`.
- Docs OG URL agrees with the canonical URL.

### 4. Add Astro canonical URL helpers and use them in layouts

Add a small site-side helper, likely under `site/src/lib/`, for canonical HTML paths and absolute canonical URLs.

Use it in:

- `site/src/layouts/Base.astro` for `rel=canonical` and default `og:url` (`site/src/layouts/Base.astro:26-33`).
- `site/src/layouts/BlogPost.astro` for article `og:url` (`site/src/layouts/BlogPost.astro:48-59`).

The helper should add trailing slashes to HTML page paths but leave file-like paths unchanged.

Acceptance:

- `/blog` and `/blog/`, if both are reachable during preview, do not claim different canonicals.
- Blog article metadata and Base metadata emit the same canonical URL.
- `og:type` values are not contradictory: either `Base.astro` accepts an article/page type override, or duplicate blog-specific OG tags are proven to resolve to the same values.
- File-like URLs such as sitemap XML and docs `.md` paths are not modified by the helper.

### 5. Align Astro routing, redirects, and internal links

Change the Astro URL policy to trailing slash for HTML pages, most likely by setting `trailingSlash: 'always'` in `site/astro.config.mjs:10-11`.

Update existing internal redirect destinations in `site/astro.config.mjs:12-20` so permanent internal redirects land on canonical slash URLs:

- `/get-started` -> `/book-a-demo/`
- `/docs/concepts/memory` -> `/docs/concepts/checkpoints/`
- `/docs/guides/memory` -> `/docs/guides/artifacts/`
- `/blog/kitaru-agents-now-have-memory` -> `/blog/`

Update hard-coded internal links that currently point to no-slash HTML pages:

- `site/src/components/Nav.astro:21-29`, `site/src/components/Nav.astro:45-52`
- `site/src/components/Footer.astro:20-24`
- `site/src/pages/blog/index.astro:26-30`, `site/src/pages/blog/index.astro:54-70`
- `site/src/pages/blog/[slug].astro:10-20`

Do not convert external redirects such as `/roadmap` in this first PR unless a reviewer explicitly wants that included. It is lower priority than canonical consistency.

For every redirected internal URL, choose one state and make the sitemap match it:

1. keep the page and remove the redirect; or
2. keep the redirect and ensure the redirected source URL is absent from generated sitemaps.

Current retired-looking redirects to validate this way are `/docs/concepts/memory`, `/docs/guides/memory`, and `/blog/kitaru-agents-now-have-memory` (`site/astro.config.mjs:18-20`).

Acceptance:

- Root sitemap and docs sitemap list final canonical HTML URLs only.
- Nav, footer, blog cards, related posts, content collections, MDX content, and generated docs link directly to canonical URLs after a repo/build-wide scan.
- Existing legacy internal redirects land on canonical final URLs and their source URLs are not emitted in sitemaps.

### 6. Add build-time SEO validation

Extend the existing site build proof path rather than creating a separate manual-only process. The workflow already generates docs, builds docs, builds Astro, merges them, and runs Lychee (`.github/workflows/site.yml:37-82`). Add a lightweight validation script or CI step after merge.

Checks should include:

- No HTML sitemap entry for a directory page lacks a trailing slash.
- No sitemap entry redirects when checked against the built preview/server.
- If no-slash HTML pages are intended to redirect, the status is `301` or `308`, not `307`.
- Representative HTML files contain a canonical URL and matching `og:url`.
- No built HTML contains `https://kitaru.ai/docs/docs`.
- Generated CLI docs no longer contain fragile `./child` command links.
- `robots.txt` still advertises both sitemap locations (`site/public/robots.txt`, `docs/app/robots.ts`).

Acceptance:

- `just site-build` remains the local smoke path.
- CI fails if sitemap/canonical/link-shape drift returns.

### 7. Validate preview before production

Use the PR preview Worker path already present in `.github/workflows/site.yml:95-120`.

Spot-check the same URL inventory from Work Item 1 against the preview URL. Include docs markdown negotiation checks:

- HTML: `/docs/cli/executions/` returns HTML with slash canonical.
- HTML: `/docs/cli/executions` either permanently redirects to the slash URL or emits the slash canonical.
- Markdown: `/docs/cli/executions` with `Accept: text/markdown` still serves markdown.
- Markdown: `/docs/cli/executions/` with `Accept: text/markdown` still serves markdown.

Acceptance:

- Preview behavior matches the canonical policy before merging.
- The PR description includes the before/after checks and the command snippet reviewers can run locally.

### 8. Post-deploy verification

After production deploy, re-run the same inventory against `https://kitaru.ai`.

Expected Ahrefs outcomes after re-crawl:

- Duplicate pages without canonical should drop sharply, ideally to zero for slash/no-slash pairs.
- 404 targets from generated CLI relative links should disappear.
- Sitemap redirect and canonical-to-redirect counts should drop to zero.
- Indexable pages missing from sitemap should drop for canonical docs pages.

## Risk Controls

- Keep commits small and separable:
  1. generated CLI link fix
  2. docs canonical + docs sitemap fix
  3. Astro canonical + trailing-slash + internal-link fix
  4. validation checks
- If the preview shows unexpected marketing URL behavior, revert or delay the Astro routing/internal-link commit while keeping the generated CLI/docs canonical fixes.
- Do not hand-edit ignored generated docs output.
- Do not include metadata-description rewrites, high-AI-content review, or broad content edits in the first PR. Those are useful follow-ups, but they are not needed to fix the P0 URL identity problem.

## Open Questions

- Should `/roadmap` remain a temporary external redirect, become permanent, or become a first-party page? This does not block the P0 work.
- Should reference-doc meta descriptions and short titles ship as a second SEO PR? Recommendation: yes, after URL identity is stable.

## References

- GitHub issue #323: https://github.com/zenml-io/kitaru/issues/323
- Local audit: `/Users/strickvl/Desktop/kitaru-technical-audit.md`
- Google canonicalization guide: https://developers.google.com/search/docs/crawling-indexing/consolidate-duplicate-urls
- Google redirects guide: https://developers.google.com/search/docs/crawling-indexing/301-redirects
- Google sitemap guide: https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap
- Astro configuration reference: https://docs.astro.build/en/reference/configuration-reference/
- Next `trailingSlash` docs: https://nextjs.org/docs/app/api-reference/config/next-config-js/trailingSlash
