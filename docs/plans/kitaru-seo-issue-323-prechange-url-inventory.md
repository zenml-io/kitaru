# Kitaru SEO issue 323 pre-change URL inventory

- Captured: 2026-05-12T14:02:17+00:00
- Target: `https://kitaru.ai` production
- Scope: representative URLs from Work Item 1 before this worktree made URL-shape changes.
- Sitemap presence columns are exact-match checks against `/docs/sitemap.xml`, `/sitemap-index.xml`, and `/sitemap-0.xml`.

| URL | Initial status / redirect | Final URL | Canonical | og:url | Exact sitemap presence | Final URL in sitemap | File/API no-slash exception |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `/` | `200` / — | `https://kitaru.ai/` | `https://kitaru.ai/` | `https://kitaru.ai/` | no | no | no |
| `/blog` | `307` / 307 -> https://kitaru.ai/blog/ | `https://kitaru.ai/blog/` | `https://kitaru.ai/blog` | `https://kitaru.ai/blog` | /sitemap-0.xml | no | no |
| `/blog/` | `200` / — | `https://kitaru.ai/blog/` | `https://kitaru.ai/blog` | `https://kitaru.ai/blog` | no | no | no |
| `/blog/no-journal-replay` | `307` / 307 -> https://kitaru.ai/blog/no-journal-replay/ | `https://kitaru.ai/blog/no-journal-replay/` | `https://kitaru.ai/blog/no-journal-replay` | `https://kitaru.ai/blog/no-journal-replay` | /sitemap-0.xml | no | no |
| `/blog/no-journal-replay/` | `200` / — | `https://kitaru.ai/blog/no-journal-replay/` | `https://kitaru.ai/blog/no-journal-replay` | `https://kitaru.ai/blog/no-journal-replay` | no | no | no |
| `/compare` | `307` / 307 -> https://kitaru.ai/compare/ | `https://kitaru.ai/compare/` | `https://kitaru.ai/compare` | `https://kitaru.ai/compare` | /sitemap-0.xml | no | no |
| `/compare/` | `200` / — | `https://kitaru.ai/compare/` | `https://kitaru.ai/compare` | `https://kitaru.ai/compare` | no | no | no |
| `/docs` | `200` / duplicate 200 risk | `https://kitaru.ai/docs` | `—` | `—` | no | no | no |
| `/docs/` | `200` / — | `https://kitaru.ai/docs/` | `—` | `—` | /docs/sitemap.xml | /docs/sitemap.xml | no |
| `/docs/cli/executions` | `200` / duplicate 200 risk | `https://kitaru.ai/docs/cli/executions` | `—` | `—` | /docs/sitemap.xml | /docs/sitemap.xml | no |
| `/docs/cli/executions/` | `200` / — | `https://kitaru.ai/docs/cli/executions/` | `—` | `—` | no | no | no |
| `/docs/cli/executions.md` | `200` / — | `https://kitaru.ai/docs/cli/executions.md` | `—` | `—` | no | no | yes |
| `/docs/sitemap.xml` | `200` / — | `https://kitaru.ai/docs/sitemap.xml` | `—` | `—` | no | no | yes |
| `/sitemap-index.xml` | `200` / — | `https://kitaru.ai/sitemap-index.xml` | `—` | `—` | no | no | yes |
| `/sitemap-0.xml` | `200` / — | `https://kitaru.ai/sitemap-0.xml` | `—` | `—` | /sitemap-index.xml | /sitemap-index.xml | yes |

## Notes

- This is a pre-change production snapshot, not a post-fix validation.
- No-slash HTML URLs that return `200` are the duplicate-door cases the plan is designed to stop or make unambiguous with canonical metadata.
- File-like URLs such as `.xml` and `.md` are intentionally not slash-normalized.
