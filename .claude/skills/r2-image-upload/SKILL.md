---
name: r2-image-upload
description: >-
  Upload images and assets to Kitaru's Cloudflare R2 bucket. Use when adding
  new images to blog posts or any content that needs an R2-hosted URL. Handles
  uploading, key generation, and prints paste-ready frontmatter. Triggers:
  "upload image", "add image to R2", "new blog image", "upload asset",
  "R2 upload".
---

# R2 Image Upload

Upload images to the Kitaru R2 bucket (`kitaru-assets`) and get back absolute URLs for use in content frontmatter.

## Two-Tier Image Decision

**Before uploading to R2, decide which tier the image belongs to:**

| Tier | Where | When to use | Reference pattern |
|------|-------|-------------|-------------------|
| **A: public/** | `site/public/` | Small site-wide UI images: SVG logos, icons, favicons | `"/filename.svg"` (root-relative) |
| **B: R2** | `kitaru-assets` bucket | Content images: blog heroes, OG images, screenshots, article imagery | `"https://assets.kitaru.ai/content/..."` (absolute URL) |

**Rule of thumb:** If it appears in blog frontmatter (`image`, `ogImage`), it goes to R2 (content schemas enforce `z.string().url()`). If it's a reusable UI asset (SVG, favicon), it stays in `site/public/`.

## Upload Workflow

### Prerequisites

R2 credentials must be in `.env` (copy from `.env.example`):
```
CLOUDFLARE_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
```

### Upload command

```bash
# Single image
uv run scripts/r2-upload.py path/to/image.avif

# With custom prefix (e.g., for blog images)
uv run scripts/r2-upload.py path/to/hero.avif --prefix content/blog

# Multiple images
uv run scripts/r2-upload.py img1.png img2.jpg img3.avif

# Print paste-ready frontmatter YAML
uv run scripts/r2-upload.py path/to/hero.avif --frontmatter
```

### R2 key structure

New uploads use: `content/uploads/{sha256_8}/{sanitized-filename}`

Example: `content/uploads/1a2b3c4d/hero-image.avif`

### After uploading

1. **Verify** the URL loads: `curl -sI <url>` should return HTTP 200
2. **Paste** the URL into frontmatter or data file
3. For `site/src/lib/*.ts` files: prefer building URLs from `ASSET_BASE_URL`:
   ```ts
   import { ASSET_BASE_URL } from "./blog";
   const heroUrl = `${ASSET_BASE_URL}/content/uploads/1a2b3c4d/hero.avif`;
   ```

## For Tier A (site/public)

No upload needed. Just place the file:

```bash
cp path/to/logo.svg site/public/new-logo.svg
# Reference in code as: "/new-logo.svg"
```

## Common prefixes

| Content type | Recommended `--prefix` |
|-------------|----------------------|
| Blog post images | `content/blog` |
| General/misc | `content/uploads` (default) |
