// Render the React diagram components to static PNGs for the GitBook docs port.
//
// Pipeline:
//   1. `pnpm exec next dev` (or a running dev server) serves /docs/diagram-export
//      which renders each diagram inside a <div data-diagram="<slug>">.
//   2. This script drives a headless Chromium (Playwright) to that page, forces
//      light theme, and screenshots each diagram element to
//      docs/book/.gitbook/assets/<slug>.png at 3x device scale.
//
// Usage:
//   pnpm add -D playwright          # browsers are already cached system-wide
//   node scripts/render-diagrams.mjs [baseUrl]
//
// baseUrl defaults to http://localhost:3000 (the dev server). The script
// targets `${baseUrl}/docs/diagram-export` because next.config.mjs sets
// basePath: '/docs'.
//
// Publishing: the GitBook pages reference these PNGs from Cloudflare R2
// (https://assets.kitaru.ai/docs/diagrams/<slug>.png), not from the repo —
// docs/book/.gitbook/assets/ is gitignored. After rendering, compress and
// upload (compression: downscale 0.5 + 256-color palette via Pillow; upload to
// the kitaru-assets bucket, custom domain assets.kitaru.ai):
//
//   uv run python - <<'PY'
//   import glob; from PIL import Image
//   for f in glob.glob("docs/book/.gitbook/assets/*.png"):
//       im = Image.open(f).convert("RGBA"); w,h = im.size
//       im = im.resize((round(w*0.5), round(h*0.5)), Image.LANCZOS)
//       bg = Image.new("RGB", im.size, (255,255,255)); bg.paste(im, mask=im.split()[3])
//       bg.quantize(256, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE).save(f, optimize=True)
//   PY
//   for f in docs/book/.gitbook/assets/*.png; do \
//     npx wrangler r2 object put "kitaru-assets/docs/diagrams/$(basename "$f")" \
//       --file "$f" --content-type image/png --remote; done
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const here = dirname(fileURLToPath(import.meta.url));
const assetsDir = resolve(here, "../book/.gitbook/assets");
const baseUrl = process.argv[2] ?? "http://localhost:3000";
const target = `${baseUrl}/docs/diagram-export`;

await mkdir(assetsDir, { recursive: true });

const browser = await chromium.launch();
const page = await browser.newPage({ deviceScaleFactor: 3 });
await page.goto(target, { waitUntil: "networkidle" });

// Force light theme so assets match the GitBook (light) reading surface, and
// hide the Next.js dev-tools indicator (a fixed overlay that otherwise bleeds
// into element screenshots near the viewport corners).
await page.addStyleTag({
  content:
    "nextjs-portal,[data-nextjs-dev-tools-button],#nextjs-dev-tools-menu{display:none !important}",
});
await page.evaluate(() => {
  document.documentElement.classList.remove("dark");
  document.documentElement.style.colorScheme = "light";
});
await page.waitForTimeout(300);

const slugs = await page.$$eval("[data-diagram]", (els) =>
  els.map((el) => el.getAttribute("data-diagram")),
);

for (const slug of slugs) {
  const el = await page.$(`[data-diagram="${slug}"] figure`);
  const targetEl = el ?? (await page.$(`[data-diagram="${slug}"]`));
  const out = resolve(assetsDir, `${slug}.png`);
  await targetEl.screenshot({ path: out });
  console.log(`rendered ${slug}.png`);
}

await browser.close();
console.log(`\nDone. ${slugs.length} diagrams written to ${assetsDir}`);
