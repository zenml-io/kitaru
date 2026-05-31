#!/usr/bin/env node
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  DOCS_PREFIX,
  RETIRED_DOCS_REDIRECT_STATUS,
  RETIRED_DOCS_REDIRECTS,
  ROOT_DOCS_ASSET_PATHS,
} from "../worker/docs-routing.mjs";
import worker from "../worker/handler.mjs";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const docsRoot = resolve(scriptDir, "..");
const outDir = join(docsRoot, "out");
const allowedRootDocsAssetPaths = new Set(ROOT_DOCS_ASSET_PATHS);
const requiredPublicPaths = [
  `${DOCS_PREFIX}/`,
  `${DOCS_PREFIX}/sitemap.xml`,
  "/favicon.svg",
];
const badOutputSnippets = [
  "https://kitaru.ai/docs/docs",
  "/docs/docs/",
  "https://kitaru.ai/og/docs",
];

const failures = [];
const pathResolutionCache = new Map();

function fail(message) {
  failures.push(message);
}

function walkHtmlFiles(directory) {
  if (!existsSync(directory)) {
    return [];
  }

  return readdirSync(directory).flatMap((entry) => {
    const path = join(directory, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      return walkHtmlFiles(path);
    }

    return path.endsWith(".html") ? [path] : [];
  });
}

function publicPathToCandidates(publicPath) {
  const url = new URL(publicPath, "https://kitaru.ai");
  const pathnames = [url.pathname];

  if (url.pathname.startsWith(`${DOCS_PREFIX}/`)) {
    pathnames.push(url.pathname.slice(DOCS_PREFIX.length) || "/");
  }

  return pathnames.flatMap((pathname) => {
    const decoded = decodeURIComponent(pathname);
    const relative = decoded.replace(/^\/+/, "");

    if (decoded.endsWith("/")) {
      return [join(outDir, relative, "index.html")];
    }

    return [
      join(outDir, relative),
      join(outDir, `${relative}.html`),
      join(outDir, relative, "index.html"),
    ];
  });
}

function resolvesPublicPath(publicPath) {
  const cachedResult = pathResolutionCache.get(publicPath);
  if (cachedResult !== undefined) {
    return cachedResult;
  }

  const resolves = publicPathToCandidates(publicPath).some((candidate) =>
    existsSync(candidate),
  );
  pathResolutionCache.set(publicPath, resolves);
  return resolves;
}

function isExternalOrSpecialReference(reference) {
  const normalizedReference = reference.trim().toLowerCase();
  return (
    normalizedReference === "" ||
    normalizedReference.startsWith("#") ||
    normalizedReference.startsWith("http://") ||
    normalizedReference.startsWith("https://") ||
    normalizedReference.startsWith("mailto:") ||
    normalizedReference.startsWith("tel:") ||
    normalizedReference.startsWith("data:")
  );
}

async function validateRetiredDocsRedirects() {
  for (const [sourcePath, targetPath] of RETIRED_DOCS_REDIRECTS) {
    if (!resolvesPublicPath(targetPath)) {
      fail(`Retired docs redirect target does not resolve: ${targetPath}`);
    }

    const variants = [sourcePath, `${sourcePath}/`];
    for (const variant of variants) {
      const assetFetchCalls = [];
      const env = {
        ASSETS: {
          fetch: async (assetRequest) => {
            assetFetchCalls.push(new URL(assetRequest.url).pathname);
            return new Response("asset 404", { status: 404 });
          },
        },
      };
      const requestUrl = new URL(variant, "https://kitaru.ai");
      requestUrl.search = "?utm_source=validator";
      const response = await worker.fetch(new Request(requestUrl), env);
      const expectedLocation = new URL(targetPath, requestUrl);
      expectedLocation.search = requestUrl.search;

      if (response.status !== RETIRED_DOCS_REDIRECT_STATUS) {
        fail(
          `${variant} returned ${response.status}, expected ${RETIRED_DOCS_REDIRECT_STATUS}`,
        );
      }

      if (response.headers.get("location") !== expectedLocation.href) {
        fail(
          `${variant} redirected to ${response.headers.get("location")}, expected ${expectedLocation.href}`,
        );
      }

      if (assetFetchCalls.length > 0) {
        fail(
          `${variant} reached docs asset serving before redirect: ${assetFetchCalls.join(", ")}`,
        );
      }
    }
  }
}

function validateReference(reference, htmlPath) {
  const normalizedReference = reference.trim().toLowerCase();
  if (normalizedReference.startsWith("javascript:")) {
    fail(`${htmlPath} contains javascript URL reference: ${reference}`);
    return;
  }

  if (isExternalOrSpecialReference(reference)) {
    return;
  }

  if (!reference.startsWith("/")) {
    return;
  }

  if (!reference.startsWith(`${DOCS_PREFIX}/`)) {
    if (
      allowedRootDocsAssetPaths.has(
        new URL(reference, "https://kitaru.ai").pathname,
      )
    ) {
      if (!resolvesPublicPath(reference)) {
        fail(`${htmlPath} references missing root docs asset: ${reference}`);
      }
      return;
    }

    fail(
      `${htmlPath} contains root-relative reference outside /docs: ${reference}`,
    );
    return;
  }

  if (!resolvesPublicPath(reference)) {
    fail(`${htmlPath} references missing docs asset/path: ${reference}`);
  }
}

if (!existsSync(outDir)) {
  fail("docs/out does not exist. Run `just docs-build` first.");
} else {
  for (const publicPath of requiredPublicPaths) {
    if (!resolvesPublicPath(publicPath)) {
      fail(`Required public docs path does not resolve: ${publicPath}`);
    }
  }

  await validateRetiredDocsRedirects();

  for (const htmlPath of walkHtmlFiles(outDir)) {
    const content = readFileSync(htmlPath, "utf8");

    for (const snippet of badOutputSnippets) {
      if (content.includes(snippet)) {
        fail(`${htmlPath} contains invalid output snippet: ${snippet}`);
      }
    }

    const attributePattern = /\b(?:href|src)=["']([^"']+)["']/g;
    for (const match of content.matchAll(attributePattern)) {
      validateReference(match[1], htmlPath);
    }
  }
}

if (failures.length > 0) {
  console.error("Docs static export validation failed:");
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exit(1);
}

console.log("Docs static export validation passed.");
