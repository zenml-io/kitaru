// Redirect-only worker for the kitaru.ai domain.
//
// The Kitaru docs moved to GitBook (docs.zenml.io/kitaru) and the generated
// SDK/CLI reference to sdkdocs.kitaru.ai, so this worker no longer serves any
// content — it 301-redirects old kitaru.ai URLs to their new homes:
//
//   kitaru.ai/docs/cli...              -> sdkdocs.kitaru.ai/cli...
//   kitaru.ai/docs/reference/python... -> sdkdocs.kitaru.ai/reference/python...
//   kitaru.ai/docs/changelog           -> docs.zenml.io/changelog
//   kitaru.ai/docs/<anything else>     -> docs.zenml.io/kitaru/<anything else>
//   kitaru.ai/ , /pricing, /blog/* ... -> www.zenml.io (marketing)
//
// Two exceptions serve content from the repository's main branch: /install
// (and /install.sh) is install.sh, so `curl -fsSL https://kitaru.ai/install |
// bash` works without a redirect hop, and /install.md is the installation
// docs page as Markdown for coding agents.
import {
  DOCS_PREFIX,
  LEGACY_MARKETING_PREFIX_REDIRECTS,
  LEGACY_MARKETING_REDIRECTS,
  RETIRED_DOCS_REDIRECTS,
  ZENML_BASE_URL,
} from "./docs-routing.mjs";

const GITBOOK_BASE = "https://docs.zenml.io/kitaru";
const SDKDOCS_BASE = "https://sdkdocs.kitaru.ai";
const CHANGELOG_URL = "https://docs.zenml.io/changelog";
const RAW_MAIN = "https://raw.githubusercontent.com/zenml-io/kitaru/main";
// kitaru.ai/install serves the installer script; kitaru.ai/install.md serves
// the installation page as plain Markdown so a coding agent can be told to
// "follow https://kitaru.ai/install.md" without hitting GitBook's HTML.
const INSTALL_FILES = new Map([
  ["/install", { url: `${RAW_MAIN}/install.sh`, type: "text/plain" }],
  ["/install.sh", { url: `${RAW_MAIN}/install.sh`, type: "text/plain" }],
  [
    "/install.md",
    {
      url: `${RAW_MAIN}/docs/book/getting-started/installation.md`,
      type: "text/markdown",
    },
  ],
]);

const retiredDocsRedirects = new Map(RETIRED_DOCS_REDIRECTS);
const marketingRedirects = new Map(LEGACY_MARKETING_REDIRECTS);

function normalizePath(pathname) {
  return pathname.replace(/\/+$/, "") || "/";
}

function withPreservedQuery(target, sourceUrl) {
  const targetUrl = new URL(target);
  if (sourceUrl.search) targetUrl.search = sourceUrl.search;
  return targetUrl.href;
}

export function docsRedirectTarget(pathname) {
  // Resolve any retired in-docs redirect first (its value is also /docs-rooted).
  const resolved =
    retiredDocsRedirects.get(normalizePath(pathname)) ?? pathname;
  const rest = resolved.slice(DOCS_PREFIX.length); // "" or "/concepts/flows" ...

  // Segment-aware matching so e.g. /docs/clipboard does not match /cli and
  // /docs/changelog-old does not match /changelog.
  const isUnder = (prefix) => rest === prefix || rest.startsWith(`${prefix}/`);

  if (isUnder("/changelog")) {
    return CHANGELOG_URL;
  }
  if (isUnder("/cli") || isUnder("/reference")) {
    return `${SDKDOCS_BASE}${rest}`;
  }
  return `${GITBOOK_BASE}${rest}`;
}

function marketingRedirectTarget(pathname) {
  const norm = normalizePath(pathname);
  const exact = marketingRedirects.get(norm);
  if (exact) return exact;
  const prefix = LEGACY_MARKETING_PREFIX_REDIRECTS.find(
    (p) => norm === p || norm.startsWith(`${p}/`),
  );
  return prefix ? `${ZENML_BASE_URL}${norm}` : null;
}

/**
 * Serve one file from GitHub main, cached at the edge for five minutes so a
 * release lands quickly without hammering GitHub.
 */
export async function serveInstallFile(request, { url, type }) {
  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response("Method not allowed", { status: 405 });
  }
  const unavailable = () =>
    new Response(
      "This file is temporarily unavailable. See https://docs.zenml.io/kitaru/getting-started/installation\n",
      { status: 502, headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  let upstream;
  try {
    upstream = await fetch(url, {
      cf: { cacheTtl: 300, cacheEverything: true },
    });
  } catch {
    // DNS, connection, and timeout failures reject rather than resolve.
    return unavailable();
  }
  if (!upstream.ok) {
    return unavailable();
  }
  return new Response(request.method === "HEAD" ? null : upstream.body, {
    status: 200,
    headers: {
      "content-type": `${type}; charset=utf-8`,
      "cache-control": "public, max-age=300",
      "x-content-type-options": "nosniff",
    },
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const { pathname } = url;

    const installFile = INSTALL_FILES.get(normalizePath(pathname));
    if (installFile) {
      return serveInstallFile(request, installFile);
    }

    if (
      pathname === DOCS_PREFIX ||
      pathname.startsWith(`${DOCS_PREFIX}/`) ||
      pathname.startsWith(`${DOCS_PREFIX}.`)
    ) {
      return Response.redirect(
        withPreservedQuery(docsRedirectTarget(pathname), url),
        301,
      );
    }

    const marketing = marketingRedirectTarget(pathname);
    if (marketing) {
      return Response.redirect(withPreservedQuery(marketing, url), 301);
    }

    // Fallback: anything else on kitaru.ai goes to the product page.
    return Response.redirect(`${ZENML_BASE_URL}/product/kitaru`, 301);
  },
};
