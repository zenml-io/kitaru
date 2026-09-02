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
// One exception serves content: kitaru.ai/install (and /install.sh) proxies
// install.sh from the repository's main branch so `curl -fsSL
// https://kitaru.ai/install | bash` works without a redirect hop.
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
const INSTALL_SCRIPT_URL =
  "https://raw.githubusercontent.com/zenml-io/kitaru/main/install.sh";
const INSTALL_PATHS = new Set(["/install", "/install.sh"]);

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
 * Serve install.sh from GitHub main as text/plain, cached at the edge for
 * five minutes so a release lands quickly without hammering GitHub.
 */
export async function serveInstallScript(request) {
  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response("Method not allowed", { status: 405 });
  }
  const upstream = await fetch(INSTALL_SCRIPT_URL, {
    cf: { cacheTtl: 300, cacheEverything: true },
  });
  if (!upstream.ok) {
    return new Response(
      "install.sh is temporarily unavailable. See https://docs.zenml.io/kitaru/getting-started/setup\n",
      { status: 502, headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  }
  return new Response(request.method === "HEAD" ? null : upstream.body, {
    status: 200,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "public, max-age=300",
      "x-content-type-options": "nosniff",
    },
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const { pathname } = url;

    if (INSTALL_PATHS.has(normalizePath(pathname))) {
      return serveInstallScript(request);
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
