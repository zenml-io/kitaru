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

function docsRedirectTarget(pathname) {
  // Resolve any retired in-docs redirect first (its value is also /docs-rooted).
  const resolved = retiredDocsRedirects.get(normalizePath(pathname)) ?? pathname;
  const rest = resolved.slice(DOCS_PREFIX.length); // "" or "/concepts/flows" ...

  if (rest === "/changelog" || rest.startsWith("/changelog")) {
    return CHANGELOG_URL;
  }
  if (rest.startsWith("/cli") || rest.startsWith("/reference/python")) {
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

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const { pathname } = url;

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
