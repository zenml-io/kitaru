import {
  DOCS_PREFIX,
  LEGACY_MARKETING_PREFIX_REDIRECTS,
  LEGACY_MARKETING_REDIRECTS,
  RETIRED_DOCS_REDIRECT_STATUS,
  RETIRED_DOCS_REDIRECTS,
  ROOT_DOCS_ASSET_PATHS,
  ZENML_BASE_URL,
} from "./docs-routing.mjs";

const rootDocsAssetPaths = new Set(ROOT_DOCS_ASSET_PATHS);
const retiredDocsRedirects = new Map(RETIRED_DOCS_REDIRECTS);

// The marketing app now lives in zenml-io-v2. These legacy redirects only keep
// old kitaru.ai public URLs useful while this Worker continues to front the
// custom domain for kitaru.ai/docs.
const legacyMarketingRedirects = new Map(LEGACY_MARKETING_REDIRECTS);

function withPreservedQuery(target, sourceUrl) {
  const targetUrl = new URL(target);
  if (sourceUrl.search) {
    targetUrl.search = sourceUrl.search;
  }
  return targetUrl.href;
}

function normalizePath(pathname) {
  return pathname.replace(/\/+$/, "") || "/";
}

function getRetiredDocsRedirectTarget(pathname) {
  return retiredDocsRedirects.get(normalizePath(pathname)) ?? null;
}

function getMarketingRedirectTarget(pathname) {
  const normalizedPath = normalizePath(pathname);
  const exactRedirect = legacyMarketingRedirects.get(normalizedPath);
  if (exactRedirect) {
    return exactRedirect;
  }

  const matchingPrefix = LEGACY_MARKETING_PREFIX_REDIRECTS.find(
    (prefix) =>
      normalizedPath === prefix || normalizedPath.startsWith(`${prefix}/`),
  );
  if (matchingPrefix) {
    return `${ZENML_BASE_URL}${normalizedPath}`;
  }

  return null;
}

function assetRequestForPath(request, pathname) {
  const url = new URL(request.url);
  url.pathname = pathname;
  return new Request(url, request);
}

function acceptsMarkdown(request) {
  return request.headers.get("accept")?.toLowerCase().includes("text/markdown");
}

function markdownAssetPathFor(strippedPathname) {
  if (strippedPathname.endsWith(".md")) {
    return strippedPathname;
  }

  const normalizedPathname = strippedPathname.replace(/\/+$/, "") || "/index";
  return `${normalizedPathname}.md`;
}

async function fetchDocsAsset(request, env) {
  const url = new URL(request.url);

  if (url.pathname.startsWith(`${DOCS_PREFIX}/`)) {
    const strippedPathname = url.pathname.slice(DOCS_PREFIX.length) || "/";

    if (acceptsMarkdown(request)) {
      const markdownResponse = await env.ASSETS.fetch(
        assetRequestForPath(request, markdownAssetPathFor(strippedPathname)),
      );
      if (markdownResponse.status !== 404) {
        return markdownResponse;
      }
    }

    const strippedResponse = await env.ASSETS.fetch(
      assetRequestForPath(request, strippedPathname),
    );
    if (strippedResponse.status !== 404) {
      return strippedResponse;
    }
  }

  return env.ASSETS.fetch(request);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === DOCS_PREFIX) {
      url.pathname = `${DOCS_PREFIX}/`;
      return Response.redirect(url.href, 308);
    }

    const retiredDocsRedirectTarget = getRetiredDocsRedirectTarget(
      url.pathname,
    );
    if (retiredDocsRedirectTarget) {
      url.pathname = retiredDocsRedirectTarget;
      return Response.redirect(url.href, RETIRED_DOCS_REDIRECT_STATUS);
    }

    if (
      url.pathname.startsWith(`${DOCS_PREFIX}/`) ||
      rootDocsAssetPaths.has(url.pathname)
    ) {
      return fetchDocsAsset(request, env);
    }

    if (url.pathname.startsWith("/api/")) {
      return new Response("Not found", { status: 404 });
    }

    const redirectTarget = getMarketingRedirectTarget(url.pathname);
    if (redirectTarget) {
      return Response.redirect(withPreservedQuery(redirectTarget, url), 301);
    }

    return new Response("Not found", { status: 404 });
  },
};
