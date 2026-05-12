import astroWorker from "../dist/_worker.js/index.js";

function acceptsMarkdown(acceptHeader) {
  if (!acceptHeader) return false;

  return acceptHeader.split(",").some((entry) => {
    const [mediaType, ...params] = entry
      .split(";")
      .map((part) => part.trim().toLowerCase());
    if (mediaType !== "text/markdown") return false;

    const q = params.find((param) => param.startsWith("q="));
    return q ? Number.parseFloat(q.slice(2)) > 0 : true;
  });
}


const PERMANENT_REDIRECTS = new Map([
  ["/get-started", "/book-a-demo/"],
  ["/get-started/", "/book-a-demo/"],
  ["/docs/concepts/memory", "/docs/concepts/checkpoints/"],
  ["/docs/concepts/memory/", "/docs/concepts/checkpoints/"],
  ["/docs/guides/memory", "/docs/guides/artifacts/"],
  ["/docs/guides/memory/", "/docs/guides/artifacts/"],
  ["/blog/kitaru-agents-now-have-memory", "/blog/"],
  ["/blog/kitaru-agents-now-have-memory/", "/blog/"],
]);

function isFileLikeOrApiPath(pathname) {
  if (pathname.startsWith("/api/")) return true;

  const segments = pathname.split("/").filter(Boolean);
  const lastSegment = segments.at(-1) ?? "";
  return /\.[^/]+$/.test(lastSegment);
}

function acceptsHtml(acceptHeader) {
  if (!acceptHeader) return true;

  return acceptHeader.split(",").some((entry) => {
    const [mediaType, ...params] = entry
      .split(";")
      .map((part) => part.trim().toLowerCase());
    if (mediaType !== "text/html" && mediaType !== "*/*") return false;

    const q = params.find((param) => param.startsWith("q="));
    return q ? Number.parseFloat(q.slice(2)) > 0 : true;
  });
}

function redirectResponse(request, destination, status) {
  const sourceUrl = new URL(request.url);
  const url = new URL(destination, sourceUrl);
  if (!url.search && sourceUrl.search) {
    url.search = sourceUrl.search;
  }

  return Response.redirect(url.href, status);
}

function permanentRedirectResponse(request, pathname) {
  const destination = PERMANENT_REDIRECTS.get(pathname);
  return destination ? redirectResponse(request, destination, 301) : undefined;
}

function canonicalSlashRedirectResponse(request, pathname) {
  if (request.method !== "GET" && request.method !== "HEAD") return undefined;
  if (pathname === "/" || pathname.endsWith("/")) return undefined;
  if (isFileLikeOrApiPath(pathname)) return undefined;
  if (acceptsMarkdown(request.headers.get("accept"))) return undefined;
  if (!acceptsHtml(request.headers.get("accept"))) return undefined;

  return redirectResponse(request, `${pathname}/`, 308);
}

function isDocsPath(pathname) {
  return pathname === "/docs" || pathname.startsWith("/docs/");
}

function markdownPathname(pathname) {
  const withoutTrailingSlash = pathname.replace(/\/+$/, "");

  if (withoutTrailingSlash === "/docs") {
    return "/docs/index.md";
  }

  if (withoutTrailingSlash.endsWith(".html")) {
    return `${withoutTrailingSlash.slice(0, -".html".length)}.md`;
  }

  if (/\.[^/]+$/.test(withoutTrailingSlash)) {
    return undefined;
  }

  return `${withoutTrailingSlash}.md`;
}

function markdownAssetRequest(sourceRequest, pathname) {
  const url = new URL(sourceRequest.url);
  url.pathname = pathname;

  return new Request(url, sourceRequest);
}

function astroDispatchRequest(sourceRequest, pathname) {
  if (!pathname.startsWith("/api/") || pathname.endsWith("/")) {
    return sourceRequest;
  }

  const url = new URL(sourceRequest.url);
  url.pathname = `${pathname}/`;

  return new Request(url, sourceRequest);
}

function addVaryAccept(headers) {
  const vary = headers.get("Vary");
  if (!vary) {
    headers.set("Vary", "Accept");
    return;
  }

  const values = vary.split(",").map((value) => value.trim().toLowerCase());
  if (!values.includes("*") && !values.includes("accept")) {
    headers.set("Vary", `${vary}, Accept`);
  }
}

function responseWithAcceptVary(response, contentType) {
  const headers = new Headers(response.headers);
  if (contentType) {
    headers.set("Content-Type", contentType);
  }
  addVaryAccept(headers);

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

async function negotiatedMarkdownResponse(request, env, pathname) {
  if (!isDocsPath(pathname)) return undefined;
  if (!acceptsMarkdown(request.headers.get("accept"))) return undefined;

  const markdownPath = markdownPathname(pathname);
  if (!markdownPath) return undefined;

  const response = await env.ASSETS.fetch(
    markdownAssetRequest(request, markdownPath),
  );
  return response.ok
    ? responseWithAcceptVary(response, "text/markdown; charset=utf-8")
    : undefined;
}

export default {
  async fetch(request, env, context) {
    const url = new URL(request.url);
    const permanentRedirect = permanentRedirectResponse(request, url.pathname);
    if (permanentRedirect) return permanentRedirect;

    const canonicalRedirect = canonicalSlashRedirectResponse(request, url.pathname);
    if (canonicalRedirect) return canonicalRedirect;

    const response = await negotiatedMarkdownResponse(
      request,
      env,
      url.pathname,
    );
    if (response) return response;

    const delegatedResponse = await astroWorker.fetch(
      astroDispatchRequest(request, url.pathname),
      env,
      context,
    );

    return isDocsPath(url.pathname)
      ? responseWithAcceptVary(delegatedResponse)
      : delegatedResponse;
  },
};
