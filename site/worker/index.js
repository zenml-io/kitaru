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
    const response = await negotiatedMarkdownResponse(
      request,
      env,
      url.pathname,
    );
    if (response) return response;

    const delegatedResponse = await astroWorker.fetch(request, env, context);

    return isDocsPath(url.pathname)
      ? responseWithAcceptVary(delegatedResponse)
      : delegatedResponse;
  },
};
