// SEO helpers for the standalone SDK/CLI reference site at sdkdocs.kitaru.ai.
// This site serves at the domain root (no /docs basePath) and has no retired
// redirects of its own.
const SITE_ORIGIN = "https://sdkdocs.kitaru.ai";

function hasFileExtension(pathname: string): boolean {
  const lastSegment = pathname.split("/").pop() ?? "";
  return /\.[^/]+$/.test(lastSegment);
}

function ensureLeadingSlash(pathname: string): string {
  return pathname.startsWith("/") ? pathname : `/${pathname}`;
}

function ensureTrailingSlashForHtml(pathname: string): string {
  if (pathname.endsWith("/") || hasFileExtension(pathname)) return pathname;
  return `${pathname}/`;
}

export function canonicalDocsPath(pageUrl: string): string {
  const inputPath = new URL(pageUrl, SITE_ORIGIN).pathname;
  return ensureTrailingSlashForHtml(
    ensureLeadingSlash(inputPath).replace(/\/+/g, "/"),
  );
}

export function canonicalDocsUrl(pageUrl: string): string {
  return `${SITE_ORIGIN}${canonicalDocsPath(pageUrl)}`;
}

export function isRetiredRedirectedDocsPath(_pageUrl: string): boolean {
  return false;
}
