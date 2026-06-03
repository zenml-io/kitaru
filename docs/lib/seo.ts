import { RETIRED_DOCS_REDIRECTS } from "../worker/docs-routing.mjs";

const SITE_ORIGIN = "https://kitaru.ai";
const DOCS_BASE_PATH = "/docs";

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
  const pathWithSlash = ensureLeadingSlash(inputPath);
  const docsPath =
    pathWithSlash === DOCS_BASE_PATH ||
    pathWithSlash.startsWith(`${DOCS_BASE_PATH}/`)
      ? pathWithSlash
      : `${DOCS_BASE_PATH}${pathWithSlash}`;

  return ensureTrailingSlashForHtml(docsPath.replace(/\/+/g, "/"));
}

export function canonicalDocsUrl(pageUrl: string): string {
  return `${SITE_ORIGIN}${canonicalDocsPath(pageUrl)}`;
}

const retiredRedirectedDocsPaths = new Set(
  RETIRED_DOCS_REDIRECTS.map(([sourcePath]) => canonicalDocsPath(sourcePath)),
);

export function isRetiredRedirectedDocsPath(pageUrl: string): boolean {
  return retiredRedirectedDocsPaths.has(canonicalDocsPath(pageUrl));
}
