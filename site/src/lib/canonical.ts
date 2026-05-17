const SITE_ORIGIN = 'https://kitaru.ai';

/**
 * Return true for paths that should keep their exact URL shape.
 *
 * HTML pages get a trailing slash, but assets, markdown negotiation, and API
 * routes must not be rewritten to directory-style URLs.
 */
export function isFileLikeOrApiPath(pathname: string): boolean {
  if (pathname.startsWith('/api/')) return true;

  const lastSegment = pathname.split('/').filter(Boolean).at(-1) ?? '';
  return /\.[^/]+$/.test(lastSegment);
}

/** Canonicalize a same-site pathname without changing file-like/API paths. */
export function canonicalSitePath(pathname: string): string {
  const parsed = new URL(pathname, SITE_ORIGIN);
  const { pathname: parsedPathname } = parsed;

  if (parsedPathname === '/' || isFileLikeOrApiPath(parsedPathname)) {
    return parsedPathname;
  }

  return parsedPathname.endsWith('/') ? parsedPathname : `${parsedPathname}/`;
}

/** Build the absolute canonical URL for a same-site HTML or asset path. */
export function canonicalSiteUrl(pathname: string, site: URL | string = SITE_ORIGIN): string {
  return new URL(canonicalSitePath(pathname), site).href;
}
