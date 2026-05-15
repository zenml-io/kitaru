import { source } from '@/lib/source';
import { canonicalDocsUrl, isRetiredRedirectedDocsPath } from '@/lib/seo';
import type { MetadataRoute } from 'next';

export const revalidate = false;


export default function sitemap(): MetadataRoute.Sitemap {
  return source
    .getPages()
    .filter((page) => !isRetiredRedirectedDocsPath(page.url))
    .map((page) => ({
      url: canonicalDocsUrl(page.url),
      lastModified: new Date(),
    }));
}
