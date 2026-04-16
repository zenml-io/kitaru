import { source } from '@/lib/source';
import type { MetadataRoute } from 'next';

export const revalidate = false;

const baseUrl = 'https://kitaru.ai';

export default function sitemap(): MetadataRoute.Sitemap {
  return source.getPages().map((page) => ({
    url: `${baseUrl}/docs${page.url}`,
    lastModified: new Date(),
  }));
}
