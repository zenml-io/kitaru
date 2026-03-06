import { source } from '@/lib/source';
import type { MetadataRoute } from 'next';

export const revalidate = false;

const baseUrl = 'https://docs.kitaru.ai';

export default function sitemap(): MetadataRoute.Sitemap {
  return [
    { url: baseUrl, lastModified: new Date() },
    ...source.getPages().map((page) => ({
      url: `${baseUrl}${page.url}`,
      lastModified: new Date(),
    })),
  ];
}
