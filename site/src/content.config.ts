import { defineCollection, z } from 'astro:content';
import { glob } from 'astro/loaders';

const blog = defineCollection({
  loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/blog' }),
  schema: z.object({
    title: z.string(),
    description: z.string(),
    date: z.coerce.date(),
    author: z.string(),
    draft: z.boolean().default(false),
    category: z
      .enum(['Agents', 'Infrastructure', 'Design', 'Philosophy'])
      .default('Agents'),
    ogImage: z.string().optional(),
  }),
});

export const collections = { blog };
