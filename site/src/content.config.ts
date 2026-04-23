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
      .enum(['Agents', 'Infrastructure', 'Design', 'Philosophy', 'Kitaru'])
      .default('Agents'),
    ogImage: z.string().url().optional(),
    image: z.string().url().optional(),
  }),
});

const comparisons = defineCollection({
  loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/comparisons' }),
  schema: z.object({
    competitor: z.string(),
    competitorLogo: z.string().optional(),
    competitorTagline: z.string(),
    title: z.string(),
    shortTitle: z.string().optional(),
    description: z.string(),
    cardSubtitle: z.string(),
    ctaHeading: z.string().default('Ready to try Kitaru?'),
    order: z.number().default(100),
    draft: z.boolean().default(false),
    ogImage: z.string().url().optional(),
  }),
});

export const collections = { blog, comparisons };
