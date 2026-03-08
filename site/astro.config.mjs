import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import sitemap from '@astrojs/sitemap';
import mdx from '@astrojs/mdx';
import cloudflare from '@astrojs/cloudflare';

export default defineConfig({
  site: 'https://kitaru.ai',
  trailingSlash: 'never',
  integrations: [sitemap(), mdx()],
  vite: {
    plugins: [tailwindcss()]
  },
  adapter: cloudflare()
});
