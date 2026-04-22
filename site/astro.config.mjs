import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import sitemap from '@astrojs/sitemap';
import mdx from '@astrojs/mdx';
import cloudflare from '@astrojs/cloudflare';
import kitaruLight from './src/styles/kitaru-light.json';

export default defineConfig({
  site: 'https://kitaru.ai',
  trailingSlash: 'never',
  redirects: {
    '/banner': { status: 302, destination: '/' },
    '/onepager': { status: 302, destination: '/' },
    '/roadmap': { status: 302, destination: 'https://github.com/orgs/zenml-io/projects/5' },
    '/community': { status: 302, destination: 'https://github.com/zenml-io/kitaru/discussions' },
  },
  integrations: [sitemap(), mdx()],
  markdown: {
    shikiConfig: {
      theme: kitaruLight,
    },
  },
  vite: {
    plugins: [tailwindcss()]
  },
  adapter: cloudflare()
});
