import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import sitemap from '@astrojs/sitemap';
import mdx from '@astrojs/mdx';
import cloudflare from '@astrojs/cloudflare';
import kitaruLight from './src/styles/kitaru-light.json';
import kitaruDark from './src/styles/kitaru-dark.json';

export default defineConfig({
  site: 'https://kitaru.ai',
  trailingSlash: 'always',
  redirects: {
    '/banner': { status: 302, destination: '/' },
    '/onepager': { status: 302, destination: '/' },
    '/roadmap': { status: 302, destination: 'https://github.com/orgs/zenml-io/projects/5' },
    '/community': { status: 302, destination: 'https://github.com/zenml-io/kitaru/discussions' },
    '/get-started': { status: 301, destination: '/book-a-demo/' },
    '/docs/concepts/memory': { status: 301, destination: '/docs/concepts/checkpoints/' },
    '/docs/guides/memory': { status: 301, destination: '/docs/guides/artifacts/' },
    '/blog/kitaru-agents-now-have-memory': { status: 301, destination: '/blog/' },
  },
  integrations: [sitemap(), mdx()],
  markdown: {
    shikiConfig: {
      themes: {
        light: kitaruLight,
        dark: kitaruDark,
      },
      defaultColor: 'light',
    },
  },
  vite: {
    plugins: [tailwindcss()]
  },
  adapter: cloudflare()
});
