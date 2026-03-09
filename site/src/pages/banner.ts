import type { APIRoute } from 'astro';
import { BANNER_REDIRECT_TARGET } from '../lib/redirects';

export const prerender = false;

export const GET: APIRoute = ({ url }) => {
  const target = BANNER_REDIRECT_TARGET.startsWith('http')
    ? BANNER_REDIRECT_TARGET
    : new URL(BANNER_REDIRECT_TARGET, url.origin).toString();
  return new Response(null, {
    status: 302,
    headers: { Location: target },
  });
};
