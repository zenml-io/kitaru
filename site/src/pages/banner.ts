import type { APIRoute } from 'astro';
import { BANNER_REDIRECT_TARGET } from '../lib/redirects';

export const prerender = false;

export const GET: APIRoute = ({ url }) => {
  // Resolve relative targets against the request origin
  const target = new URL(BANNER_REDIRECT_TARGET, url.origin).toString();
  return new Response(null, {
    status: 302,
    headers: { Location: target },
  });
};
