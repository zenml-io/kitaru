import type { APIRoute } from 'astro';
import { FORM_TYPES } from '../../lib/formTypes';
import { segmentCall } from '../../lib/segment';

export const prerender = false;

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

export const POST: APIRoute = async ({ request, locals }) => {
  try {
    const data = await request.json();
    const email = String(data.email ?? '').trim().toLowerCase();

    if (!email || !EMAIL_RE.test(email)) {
      return jsonResponse({ error: 'Invalid email' }, 400);
    }

    const runtime = (locals as { runtime?: { env?: Record<string, unknown>; ctx?: { waitUntil?: (p: Promise<unknown>) => void } } }).runtime;
    const env = runtime?.env;
    const kv = env?.NEWSLETTER_KV as
      | { get: (k: string) => Promise<string | null>; put: (k: string, v: string) => Promise<void> }
      | undefined;

    if (!kv) {
      return jsonResponse({ error: 'KV not configured' }, 500);
    }

    // Skip Segment re-fire on duplicate so signup counts stay clean.
    const existing = await kv.get(email);
    if (existing) {
      return jsonResponse({ ok: true, alreadySubscribed: true });
    }

    await kv.put(email, JSON.stringify({
      email,
      timestamp: new Date().toISOString(),
      source: 'newsletter',
      formType: FORM_TYPES.NEWSLETTER,
    }));

    const referer = request.headers.get('referer') ?? '';
    const userAgent = request.headers.get('user-agent') ?? '';
    const segmentContext = { page: { url: referer }, userAgent };

    const identifyCall = segmentCall('identify', {
      userId: email,
      traits: { email },
      context: segmentContext,
    });
    const trackCall = segmentCall('track', {
      userId: email,
      event: 'Newsletter Signup',
      properties: {
        email,
        source: 'newsletter',
        formType: FORM_TYPES.NEWSLETTER,
      },
      context: segmentContext,
    });

    const analyticsCalls = Promise.all([identifyCall, trackCall]).catch((analyticsError) => {
      console.error('[segment] newsletter analytics failed', analyticsError);
    });
    if (runtime?.ctx?.waitUntil) {
      runtime.ctx.waitUntil(analyticsCalls);
    } else {
      await analyticsCalls;
    }

    return jsonResponse({ ok: true, alreadySubscribed: false });
  } catch {
    return jsonResponse({ error: 'Server error' }, 500);
  }
};
