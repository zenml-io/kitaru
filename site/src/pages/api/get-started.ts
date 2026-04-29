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

async function verifyTurnstile(secret: string, token: string): Promise<boolean> {
  const verifyResponse = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ secret, response: token }),
  });

  if (!verifyResponse.ok) return false;

  const result = (await verifyResponse.json()) as { success?: boolean };
  return result.success === true;
}

export const POST: APIRoute = async ({ request, locals }) => {
  try {
    const data = await request.json();
    const name = String(data.name ?? '').trim();
    const company = String(data.company ?? '').trim();
    const email = String(data.email ?? '').trim().toLowerCase();
    const turnstileToken = String(data.turnstileToken ?? data['cf-turnstile-response'] ?? '').trim();

    if (!name) {
      return jsonResponse({ error: 'Name is required' }, 400);
    }

    if (!company) {
      return jsonResponse({ error: 'Company is required' }, 400);
    }

    if (!email || !EMAIL_RE.test(email)) {
      return jsonResponse({ error: 'Invalid email' }, 400);
    }

    const runtime = (locals as any).runtime;
    const env = runtime?.env;
    const kv = env?.GET_STARTED_KV;

    if (!kv) {
      return jsonResponse({ error: 'KV not configured' }, 500);
    }

    // Turnstile is enforced whenever the private secret is configured.
    // Deployments must also provide PUBLIC_TURNSTILE_SITE_KEY at site build time
    // so the browser can render the widget that generates this token.
    const turnstileSecret = env?.TURNSTILE_SECRET_KEY;
    if (turnstileSecret) {
      if (!turnstileToken) {
        return jsonResponse({ error: 'Bot verification is required' }, 403);
      }

      const verified = await verifyTurnstile(turnstileSecret, turnstileToken);
      if (!verified) {
        return jsonResponse({ error: 'Bot verification failed. Please try again.' }, 403);
      }
    }

    await kv.put(email, JSON.stringify({
      name,
      company,
      email,
      timestamp: new Date().toISOString(),
      source: 'book-a-demo',
      formType: FORM_TYPES.DEMO_REQUEST,
      product: 'kitaru',
      nextStep: 'cal-inline-booking',
    }));

    // Send identify + track to Segment server-side (fire-and-forget)
    const referer = request.headers.get('referer') ?? '';
    const userAgent = request.headers.get('user-agent') ?? '';
    const segmentContext = { page: { url: referer }, userAgent };

    const identifyCall = segmentCall('identify', {
      userId: email,
      traits: { name, company, email },
      context: segmentContext,
    });
    const trackCall = segmentCall('track', {
      userId: email,
      event: 'Book a Demo Signup',
      properties: {
        name,
        company,
        email,
        source: 'book-a-demo',
        formType: FORM_TYPES.DEMO_REQUEST,
        product: 'kitaru',
        nextStep: 'cal-inline-booking',
      },
      context: segmentContext,
    });

    const analyticsCalls = Promise.all([identifyCall, trackCall]).catch((analyticsError) => {
      console.error('[segment] book-a-demo analytics failed', analyticsError);
    });
    if (runtime?.ctx?.waitUntil) {
      runtime.ctx.waitUntil(analyticsCalls);
    } else {
      await analyticsCalls;
    }

    return jsonResponse({ ok: true, nextStep: 'calendar' });
  } catch (err) {
    return jsonResponse({ error: 'Server error' }, 500);
  }
};
