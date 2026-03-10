import type { APIRoute } from 'astro';

export const prerender = false;

/** Send a single call to Segment's HTTP Tracking API. */
async function segmentCall(
  endpoint: 'identify' | 'track',
  writeKey: string,
  body: Record<string, unknown>,
): Promise<void> {
  const resp = await fetch(`https://api.segment.io/v1/${endpoint}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Basic ${btoa(writeKey + ':')}`,
    },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const text = await resp.text();
    console.error(`[segment:${endpoint}] ${resp.status}: ${text}`);
  }
}

export const POST: APIRoute = async ({ request, locals }) => {
  try {
    const data = await request.json();
    const email = data.email?.trim().toLowerCase();

    if (!email || !email.includes('@')) {
      return new Response(JSON.stringify({ error: 'Invalid email' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const runtime = (locals as any).runtime;
    const env = runtime?.env;
    const kv = env?.WAITLIST_KV;

    if (!kv) {
      return new Response(JSON.stringify({ error: 'KV not configured' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    await kv.put(email, JSON.stringify({
      email,
      timestamp: new Date().toISOString(),
      source: 'landing-page',
    }));

    // Send identify + track to Segment server-side (fire-and-forget)
    const segmentKey = env?.SEGMENT_WRITE_KEY;
    if (segmentKey) {
      const referer = request.headers.get('referer') ?? '';
      const userAgent = request.headers.get('user-agent') ?? '';
      const segmentContext = { page: { url: referer }, userAgent };

      const identifyCall = segmentCall('identify', segmentKey, {
        userId: email,
        traits: { email },
        context: segmentContext,
      });
      const trackCall = segmentCall('track', segmentKey, {
        userId: email,
        event: 'Waitlist Signup',
        properties: {
          email,
          source: 'landing-page',
          formType: 'waitlist',
        },
        context: segmentContext,
      });

      runtime.ctx.waitUntil(Promise.all([identifyCall, trackCall]));
    }

    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Server error' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
