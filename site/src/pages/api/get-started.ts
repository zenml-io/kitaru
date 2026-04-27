import type { APIRoute } from 'astro';
import { segmentCall } from '../../lib/segment';

export const prerender = false;

export const POST: APIRoute = async ({ request, locals }) => {
  try {
    const data = await request.json();
    const name = data.name?.trim();
    const company = data.company?.trim();
    const email = data.email?.trim().toLowerCase();

    if (!name) {
      return new Response(JSON.stringify({ error: 'Name is required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (!company) {
      return new Response(JSON.stringify({ error: 'Company is required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return new Response(JSON.stringify({ error: 'Invalid email' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const runtime = (locals as any).runtime;
    const env = runtime?.env;
    const kv = env?.GET_STARTED_KV;

    if (!kv) {
      return new Response(JSON.stringify({ error: 'KV not configured' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    await kv.put(email, JSON.stringify({
      name,
      company,
      email,
      timestamp: new Date().toISOString(),
      source: 'book-a-demo',
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
        formType: 'book-a-demo',
      },
      context: segmentContext,
    });

    runtime.ctx.waitUntil(Promise.all([identifyCall, trackCall]));

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
