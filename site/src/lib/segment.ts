export const SEGMENT_WRITE_KEY = 'MMarT0XoV4LJH8wR7wpmkTbF7txc9Bsg';

/** Send a single call to Segment's HTTP Tracking API. */
export async function segmentCall(
  endpoint: 'identify' | 'track',
  body: Record<string, unknown>,
): Promise<void> {
  const resp = await fetch(`https://api.segment.io/v1/${endpoint}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Basic ${btoa(SEGMENT_WRITE_KEY + ':')}`,
    },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const text = await resp.text();
    console.error(`[segment:${endpoint}] ${resp.status}: ${text}`);
  }
}
