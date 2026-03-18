/**
 * Blog utilities — reading time, author data, category styling.
 */

/** Canonical base URL for R2-hosted content assets. */
export const ASSET_BASE_URL = "https://assets.kitaru.ai";

/** Compute reading time from raw text (250 wpm average). */
export function readingTime(text: string): string {
  const words = text.trim().split(/\s+/).length;
  const minutes = Math.max(1, Math.round(words / 250));
  return `${minutes} min read`;
}

/** Author metadata keyed by display name. */
export const AUTHORS: Record<string, { initials: string; color: string; image?: string }> = {
  'Hamza Tahir': { initials: 'HT', color: '#F17829', image: 'https://assets.zenml.io/webflow/64a817a2e7e2208272d1ce30/a8ce9c50/652f8dcd929fdbade2b3639a_hamza.png' },
  'Alex Strick van Linschoten': { initials: 'AS', color: '#A07848' },
};

/** Category → pill color mapping. */
export const CATEGORY_STYLES: Record<
  string,
  { bg: string; color: string }
> = {
  Agents: { bg: 'rgba(30,80,50,0.1)', color: '#1E5032' },
  Infrastructure: { bg: 'rgba(241,120,41,0.1)', color: '#C45A1A' },
  Design: { bg: 'rgba(61,140,92,0.1)', color: '#3D8C5C' },
  Philosophy: { bg: 'rgba(164,105,72,0.12)', color: '#A07848' },
  Kitaru: { bg: 'rgba(241,120,41,0.14)', color: '#B84D0E' },
};
