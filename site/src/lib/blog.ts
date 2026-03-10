/**
 * Blog utilities — reading time, author data, category styling.
 */

/** Compute reading time from raw text (250 wpm average). */
export function readingTime(text: string): string {
  const words = text.trim().split(/\s+/).length;
  const minutes = Math.max(1, Math.round(words / 250));
  return `${minutes} min read`;
}

/** Author metadata keyed by display name. */
export const AUTHORS: Record<string, { initials: string; color: string }> = {
  'Hamza Tahir': { initials: 'HT', color: '#F17829' },
  'Alex Strick van Linschoten': { initials: 'AS', color: '#A07848' },
};

/** Category → pill color mapping. */
export const CATEGORY_STYLES: Record<
  string,
  { bg: string; color: string }
> = {
  Agents: { bg: 'rgba(232,168,64,0.08)', color: '#E8A840' },
  Infrastructure: { bg: 'rgba(241,120,41,0.1)', color: '#C45A1A' },
  Design: { bg: 'rgba(61,140,92,0.1)', color: '#3D8C5C' },
  Philosophy: { bg: 'rgba(164,105,72,0.12)', color: '#A07848' },
};
