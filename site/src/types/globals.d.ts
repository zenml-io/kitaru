/** Segment Analytics (loaded via snippet in Base.astro) */
interface SegmentAnalytics {
  track(event: string, properties?: Record<string, unknown>): void;
  identify(userId: string, traits?: Record<string, unknown>): void;
  page(): void;
  load(writeKey: string, options?: Record<string, unknown>): void;
  initialized?: boolean;
  invoked?: boolean;
  push(args: unknown[]): void;
  [key: string]: unknown;
}

/** Plausible Analytics (loaded via snippet in Base.astro) */
interface PlausibleFunction {
  (...args: unknown[]): void;
  q?: unknown[];
  init?: (options?: Record<string, unknown>) => void;
  o?: Record<string, unknown>;
}

declare global {
  interface Window {
    analytics?: SegmentAnalytics;
    plausible?: PlausibleFunction;
  }
}

export {};
