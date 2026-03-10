/**
 * Cal.com embed configuration for Kitaru.
 *
 * Constants and config objects for inline Cal.com booking widgets.
 * Pattern follows zenml-io-v2's CalEmbed integration.
 */

// ---------------------------------------------------------------------------
// Cal.com embed constants
// ---------------------------------------------------------------------------

export const CAL_ORIGIN = "https://app.cal.com";
export const CAL_EMBED_SCRIPT = "https://app.cal.com/embed/embed.js";

// ---------------------------------------------------------------------------
// Embed config type
// ---------------------------------------------------------------------------

export interface CalEmbedConfig {
  /** Cal.com namespace — isolates multiple widgets on the same page */
  namespace: string;
  /** Calendar link — e.g. "zenml/kitaru-product-demo" */
  calLink: string;
  /** DOM element id for the embed target */
  elementId: string;
  /** Layout style — defaults to "month_view" */
  layout?: "month_view";
}

// ---------------------------------------------------------------------------
// Kitaru product demo config
// ---------------------------------------------------------------------------

export const KITARU_DEMO_CAL: CalEmbedConfig = {
  namespace: "kitaru-demo",
  calLink: "zenml/kitaru-product-demo",
  elementId: "kitaru-cal-inline-demo",
  layout: "month_view",
};
