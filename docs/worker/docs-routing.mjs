export const DOCS_PREFIX = "/docs";
export const ZENML_BASE_URL = "https://www.zenml.io";

export const RETIRED_DOCS_REDIRECT_STATUS = 308;

export const RETIRED_DOCS_REDIRECTS = [
  [`${DOCS_PREFIX}/concepts/memory`, `${DOCS_PREFIX}/concepts/checkpoints/`],
  [`${DOCS_PREFIX}/guides/memory`, `${DOCS_PREFIX}/guides/artifacts/`],
];

export const ROOT_DOCS_ASSET_PATHS = [
  "/favicon.svg",
  "/kitaru-logo.svg",
  "/robots.txt",
  "/sitemap.xml",
  "/llms.txt",
  "/llms-full.txt",
];

export const LEGACY_MARKETING_REDIRECTS = [
  ["/", `${ZENML_BASE_URL}/product/kitaru`],
  ["/pricing", `${ZENML_BASE_URL}/pricing`],
  ["/book-a-demo", `${ZENML_BASE_URL}/book-your-demo`],
  ["/newsletter", `${ZENML_BASE_URL}/newsletter-signup`],
  ["/get-started", `${ZENML_BASE_URL}/book-your-demo`],
];

export const LEGACY_MARKETING_PREFIX_REDIRECTS = ["/blog", "/compare"];
