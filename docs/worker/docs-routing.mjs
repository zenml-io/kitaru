export const DOCS_PREFIX = "/docs";
export const ZENML_BASE_URL = "https://www.zenml.io";

export const RETIRED_DOCS_REDIRECT_STATUS = 308;

export const RETIRED_DOCS_REDIRECTS = [
  [`${DOCS_PREFIX}/concepts/memory`, `${DOCS_PREFIX}/concepts/checkpoints/`],
  [`${DOCS_PREFIX}/guides/memory`, `${DOCS_PREFIX}/guides/artifacts/`],
  [
    `${DOCS_PREFIX}/guides/pydantic-ai-adapter`,
    `${DOCS_PREFIX}/adapters/pydantic-ai/`,
  ],
  [
    `${DOCS_PREFIX}/guides/pydantic-ai-adapter.md`,
    `${DOCS_PREFIX}/adapters/pydantic-ai.md`,
  ],
  [
    `${DOCS_PREFIX}/guides/openai-agents-adapter`,
    `${DOCS_PREFIX}/adapters/openai-agents/`,
  ],
  [
    `${DOCS_PREFIX}/guides/openai-agents-adapter.md`,
    `${DOCS_PREFIX}/adapters/openai-agents.md`,
  ],
  [
    `${DOCS_PREFIX}/guides/claude-agent-sdk-adapter`,
    `${DOCS_PREFIX}/adapters/claude-agent-sdk/`,
  ],
  [
    `${DOCS_PREFIX}/guides/claude-agent-sdk-adapter.md`,
    `${DOCS_PREFIX}/adapters/claude-agent-sdk.md`,
  ],
  [
    `${DOCS_PREFIX}/guides/gemini-interactions-adapter`,
    `${DOCS_PREFIX}/adapters/gemini-interactions/`,
  ],
  [
    `${DOCS_PREFIX}/guides/gemini-interactions-adapter.md`,
    `${DOCS_PREFIX}/adapters/gemini-interactions.md`,
  ],
  [
    `${DOCS_PREFIX}/guides/langgraph-adapter`,
    `${DOCS_PREFIX}/adapters/langgraph/`,
  ],
  [
    `${DOCS_PREFIX}/guides/langgraph-adapter.md`,
    `${DOCS_PREFIX}/adapters/langgraph.md`,
  ],
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
  // Support entry points, announced for the v2 launch.
  ["/help", "https://github.com/zenml-io/kitaru/issues"],
  ["/slack", `${ZENML_BASE_URL}/slack`],
];

export const LEGACY_MARKETING_PREFIX_REDIRECTS = ["/blog", "/compare"];
