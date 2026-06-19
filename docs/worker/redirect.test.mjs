// Smoke tests for the kitaru.ai/docs redirect mapping.
// Run: node --test docs/worker/redirect.test.mjs
import assert from "node:assert/strict";
import { test } from "node:test";
import { docsRedirectTarget } from "./redirect.mjs";

const cases = [
  // hand-written docs -> GitBook
  ["/docs", "https://docs.zenml.io/kitaru"],
  ["/docs/concepts/flows", "https://docs.zenml.io/kitaru/concepts/flows"],
  // generated reference -> sdkdocs
  ["/docs/cli", "https://sdkdocs.kitaru.ai/cli"],
  ["/docs/cli/stack/create", "https://sdkdocs.kitaru.ai/cli/stack/create"],
  [
    "/docs/reference/python/flow",
    "https://sdkdocs.kitaru.ai/reference/python/flow",
  ],
  // changelog
  ["/docs/changelog", "https://docs.zenml.io/changelog"],
  // segment-aware: these must NOT match /cli or /changelog prefixes
  ["/docs/clipboard", "https://docs.zenml.io/kitaru/clipboard"],
  ["/docs/changelog-old", "https://docs.zenml.io/kitaru/changelog-old"],
  [
    "/docs/reference-architecture",
    "https://docs.zenml.io/kitaru/reference-architecture",
  ],
  // retired in-docs redirects resolve to their new home
  [
    "/docs/guides/pydantic-ai-adapter",
    "https://docs.zenml.io/kitaru/adapters/pydantic-ai/",
  ],
  [
    "/docs/concepts/memory",
    "https://docs.zenml.io/kitaru/concepts/checkpoints/",
  ],
];

for (const [input, expected] of cases) {
  test(`docsRedirectTarget(${input})`, () => {
    assert.equal(docsRedirectTarget(input), expected);
  });
}
