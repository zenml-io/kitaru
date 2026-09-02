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

import worker from "./redirect.mjs";

const marketingCases = [
  ["https://kitaru.ai/help", "https://github.com/zenml-io/kitaru/issues"],
  ["https://kitaru.ai/slack", "https://www.zenml.io/slack"],
];

for (const [input, expected] of marketingCases) {
  test(`fetch(${input})`, async () => {
    const response = await worker.fetch(new Request(input));
    assert.equal(response.status, 301);
    assert.equal(response.headers.get("location"), expected);
  });
}

// kitaru.ai/install serves the installer script instead of redirecting.
const installCases = [
  "https://kitaru.ai/install",
  "https://kitaru.ai/install.sh",
  "https://kitaru.ai/install/",
];

for (const input of installCases) {
  test(`fetch(${input}) serves install.sh`, async () => {
    const realFetch = globalThis.fetch;
    let requested;
    globalThis.fetch = async (url) => {
      requested = String(url);
      return new Response("#!/usr/bin/env bash\necho kitaru\n", {
        status: 200,
      });
    };
    try {
      const response = await worker.fetch(new Request(input));
      assert.equal(response.status, 200);
      assert.equal(
        response.headers.get("content-type"),
        "text/plain; charset=utf-8",
      );
      assert.match(await response.text(), /^#!\/usr\/bin\/env bash/);
      assert.equal(
        requested,
        "https://raw.githubusercontent.com/zenml-io/kitaru/main/install.sh",
      );
    } finally {
      globalThis.fetch = realFetch;
    }
  });
}

test("fetch(/install.md) serves the installation page as Markdown", async () => {
  const realFetch = globalThis.fetch;
  let requested;
  globalThis.fetch = async (url) => {
    requested = String(url);
    return new Response("# Installation\n", { status: 200 });
  };
  try {
    const response = await worker.fetch(
      new Request("https://kitaru.ai/install.md"),
    );
    assert.equal(response.status, 200);
    assert.equal(
      response.headers.get("content-type"),
      "text/markdown; charset=utf-8",
    );
    assert.match(await response.text(), /^# Installation/);
    assert.equal(
      requested,
      "https://raw.githubusercontent.com/zenml-io/kitaru/main/docs/book/getting-started/installation.md",
    );
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("fetch(/install) returns 502 when GitHub is unavailable", async () => {
  const realFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("nope", { status: 500 });
  try {
    const response = await worker.fetch(
      new Request("https://kitaru.ai/install"),
    );
    assert.equal(response.status, 502);
  } finally {
    globalThis.fetch = realFetch;
  }
});
