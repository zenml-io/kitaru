import { spawnSync } from "node:child_process";
import { describe, expect, it } from "vitest";

import {
  evaluateRequest,
  type SessionView,
  validateEvaluationResults,
} from "../src/evaluator/index.js";

const session = {
  session: {
    id: "recorded-session",
    inputs: { messages: ["first", "second"] },
  },
  nodes: [{ node_type: "tool_call", outputs: { found: true } }],
} as unknown as SessionView;

describe("evaluator protocol", () => {
  it("passes the entire session and params and returns the versioned result", async () => {
    const params = { threshold: 0.5 };
    await expect(
      evaluateRequest(
        { schema_version: 1, session, params },
        (view, options) => {
          expect(view).toBe(session);
          expect(options).toBe(params);
          return [
            { name: "conversation", score: 0.9, explanation: "All turns" },
          ];
        },
      ),
    ).resolves.toEqual({
      schema_version: 1,
      results: [{ name: "conversation", score: 0.9, explanation: "All turns" }],
    });
  });

  it.each([
    { schema_version: 2, session, params: {} },
    { schema_version: 1, session, params: [] },
    { schema_version: 1, session: { session: {}, nodes: {} }, params: {} },
  ])("rejects malformed requests", async (request) => {
    await expect(evaluateRequest(request, () => [])).rejects.toThrow();
  });

  it.each(
    [
      [],
      [
        { name: "same", score: 1 },
        { name: "same", score: 0 },
      ],
      [{ name: "bad name", score: 1 }],
      [{ name: "valid\n", score: 1 }],
      [{ name: "empty" }],
      [{ name: "infinite", score: Infinity }],
      [{ name: "invalid", score: true, min_score: 0 }],
      [{ name: "invalid", score: 1, value: "label", max_score: 1 }],
      [{ name: "invalid", score: 1, passed: "yes" }],
      [{ name: "invalid", score: 1, explanation: {} }],
      [{ name: "invalid", score: 1, extra: true }],
    ].map((results) => ({ results })),
  )("rejects invalid results atomically: %j", ({ results }) => {
    expect(() => validateEvaluationResults(results)).toThrow();
  });

  it("accepts booleans, labels, categorical scores and numeric scales", () => {
    const results = [
      { name: "bool", score: false },
      { name: "label", value: "" },
      { name: "category", score: 0.5, value: "okay" },
      {
        name: "float",
        score: 0,
        min_score: 0,
        max_score: 1,
        target_score: 0.5,
      },
    ];
    expect(validateEvaluationResults(results)).toEqual(results);
  });

  it("propagates callback errors", async () => {
    await expect(
      evaluateRequest({ schema_version: 1, session, params: {} }, () => {
        throw new Error("judge unavailable");
      }),
    ).rejects.toThrow("judge unavailable");
  });
});

describe("Node evaluator entrypoint", () => {
  function run(
    callback: string,
    input = JSON.stringify({ schema_version: 1, session, params: {} }),
  ) {
    const moduleUrl = new URL("../src/evaluator/index.ts", import.meta.url)
      .href;
    return spawnSync(
      process.execPath,
      [
        "--experimental-strip-types",
        "--input-type=module",
        "--eval",
        `import { runEvaluator } from ${JSON.stringify(moduleUrl)}; await runEvaluator(${callback});`,
      ],
      { input, encoding: "utf8" },
    );
  }

  it("emits one JSON response while sending scorer console logs to stderr", () => {
    const child = run(
      `async (view) => { console.log("Scorer diagnostic"); return [{ name: "whole-session", score: view.nodes.length }]; }`,
    );
    expect(child.status).toBe(0);
    expect(JSON.parse(child.stdout)).toEqual({
      schema_version: 1,
      results: [{ name: "whole-session", score: 1 }],
    });
    expect(child.stderr).toContain("Scorer diagnostic");
  });

  it("keeps deferred scorer diagnostics out of the JSON response", () => {
    const child = run(
      `() => { setTimeout(() => console.log("Deferred diagnostic"), 0); return [{ name: "valid", score: 1 }]; }`,
    );
    expect(child.status).toBe(0);
    expect(JSON.parse(child.stdout)).toEqual({
      schema_version: 1,
      results: [{ name: "valid", score: 1 }],
    });
    expect(child.stderr).toContain("Deferred diagnostic");
  });

  it.each([
    `() => { throw new Error("judge failed"); }`,
    `() => [{ name: "valid", score: 1 }, { name: "bad", score: NaN }]`,
    `() => [{ name: "same", score: 1 }, { name: "same", score: 0 }]`,
  ])("fails with empty stdout on invalid evaluation: %s", (callback) => {
    const child = run(callback);
    expect(child.status).not.toBe(0);
    expect(child.stdout).toBe("");
  });

  it("fails with empty stdout on malformed stdin", () => {
    const child = run(`() => [{ name: "valid", score: 1 }]`, "{");
    expect(child.status).not.toBe(0);
    expect(child.stdout).toBe("");
  });
});
