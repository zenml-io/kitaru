import { afterEach, describe, expect, it, vi } from "vitest";

import { resolveKitaruEnvironment } from "../src/environment.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("Kitaru environment", () => {
  it("prefers explicit values and normalizes one trailing slash", () => {
    vi.stubEnv("KITARU_API_URL", "https://environment.example");
    vi.stubEnv("KITARU_API_TOKEN", "environment-token");

    expect(
      resolveKitaruEnvironment({
        apiKey: "explicit-token",
        apiUrl: "https://explicit.example/",
        timeoutMs: 123,
      }),
    ).toEqual({
      apiKey: "explicit-token",
      apiUrl: "https://explicit.example",
      timeoutMs: 123,
    });
  });

  it("prefers KITARU_API_TOKEN over the legacy key", () => {
    expect(
      resolveKitaruEnvironment(
        {},
        {
          KITARU_API_KEY: "legacy-key",
          KITARU_API_TOKEN: "token",
          KITARU_API_URL: "https://api.example",
        },
      ),
    ).toMatchObject({ apiKey: "token" });
  });

  it("rejects a missing URL or invalid timeout", () => {
    expect(() => resolveKitaruEnvironment({}, {})).toThrow(
      "KITARU_API_URL is not set",
    );
    expect(() =>
      resolveKitaruEnvironment(
        { timeoutMs: Number.POSITIVE_INFINITY },
        { KITARU_API_URL: "https://api.example" },
      ),
    ).toThrow("timeoutMs must be a positive finite number");
  });
});
