import { describe, expect, it } from "vitest";
import { providerFamily } from "../../src/adapter/index.js";

describe("providerFamily", () => {
  it("keeps a provider that is already a bare family", () => {
    expect(providerFamily("openai")).toBe("openai");
  });

  it("drops the transport suffix", () => {
    expect(providerFamily("openai.responses")).toBe("openai");
    expect(providerFamily("anthropic.messages")).toBe("anthropic");
  });

  it("keeps only the first segment of a multi-part provider", () => {
    expect(providerFamily("openai.chat.v1")).toBe("openai");
  });

  it("returns an empty family for a leading separator", () => {
    expect(providerFamily(".responses")).toBe("");
  });
});
