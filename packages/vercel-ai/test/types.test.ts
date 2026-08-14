import type { generateText } from "ai";
import { describe, expectTypeOf, it } from "vitest";

import { createKitaruGenerateText } from "../src/index.js";
import { AGENT_ID, FakeClient } from "./helpers.js";

describe("declaration compatibility", () => {
  it("preserves the complete native generateText generic signature", () => {
    const bound = createKitaruGenerateText({
      agentId: AGENT_ID,
      client: new FakeClient(),
      environment: {},
    });
    expectTypeOf(bound).toEqualTypeOf<typeof generateText>();
  });
});
