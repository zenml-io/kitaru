import { Agent } from "@mastra/core/agent";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { expectTypeOf, it } from "vitest";
import { z } from "zod";

import { KitaruAgent } from "../src/index.js";
import type { StreamMethod } from "../src/types.js";
import { AGENT_ID, FakeAgent } from "./helpers.js";

it("preserves native stream overloads and schema inference", async () => {
  const agent = new Agent({
    id: "typed-stream",
    instructions: "Respond.",
    model: new MastraLanguageModelV2Mock({
      modelId: "typed-model",
      provider: "test",
    }),
    name: "Typed stream",
  });
  const wrapped = new KitaruAgent(agent, {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "typed-model",
  });
  const schema = z.object({ answer: z.string() });

  expectTypeOf(wrapped.stream).toEqualTypeOf(agent.stream);
  const assertSchemaInference = async () => {
    const result = await wrapped.stream("hello", {
      structuredOutput: { schema },
    });
    expectTypeOf(await result.object).toEqualTypeOf<{ answer: string }>();
  };
  expectTypeOf(assertSchemaInference).toBeFunction();

  const generateOnly = new KitaruAgent(new FakeAgent(), {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "generate-model",
  });
  expectTypeOf(generateOnly.stream).toEqualTypeOf<never>();
  expectTypeOf<StreamMethod<typeof agent | FakeAgent>>().toEqualTypeOf<never>();
});
