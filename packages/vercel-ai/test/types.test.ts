import {
  type Agent,
  type AgentCallParameters,
  type AgentStreamParameters,
  createAgentUIStreamResponse,
  type GenerateTextResult,
  type generateText,
  jsonSchema,
  Output,
  type StreamTextResult,
  ToolLoopAgent,
  type ToolLoopAgentSettings,
  tool,
} from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expectTypeOf, it } from "vitest";

import {
  createKitaruGenerateText,
  createKitaruToolLoopAgent,
  type KitaruToolLoopAgentSettings,
} from "../src/index.js";
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

  it("freezes all four public Agent generic parameters", () => {
    type CallOptions = { tenant: string };
    type RuntimeContext = { requestId: string };

    const tools = {
      lookup: tool({
        execute: async ({ id }) => ({ name: id }),
        inputSchema: jsonSchema<{ id: string }>({
          additionalProperties: false,
          properties: { id: { type: "string" } },
          required: ["id"],
          type: "object",
        }),
      }),
    };
    const output = Output.object({
      schema: jsonSchema<{ answer: string }>({
        additionalProperties: false,
        properties: { answer: { type: "string" } },
        required: ["answer"],
        type: "object",
      }),
    });
    const settings: ToolLoopAgentSettings<
      CallOptions,
      typeof tools,
      RuntimeContext,
      typeof output
    > = {
      callOptionsSchema: jsonSchema<CallOptions>({
        additionalProperties: false,
        properties: { tenant: { type: "string" } },
        required: ["tenant"],
        type: "object",
      }),
      model: new MockLanguageModelV4(),
      output,
      prepareCall: ({ options, ...call }) => ({
        ...call,
        instructions: `Tenant: ${options.tenant}`,
      }),
      runtimeContext: { requestId: "request-1" },
      tools,
    };
    const nativeAgent = new ToolLoopAgent(settings);
    const agent: Agent<
      CallOptions,
      typeof tools,
      RuntimeContext,
      typeof output
    > = nativeAgent;

    expectTypeOf(agent.version).toEqualTypeOf<"agent-v1">();
    expectTypeOf(agent.tools).toEqualTypeOf<typeof tools>();
    expectTypeOf(agent.generate)
      .parameter(0)
      .toEqualTypeOf<
        AgentCallParameters<CallOptions, typeof tools, RuntimeContext>
      >();
    expectTypeOf(agent.generate).returns.toEqualTypeOf<
      PromiseLike<
        GenerateTextResult<typeof tools, RuntimeContext, typeof output>
      >
    >();
    expectTypeOf(agent.stream)
      .parameter(0)
      .toEqualTypeOf<
        AgentStreamParameters<CallOptions, typeof tools, RuntimeContext>
      >();
    expectTypeOf(agent.stream).returns.toEqualTypeOf<
      PromiseLike<StreamTextResult<typeof tools, RuntimeContext, typeof output>>
    >();

    const kitaruAgent = createKitaruToolLoopAgent(settings, {
      agentId: AGENT_ID,
      client: new FakeClient(),
      environment: {},
    });
    expectTypeOf(kitaruAgent).toEqualTypeOf<typeof agent>();
    const createResponse = () =>
      createAgentUIStreamResponse({
        agent: kitaruAgent,
        uiMessages: [
          {
            id: "message-1",
            parts: [{ text: "hello", type: "text" }],
            role: "user",
          },
        ],
      });
    expectTypeOf(createResponse).returns.toEqualTypeOf<Promise<Response>>();
  });

  it("accepts the exported settings alias with default generics", () => {
    const settings: KitaruToolLoopAgentSettings = {
      model: new MockLanguageModelV4(),
    };

    createKitaruToolLoopAgent(settings, {
      agentId: AGENT_ID,
      client: new FakeClient(),
      environment: {},
    });
  });
});
