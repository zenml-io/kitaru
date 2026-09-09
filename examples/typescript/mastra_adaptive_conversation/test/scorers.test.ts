import type { SessionView } from "@zenml-io/kitaru/evaluator";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Message } from "../src/conversation.js";
import {
  createConversationEvaluator,
  DEFAULT_JUDGE_INSTRUCTIONS,
  inspectEvidence,
  mapConversation,
} from "../src/scorers.js";

const messages: Message[] = [
  { role: "user", content: "My fictional parcel is delayed." },
  { role: "assistant", content: "What is your order reference?" },
  { role: "user", content: "It is FIXTURE-42." },
  { role: "assistant", content: "FIXTURE-42 is delayed; contact support." },
];

async function mockJudge(received: Message[]) {
  return {
    score: Number(
      received.some(
        (message) =>
          message.role === "user" && message.content === messages[0]?.content,
      ) &&
        received.some(
          (message) =>
            message.role === "assistant" &&
            message.content === messages[1]?.content,
        ),
    ),
    reason:
      "Checked the earlier user report and assistant question in the supplied transcript",
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

function view(transcript: Message[] = messages): SessionView {
  return {
    session: {
      status: "completed",
      outputs: {
        transcript_version: "1",
        scenario_version: "parcel-fixture-v1",
        recording: "transcript-only",
        tools: "disabled",
        stop_reason: "scenario_complete",
        messages: structuredClone(transcript),
        target: { model: "target-model", system: "target instructions" },
        simulator: { kind: "deterministic" },
        judge: { kind: "python", configuration: "recorded configuration" },
      },
    },
    nodes: [
      {
        node_type: "span",
        name: "run",
        parent_index: null,
        status: "completed",
      },
    ],
  } as unknown as SessionView;
}

describe("generated conversation scorers", () => {
  it.each([
    "What is your order reference?",
    "Which order number should I check?",
    "Could you please share your reference?",
    "Can you provide the tracking number?",
    "Please send your order ID.",
    "May I have your order reference?",
  ])("recognizes a lexical request: %s", (content) => {
    const transcript = structuredClone(messages);
    transcript[1] = { role: "assistant", content };
    expect(inspectEvidence(transcript).score).toBe(1);
  });

  it.each([
    "I cannot look up tracking; please contact support.",
    "The order reference is unavailable.",
    "Please contact support about your reference.",
    "Could you contact support? Your reference is unavailable.",
  ])("does not treat a reference mention as a request: %s", (content) => {
    const transcript = structuredClone(messages);
    transcript[1] = { role: "assistant", content };
    const result = inspectEvidence(transcript);
    expect(result.score).toBe(0);
    expect(result.reason).toContain(
      "lexical assistant reference request at -1",
    );
  });

  it("passes all ordered user and assistant messages to both native pipelines", async () => {
    const judge = vi.fn(mockJudge);
    const result = await createConversationEvaluator(judge)(view(), {});
    expect(result.map((row) => [row.name, row.score])).toEqual([
      ["conversation_evidence", 1],
      ["conversation_judge", 1],
    ]);
    expect(result.every((row) => Boolean(row.explanation))).toBe(true);
    expect(judge).toHaveBeenCalledWith(messages, {
      model: "gpt-5-nano",
      instructions: DEFAULT_JUDGE_INSTRUCTIONS,
    });
    expect(mapConversation(view()).input.messages).toEqual(messages);
  });

  it.each([
    0, 1,
  ])("changing earlier message %s changes both scores", async (index) => {
    const changed = structuredClone(messages);
    const original = changed[index];
    if (!original) throw new Error("Missing test message");
    changed[index] = { role: original.role, content: "Unrelated greeting." };
    const judge = vi.fn(mockJudge);
    const result = await createConversationEvaluator(judge)(view(changed), {});
    expect(result.map((row) => row.score)).toEqual([0, 0]);
    expect(judge.mock.calls[0]?.[0]).toEqual(changed);
  });

  it("removing the opening pair loses evidence even with the same final answer", async () => {
    const judge = vi.fn(mockJudge);
    const result = await createConversationEvaluator(judge)(
      view(messages.slice(2)),
      {},
    );
    expect(result.map((row) => row.score)).toEqual([0, 0]);
  });

  it("preserves long earlier content rather than silently truncating", () => {
    const long = structuredClone(messages);
    long[0] = {
      role: "user",
      content: `${"Earlier evidence ".repeat(1000)}delayed parcel`,
    };
    expect(mapConversation(view(long)).input.messages).toEqual(long);
  });

  it("selects judge configuration exclusively from evaluation parameters", async () => {
    const judge = vi.fn(async () => ({ score: 1, reason: "Checked history" }));
    const evaluator = createConversationEvaluator(judge);
    await evaluator(view(), {
      judge_model: "gpt-5-nano",
      judge_instructions: "Independent rubric",
    });
    expect(judge).toHaveBeenCalledWith(messages, {
      model: "gpt-5-nano",
      instructions: "Independent rubric",
    });
    const changed = view();
    Object.assign(changed.session.outputs as object, {
      target: {
        model: "another-target",
        system: "Changed target instructions",
      },
      simulator: { kind: "another-simulator" },
      judge: { configuration: "another-recorded-judge" },
    });
    await evaluator(changed, {
      judge_model: "gpt-5-nano",
      judge_instructions: "Independent rubric",
    });
    expect(judge.mock.calls[1]).toEqual(judge.mock.calls[0]);
  });

  it.each([
    "running",
    "runtime_failure",
    "call_timeout",
  ])("rejects incomplete stop reason %s before judging", async (stop_reason) => {
    const input = view();
    Object.assign(input.session.outputs as object, { stop_reason });
    const judge = vi.fn();
    await expect(
      createConversationEvaluator(judge)(input, {}),
    ).rejects.toThrow();
    expect(judge).not.toHaveBeenCalled();
  });

  it.each([
    "time_limit",
    "usage_limit",
    "turn_limit",
  ])("accepts a completed paired transcript stopped by %s but rejects a pending user", (stop_reason) => {
    const input = view();
    Object.assign(input.session.outputs as object, { stop_reason });
    expect(mapConversation(input).input.messages).toEqual(messages);
    Object.assign(input.session.outputs as object, {
      messages: messages.slice(0, -1),
    });
    expect(() => mapConversation(input)).toThrow();
  });

  it.each([
    { transcript_version: "2" },
    { tools: "enabled" },
    { recording: "telemetry" },
    { messages: messages.slice(0, -1) },
    {
      messages: [
        { role: "user", content: "Hi" },
        { role: "assistant", content: "" },
      ],
    },
    {
      messages: [
        { role: "assistant", content: "Hi" },
        { role: "user", content: "Hi" },
      ],
    },
    {
      messages: [{ role: "user", content: "Hi", tool_calls: [] }, messages[1]],
    },
  ])("rejects unsupported transcript %j", (patch) => {
    const input = view();
    Object.assign(input.session.outputs as object, patch);
    expect(() => mapConversation(input)).toThrow();
  });

  it("rejects failed sessions and tool telemetry instead of inventing tool evidence", () => {
    const failed = view();
    failed.session.status = "failed";
    expect(() => mapConversation(failed)).toThrow();
    const tool = view();
    tool.nodes[0] = {
      ...tool.nodes[0],
      node_type: "tool_call",
    } as SessionView["nodes"][number];
    expect(() => mapConversation(tool)).toThrow();
  });

  it("propagates a later judge failure without returning the deterministic row", async () => {
    const judge = vi.fn(async () => {
      throw new Error("Judge unavailable");
    });
    await expect(
      createConversationEvaluator(judge)(view(), {}),
    ).rejects.toThrow();
    expect(judge).toHaveBeenCalledOnce();
  });

  it("rejects malformed judge results and unsupported judge settings", async () => {
    await expect(
      createConversationEvaluator(async () => ({
        score: Number.NaN,
        reason: "bad",
      }))(view(), {}),
    ).rejects.toThrow();
    const judge = vi.fn();
    await expect(
      createConversationEvaluator(judge)(view(), {
        judge_model: "target-model",
      }),
    ).rejects.toThrow("Judge model");
    expect(judge).not.toHaveBeenCalled();
  });

  it("sends full evidence and independent settings through the real judge request path", async () => {
    vi.stubEnv("OPENAI_API_KEY", "test-key");
    const fetchMock = vi.fn(async (_url: string, options: RequestInit) => {
      const request = JSON.parse(String(options.body));
      expect(request.model).toBe("gpt-5-nano");
      expect(request.instructions).toBe("Independent rubric");
      expect(request.max_output_tokens).toBe(2000);
      expect(options.signal).toBeInstanceOf(AbortSignal);
      const transcript = JSON.parse(request.input).messages;
      expect(transcript).toEqual(messages);
      return Response.json({
        status: "completed",
        output: [
          {
            content: [
              {
                type: "output_text",
                text: JSON.stringify(await mockJudge(transcript)),
              },
            ],
          },
        ],
      });
    });
    vi.stubGlobal("fetch", fetchMock);
    const result = await createConversationEvaluator()(view(), {
      judge_instructions: "Independent rubric",
    });
    expect(result.map((row) => row.score)).toEqual([1, 1]);
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("does not retry a failed provider request or emit partial results", async () => {
    vi.stubEnv("OPENAI_API_KEY", "test-key");
    const fetchMock = vi.fn(
      async () => new Response("unavailable", { status: 503 }),
    );
    vi.stubGlobal("fetch", fetchMock);
    await expect(createConversationEvaluator()(view(), {})).rejects.toThrow();
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it.each([
    { status: "incomplete", text: '{"score":1,"reason":"Checked"}' },
    { status: "completed", text: "invalid JSON" },
    { status: "completed", text: '{"score":0.5,"reason":"Checked"}' },
    { status: "completed", text: '{"score":1,"reason":""}' },
  ])("rejects unusable provider verdict %j", async ({ status, text }) => {
    vi.stubEnv("OPENAI_API_KEY", "test-key");
    const fetchMock = vi.fn(async () =>
      Response.json({
        status,
        output: [{ content: [{ type: "output_text", text }] }],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    await expect(createConversationEvaluator()(view(), {})).rejects.toThrow();
    expect(fetchMock).toHaveBeenCalledOnce();
  });
});
