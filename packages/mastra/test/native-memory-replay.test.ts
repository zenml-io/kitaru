import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { APICallError } from "ai";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  consumeMemoryAgent,
  createMemoryRuntime,
  FILE_BYTES,
  FILE_URL,
  type ModelCall,
  makeMemoryAgent,
  RESOURCE,
  restoreMemory,
  seedMemory,
  snapshotMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";

let skillsPath: string;
beforeEach(async () => {
  skillsPath = await mkdtemp(join(tmpdir(), "mastra-memory-proof-"));
  await mkdir(join(skillsPath, "skills", "triage"), { recursive: true });
  await writeFile(
    join(skillsPath, "skills", "triage", "SKILL.md"),
    "---\nname: triage\ndescription: HISTORICAL_SKILL classify support issues.\n---\nKeep the original evidence.\n",
  );
});
afterEach(async () => {
  vi.restoreAllMocks();
  await rm(skillsPath, { recursive: true, force: true });
});

it("restores historical native memory into a separate store and reruns the file processor", async () => {
  const production = createMemoryRuntime({ messageTokens: 6000 });
  await seedMemory(production);
  const historical = await snapshotMemory(production, true);
  const baselineCalls: ModelCall[] = [];
  const baselineProcessors: string[] = [];
  const configurationCalls: string[] = [];
  const capturedFiles = new Map<string, Uint8Array>();
  let liveFile: Uint8Array | undefined = FILE_BYTES;
  const liveResolver = vi.fn(async (url: string) => {
    if (!liveFile) throw new Error("Original file is unavailable");
    capturedFiles.set(url, new Uint8Array(liveFile));
    return new Uint8Array(liveFile);
  });
  const model = (calls: ModelCall[]) =>
    new MastraLanguageModelV2Mock({
      modelId: "actor",
      provider: "fixture",
      doStream: async (options) => {
        calls.push(options);
        return textStream("done");
      },
    });
  const baseline = makeMemoryAgent({
    runtime: production,
    model: model(baselineCalls),
    skillsPath,
    resolveFile: liveResolver,
    processorRuns: baselineProcessors,
    configurationCalls,
  });
  expect((await consumeMemoryAgent(baseline)).text).toBe("done");
  await production.memory.settled();
  liveFile = undefined;
  await production.memory.updateWorkingMemory({
    threadId: THREAD,
    resourceId: RESOURCE,
    workingMemory: '{"preference":"production-red"}',
  });
  const record = await production.domain.getObservationalMemory(
    THREAD,
    RESOURCE,
  );
  if (!record) throw new Error("Missing observational memory");
  await production.domain.updateActiveObservations({
    id: record.id,
    observations: "PRODUCTION_RED",
    tokenCount: 10,
    lastObservedAt: new Date(),
  });

  const replay = createMemoryRuntime({ messageTokens: 6000 });
  await restoreMemory(historical, replay);
  expect(await snapshotMemory(replay, true)).toEqual(historical);
  const denied: string[] = [];
  for (const name of Object.getOwnPropertyNames(
    Object.getPrototypeOf(production.domain),
  )) {
    if (name === "constructor") continue;
    const descriptor = Object.getOwnPropertyDescriptor(
      Object.getPrototypeOf(production.domain),
      name,
    );
    if (typeof descriptor?.value === "function") {
      vi.spyOn(production.domain, name as "listMessages").mockImplementation(
        () => {
          denied.push(name);
          throw new Error(`Production access: ${name}`);
        },
      );
    }
  }
  const replayCalls: ModelCall[] = [];
  const replayProcessors: string[] = [];
  const fixture = makeMemoryAgent({
    runtime: replay,
    model: model(replayCalls),
    skillsPath,
    resolveFile: async (url) => {
      const bytes = capturedFiles.get(url);
      if (!bytes) throw new Error("Missing captured file");
      return new Uint8Array(bytes);
    },
    processorRuns: replayProcessors,
  });
  const nativeStream = vi.spyOn(fixture.agent, "stream");
  const result = await consumeMemoryAgent(fixture);
  expect(result.text).toBe("done");
  expect(result.output).toBe(await nativeStream.mock.results[0]?.value);
  expect(await fixture.agent.getMemory()).toBe(replay.memory);
  expect(await fixture.agent.getMemory()).toBe(replay.memory);
  await replay.memory.settled();
  expect(denied).toEqual([]);
  expect(liveResolver).toHaveBeenCalledExactlyOnceWith(FILE_URL);
  expect(baselineProcessors).toHaveLength(1);
  expect(replayProcessors).toHaveLength(1);
  expect(configurationCalls).toEqual(
    expect.arrayContaining(["instructions", "model", "defaultOptions"]),
  );
  expect(baselineCalls[0]?.temperature).toBe(0.25);
  const prompt = JSON.stringify(replayCalls[0]?.prompt);
  for (const marker of [
    "historical-blue",
    "HISTORICAL_OBSERVATION",
    "HISTORICAL_SKILL",
    "EXTRA_SYSTEM_CONTEXT",
    "APPLICATION_INSTRUCTIONS",
    "HISTORICAL_MESSAGE",
  ])
    expect(prompt).toContain(marker);
  expect(prompt).not.toContain("production-red");
  expect(prompt).not.toContain("PRODUCTION_RED");
  expect(replayCalls[0]?.tools?.map((tool) => tool.name)).toContain(
    "updateWorkingMemory",
  );
  expect(
    replayCalls[0]?.tools?.some((tool) => tool.name.includes("skill")),
  ).toBe(true);
  const files = (calls: ModelCall[]) =>
    calls[0]?.prompt.flatMap((message) =>
      message.role !== "system"
        ? message.content.flatMap((part) =>
            part.type === "file"
              ? [{ data: part.data, mediaType: part.mediaType }]
              : [],
          )
        : [],
    );
  expect(files(replayCalls)).toEqual(files(baselineCalls));
  expect(files(replayCalls)?.[0]).toEqual({
    data: Buffer.from(FILE_BYTES).toString("base64"),
    mediaType: "application/pdf",
  });
}, 20000);

it("runs native working-memory updates, observation and reflection inside an isolated turn", async () => {
  const production = createMemoryRuntime({ messageTokens: 600 });
  await seedMemory(production);
  const replay = createMemoryRuntime({ messageTokens: 600 });
  await restoreMemory(await snapshotMemory(production, true), replay);
  const calls: ModelCall[] = [];
  const model = new MastraLanguageModelV2Mock({
    modelId: "evolving-actor",
    provider: "fixture",
    doStream: async (options) => {
      calls.push(options);
      return calls.length === 1
        ? streamParts(
            [
              {
                type: "tool-call",
                toolCallId: "update-1",
                toolName: "updateWorkingMemory",
                input: JSON.stringify({
                  memory: {
                    preference: "replay-green",
                    notes: "A new preference",
                  },
                }),
              },
            ],
            "tool-calls",
          )
        : calls.length === 2
          ? streamParts(
              [
                {
                  type: "tool-call",
                  toolCallId: "evidence-1",
                  toolName: "readConversationEvidence",
                  input: "{}",
                },
              ],
              "tool-calls",
            )
          : textStream("evolved");
    },
  });
  const fixture = makeMemoryAgent({
    runtime: replay,
    model,
    skillsPath,
    resolveFile: async () => FILE_BYTES,
    processorRuns: [],
  });
  expect((await consumeMemoryAgent(fixture)).text).toBe("evolved");
  await replay.memory.settled();
  expect(calls).toHaveLength(3);
  expect(replay.observer.calls.length).toBeGreaterThan(0);
  expect(replay.reflector.calls.length).toBeGreaterThan(0);
  expect(
    await replay.memory.getWorkingMemory({
      threadId: THREAD,
      resourceId: RESOURCE,
    }),
  ).toContain("replay-green");
  expect(JSON.stringify(calls[2]?.prompt)).toContain("REFLECTED_REPLAY");
  const secondSystem = calls[1]?.prompt.filter(
    (message) => message.role === "system",
  );
  // Native working-memory injection runs once. The subsequent tool result
  // conveys the update; changing that behavior would cease to be native replay.
  expect(JSON.stringify(secondSystem)).toContain("historical-blue");
  expect(JSON.stringify(secondSystem)).not.toContain("replay-green");
  expect(
    JSON.stringify(
      calls[1]?.prompt.filter((message) => message.role === "tool"),
    ),
  ).toContain("replay-green");
  expect(
    await production.memory.getWorkingMemory({
      threadId: THREAD,
      resourceId: RESOURCE,
    }),
  ).toContain("historical-blue");
  const evolved = await snapshotMemory(replay, true);
  expect(
    evolved.records.some((record) => record.originType === "reflection"),
  ).toBe(true);
  const restored = createMemoryRuntime({ messageTokens: 600 });
  await restoreMemory(evolved, restored);
  expect(await snapshotMemory(restored, true)).toEqual(evolved);
}, 20000);

it("captures both retry attempts at public doStream after late native processor changes", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const received: ModelCall[] = [];
  const attempts: { id: number; options: ModelCall; failed: boolean }[] = [];
  const model = new MastraLanguageModelV2Mock({
    modelId: "retry-actor",
    provider: "fixture",
    doStream: async (options) => {
      received.push(options);
      if (received.length === 1)
        throw new APICallError({
          message: "Retryable fixture failure",
          url: "https://model.invalid",
          requestBodyValues: {},
          statusCode: 503,
          isRetryable: true,
        });
      return textStream("retried");
    },
  });
  const instrumented = new Proxy(model, {
    get(target, key) {
      if (key === "doStream")
        return async (options: ModelCall) => {
          const attempt = { id: attempts.length, options, failed: false };
          attempts.push(attempt);
          try {
            return await target.doStream(options);
          } catch (error) {
            attempt.failed = true;
            throw error;
          }
        };
      const value = Reflect.get(target, key, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const fixture = makeMemoryAgent({
    runtime,
    model: instrumented,
    skillsPath,
    resolveFile: async () => FILE_BYTES,
    processorRuns: [],
    processors: [
      {
        id: "late-settings-and-prompt",
        processInputStep: () => ({
          modelSettings: {
            temperature: 0.75,
            maxOutputTokens: 91,
            maxRetries: 1,
          },
        }),
        processLLMRequest: ({ prompt }) => ({
          prompt: [
            ...prompt,
            { role: "system", content: "LATE_PROMPT_TRANSFORMATION" },
          ],
        }),
      },
    ],
  });
  expect((await consumeMemoryAgent(fixture)).text).toBe("retried");
  await runtime.memory.settled();
  expect(attempts.map(({ id, failed }) => ({ id, failed }))).toEqual([
    { id: 0, failed: true },
    { id: 1, failed: false },
  ]);
  expect(received).toHaveLength(2);
  for (const [index, attempt] of attempts.entries()) {
    expect(attempt.options).toStrictEqual(received[index]);
    expect(attempt.options).toMatchObject({
      temperature: 0.75,
      maxOutputTokens: 91,
    });
    expect(JSON.stringify(attempt.options.prompt)).toContain(
      "LATE_PROMPT_TRANSFORMATION",
    );
  }
}, 20000);

it("joins owned background observation without discarding persisted buffers", async () => {
  let releaseObserver!: () => void;
  let observerStarted!: () => void;
  const blocked = new Promise<void>((resolve) => {
    releaseObserver = resolve;
  });
  const started = new Promise<void>((resolve) => {
    observerStarted = resolve;
  });
  const runtime = createMemoryRuntime({
    messageTokens: 10000,
    observerWait: async () => {
      observerStarted();
      await blocked;
    },
  });
  await seedMemory(runtime);
  const fixture = makeMemoryAgent({
    runtime,
    skillsPath,
    model: new MastraLanguageModelV2Mock({
      doStream: async () => textStream("done"),
    }),
    resolveFile: async () => FILE_BYTES,
    processorRuns: [],
  });
  try {
    expect((await consumeMemoryAgent(fixture)).text).toBe("done");
    await started;
    let settled = false;
    const snapshot = snapshotMemory(runtime, true).then((value) => {
      settled = true;
      return value;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(settled).toBe(false);
    releaseObserver();
    const historical = await snapshot;
    expect(
      historical.records[0]?.bufferedObservationChunks?.length,
    ).toBeGreaterThan(0);
    expect(historical.records[0]?.isBufferingObservation).toBe(false);
    const replay = createMemoryRuntime({ messageTokens: 10000 });
    await restoreMemory(historical, replay);
    expect(await snapshotMemory(replay, true)).toEqual(historical);
  } finally {
    releaseObserver();
    await runtime.memory.settled();
  }
}, 20000);

it("rejects a source without exclusive access or with work this Memory cannot join", async () => {
  const runtime = createMemoryRuntime();
  await seedMemory(runtime);
  const read = vi.spyOn(runtime.domain, "listMessages");
  await expect(snapshotMemory(runtime, false)).rejects.toThrow(
    "Exclusive source-thread ownership required",
  );
  expect(read).not.toHaveBeenCalled();
  const record = await runtime.domain.getObservationalMemory(THREAD, RESOURCE);
  if (!record) throw new Error("Missing observational memory");
  await runtime.domain.setBufferingObservationFlag(record.id, true);
  // No promise on this Memory instance corresponds to the persisted flag.
  await runtime.memory.settled();
  await expect(snapshotMemory(runtime, true)).rejects.toThrow(
    "Unjoined observational-memory work",
  );
});
