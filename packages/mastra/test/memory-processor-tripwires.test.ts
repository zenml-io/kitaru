import type { InputProcessor } from "@mastra/core/processors";
import { RequestContext } from "@mastra/core/request-context";
import type { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, expect, it, vi } from "vitest";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import {
  createMemoryRuntime,
  RESOURCE,
  seedMemory,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import { AGENT_ID, getSessionOutcomes, installTestApi } from "./helpers.js";

const stores: InMemoryStore[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const store of stores.splice(0)) await store.close();
});

/** A turn whose application input processor aborts the run. */
async function trippedTurn(
  hook: "processInput" | "processInputStep",
  captureRequestContext: (context: RequestContext) => Record<string, unknown>,
) {
  const api = installTestApi();
  const runtime = createMemoryRuntime({ messageTokens: 100_000 });
  stores.push(runtime.store);
  await seedMemory(runtime);
  const access = createProcessLocalMemoryAccess();
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("unused"),
  });
  const block = ({ abort }: { abort(reason: string): never }) =>
    abort("Blocked by the guard");
  const guard: InputProcessor =
    hook === "processInput"
      ? { id: "guard", processInput: block }
      : { id: "guard", processInputStep: block };
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "guarded",
      name: "Guarded",
      instructions: "Answer",
      memory,
      model: actor,
      inputProcessors: [guard],
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      onRecordingError: () => undefined,
      finalizationWaitMs: 500,
      captureRequestContext,
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: access,
      }),
      resolveModel: () => actor,
    },
  );
  const requestContext = new RequestContext();
  requestContext.set("tenant", "fixture");
  const output = (await adapter.stream("Hello", {
    memory: { thread: THREAD, resource: RESOURCE },
    requestContext,
  })) as { consumeStream(): Promise<void>; tripwire: unknown };
  await output.consumeStream();
  expect(output.tripwire).toMatchObject({ reason: "Blocked by the guard" });
  return { access, api };
}

it.each(["processInput", "processInputStep"] as const)(
  "releases the lease and closes the session when an application %s trips",
  async (hook) => {
    const { access, api } = await trippedTurn(hook, (context) =>
      Object.fromEntries(context.entries()),
    );
    const started = Date.now();
    const next = await access.acquire(
      { threadId: THREAD, resourceId: RESOURCE },
      { waitMs: 3_000 },
    );
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(await next.verifyEligibility()).toBe(true);
    await next();
    await vi.waitFor(() =>
      expect(getSessionOutcomes(api)).toEqual([
        "failed/ineligible/native_run_failed",
      ]),
    );
    const failed = api.calls.find(
      (call) => call.method === "PATCH" && call.body?.status === "failed",
    );
    expect(String(failed?.body?.error)).toMatch(
      /; KITARU_RECORDING_INCOMPLETE:native_run_failed$/,
    );
  },
);

it("closes a native fallback's session when an application processor trips", async () => {
  // A credential header in the context makes the turn fall back to native Mastra.
  const { api } = await trippedTurn("processInput", () => ({
    authorization: "fixture",
  }));
  await vi.waitFor(() => {
    const update = api.calls.find((call) => call.method === "PATCH");
    expect(update?.body).toMatchObject({
      status: "failed",
      metadata: {
        mastra_replay_state: "ineligible",
        mastra_replay_reason: "credential_key_unsupported",
        mastra_native_state: "failed",
      },
    });
  });
});
