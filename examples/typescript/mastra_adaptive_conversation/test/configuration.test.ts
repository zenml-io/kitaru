import type { AdapterClient } from "@zenml-io/kitaru/adapter";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_SYSTEM, MODEL } from "../src/conversation.js";
import { resolveConfiguration } from "../src/main.js";

const input = { scenario_version: "parcel-fixture-v1", prompt: DEFAULT_SYSTEM };
const id = "00000000-0000-4000-8000-000000000001";
function client() {
  return {
    getTaskSpec: vi.fn(async () => ({
      kind: "agent",
      details: { kind: "agent", inputs: input },
    })),
    getReplay: vi.fn(),
  } as unknown as AdapterClient;
}

describe("worker configuration", () => {
  it("resolves task-spec fallback once", async () => {
    const api = client();
    const config = await resolveConfiguration(api, { KITARU_TASK_ID: id });
    expect(api.getTaskSpec).toHaveBeenCalledExactlyOnceWith(id);
    expect(config.target.system).toBe(DEFAULT_SYSTEM);
  });
  it("prefers task inputs and applies prompt/model overrides to target only", async () => {
    const api = client();
    const config = await resolveConfiguration(api, {
      KITARU_TASK_ID: id,
      KITARU_TASK_INPUTS: JSON.stringify(input),
      KITARU_OVERRIDE: JSON.stringify({
        prompt: "Ask a concise reference question",
        model: "openai/gpt-5-mini",
      }),
    });
    expect(api.getTaskSpec).not.toHaveBeenCalled();
    expect(config.target.system).toBe("Ask a concise reference question");
    expect(config.target.model).toBe("openai/gpt-5-mini");
    expect(config.context.effectiveInput).toEqual({
      ...input,
      prompt: "Ask a concise reference question",
    });
  });
  it("accepts the normal replay default policy but rejects mocked tools", async () => {
    const api = client();
    vi.mocked(api.getReplay).mockResolvedValue({
      override: { prompt: "variant" },
      tool_policy: { default: { type: "passthrough" }, tools: {} },
    } as never);
    const config = await resolveConfiguration(api, {
      KITARU_REPLAY_ID: id,
      KITARU_TASK_INPUTS: JSON.stringify(input),
    });
    expect(config.target.system).toBe("variant");
    expect(api.getReplay).toHaveBeenCalledTimes(1);
    vi.mocked(api.getReplay).mockResolvedValue({
      tool_policy: { default: { type: "history" }, tools: {} },
    } as never);
    await expect(
      resolveConfiguration(api, {
        KITARU_REPLAY_ID: id,
        KITARU_TASK_INPUTS: JSON.stringify(input),
      }),
    ).rejects.toThrow("passthrough");
  });
  it("rejects unknown scenario and unapproved models before generation", async () => {
    await expect(
      resolveConfiguration(client(), {
        KITARU_TASK_INPUTS: JSON.stringify({
          ...input,
          scenario_version: "other",
        }),
      }),
    ).rejects.toThrow("versioned");
    await expect(
      resolveConfiguration(client(), {
        KITARU_OVERRIDE: JSON.stringify({ model: "openai/gpt-5" }),
      }),
    ).rejects.toThrow("allowedReplayModels");
    expect((await resolveConfiguration(client(), {})).target.model).toBe(MODEL);
  });
});
