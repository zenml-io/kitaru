export const DEFAULT_PROMPT =
  "I need help with a fictional delayed parcel. What information do you need?";
export const DEFAULT_SYSTEM =
  "You assist with fictional parcel support. Ask for the order reference first. Once given FIXTURE-42, explain that this fixture is delayed and suggest contacting support. Never claim to use tools or perform real actions. Keep each answer under 80 words.";
export const MODEL = "openai/gpt-5-nano";
export const LIMITS = Object.freeze({
  turns: 3,
  stepsPerCall: 1,
  elapsedMs: 120_000,
  callMs: 45_000,
  maxOutputTokens: 2000,
  totalTokens: 12000,
  textChars: 6000,
});
export type Message =
  | { role: "user"; content: string }
  | { role: "assistant"; content: string };
export type Limits = { [Key in keyof typeof LIMITS]: number };
export interface TargetConfig {
  model: string;
  system: string;
  maxOutputTokens: number;
}
export interface Reply {
  text: string;
  totalTokens: number;
}
export type Generate = (
  history: Message[],
  signal: AbortSignal,
) => Promise<Reply>;
export interface Transcript {
  transcript_version: "1";
  scenario_version: "parcel-fixture-v1";
  messages: Message[];
  branches: string[];
  stop_reason:
    | "running"
    | "scenario_complete"
    | "turn_limit"
    | "call_timeout"
    | "time_limit"
    | "usage_limit"
    | "runtime_failure";
  stop_detail?:
    | "call_deadline"
    | "conversation_deadline"
    | "target_or_recording_failure";
  target: TargetConfig;
  simulator: { kind: "deterministic"; version: "parcel-fixture-v1" };
  judge: { kind: "python"; configuration: "independent-evaluator-job" };
  recording: "transcript-only";
  tools: "disabled";
  total_tokens: number;
  limits: Limits;
}

export function nextMessage(
  reply: string,
): { branch: string; text: string } | undefined {
  if (/FIXTURE-42/i.test(reply) && /support|delayed/i.test(reply))
    return undefined;
  if (/order|reference|tracking/i.test(reply))
    return {
      branch: "provide_reference",
      text: "The fictional order reference is FIXTURE-42. What should I do about the delay?",
    };
  return {
    branch: "request_clarification",
    text: "Please ask me for the order reference before suggesting next steps.",
  };
}

export async function runConversation(options: {
  prompt: string;
  target: TargetConfig;
  createTarget: (config: TargetConfig) => Generate;
  checkpoint: (transcript: Transcript) => Promise<void>;
  limits?: Limits;
}): Promise<Transcript> {
  const limits = options.limits ?? LIMITS;
  const result: Transcript = {
    transcript_version: "1",
    scenario_version: "parcel-fixture-v1",
    messages: [],
    branches: [],
    stop_reason: "running",
    target: { ...options.target },
    simulator: { kind: "deterministic", version: "parcel-fixture-v1" },
    judge: { kind: "python", configuration: "independent-evaluator-job" },
    recording: "transcript-only",
    tools: "disabled",
    total_tokens: 0,
    limits: { ...limits },
  };
  const started = Date.now();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const generate = options.createTarget({ ...options.target });
    let user = options.prompt;
    for (let turn = 0; turn < limits.turns; turn++) {
      const remaining = limits.elapsedMs - (Date.now() - started);
      if (remaining <= 0) {
        result.stop_reason = "time_limit";
        result.stop_detail = "conversation_deadline";
        break;
      }
      if (result.total_tokens >= limits.totalTokens) {
        result.stop_reason = "usage_limit";
        break;
      }
      result.messages.push({ role: "user", content: user });
      await options.checkpoint(structuredClone(result));
      const callRemaining = limits.elapsedMs - (Date.now() - started);
      if (callRemaining <= 0) {
        result.stop_reason = "time_limit";
        result.stop_detail = "conversation_deadline";
        break;
      }
      const controller = new AbortController();
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => {
            // Settle the deadline first: an abort listener may reject immediately.
            reject(
              new Error(
                callRemaining <= limits.callMs
                  ? "conversation_deadline"
                  : "call_deadline",
              ),
            );
            controller.abort();
          },
          Math.min(callRemaining, limits.callMs),
        );
      });
      let reply: Reply;
      try {
        reply = await Promise.race([
          generate(structuredClone(result.messages), controller.signal),
          timeout,
        ]);
      } finally {
        clearTimeout(timer);
      }
      if (!Number.isFinite(reply.totalTokens) || reply.totalTokens < 0)
        throw new Error("Missing or invalid provider token usage");
      result.total_tokens += reply.totalTokens;
      if (!reply.text.trim() || reply.text.length > limits.textChars)
        throw new Error("Empty or oversized target response");
      result.messages.push({ role: "assistant", content: reply.text });
      await options.checkpoint(structuredClone(result));
      if (Date.now() - started >= limits.elapsedMs) {
        result.stop_reason = "time_limit";
        result.stop_detail = "conversation_deadline";
        break;
      }
      if (result.total_tokens >= limits.totalTokens) {
        result.stop_reason = "usage_limit";
        break;
      }
      const next = nextMessage(reply.text);
      if (!next) {
        result.stop_reason = "scenario_complete";
        break;
      }
      if (turn + 1 === limits.turns) {
        result.stop_reason = "turn_limit";
        break;
      }
      result.branches.push(next.branch);
      user = next.text;
    }
  } catch (error) {
    result.stop_reason =
      error instanceof Error && error.message === "conversation_deadline"
        ? "time_limit"
        : error instanceof Error && error.message === "call_deadline"
          ? "call_timeout"
          : "runtime_failure";
    result.stop_detail =
      result.stop_reason === "runtime_failure"
        ? "target_or_recording_failure"
        : result.stop_reason === "call_timeout"
          ? "call_deadline"
          : "conversation_deadline";
    await options.checkpoint(structuredClone(result));
    if (
      result.stop_reason === "runtime_failure" ||
      result.stop_reason === "call_timeout"
    )
      throw new Error("Conversation target or checkpoint failed", {
        cause: error,
      });
  } finally {
    clearTimeout(timer);
  }
  await options.checkpoint(structuredClone(result));
  return result;
}
