import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  KitaruAgent,
  type StreamRecordingErrorEvent,
} from "@zenml-io/kitaru-mastra";

import {
  createStreamingSupportAgent,
  STREAM_MODEL_ID,
} from "./stream-fixture.js";

export { STREAM_TEXT } from "./stream-fixture.js";

const STREAM_PROMPT = "Check the delivery status for order ord-1001.";
const RESULT_PREFIX = "KITARU_STREAM_RESULT ";

interface StreamingSupportOptions {
  abort?: boolean;
  agentId: string;
  apiUrl?: string;
  onChunk?: (chunk: string) => void;
}

export interface StreamingSupportResult {
  aborted: boolean;
  chunks: number;
  text: string;
}

export function reportStreamRecordingError({
  sessionId,
  stage,
}: StreamRecordingErrorEvent): void {
  const location = sessionId ? ` for session ${sessionId}` : "";
  console.error(`Kitaru recording failed at ${stage}${location}`);
}

export async function runStreamingSupport({
  abort = false,
  agentId,
  apiUrl,
  onChunk,
}: StreamingSupportOptions): Promise<StreamingSupportResult> {
  const controller = new AbortController();
  let resolveAbort!: () => void;
  const abortRecorded = new Promise<void>((resolve) => {
    resolveAbort = resolve;
  });
  const recorded = new KitaruAgent(createStreamingSupportAgent(abort), {
    agentId,
    apiUrl,
    onRecordingError: reportStreamRecordingError,
    requestedModelId: STREAM_MODEL_ID,
    sessionName: abort
      ? "Mastra streaming support triage abort"
      : "Mastra streaming support triage",
  });
  const output = await recorded.stream(STREAM_PROMPT, {
    abortSignal: controller.signal,
    onAbort: () => resolveAbort(),
  });
  const chunks: string[] = [];

  try {
    for await (const chunk of output.textStream) {
      chunks.push(chunk);
      onChunk?.(chunk);
      if (abort && chunks.length === 1) {
        controller.abort(new Error("example requested an observable abort"));
      }
    }
  } catch (error) {
    if (!abort || !controller.signal.aborted) throw error;
  }
  if (abort && !controller.signal.aborted) {
    throw new Error(
      "The example stream ended before it could request an abort",
    );
  }
  if (controller.signal.aborted) await abortRecorded;

  return {
    aborted: controller.signal.aborted,
    chunks: chunks.length,
    text: chunks.join(""),
  };
}

function getRequiredOption(name: string, value: string | undefined): string {
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function getArgument(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

async function main(): Promise<void> {
  const result = await runStreamingSupport({
    abort: process.argv.includes("--abort"),
    agentId: getRequiredOption(
      "--agent-id or KITARU_AGENT_ID",
      getArgument("--agent-id") ?? process.env.KITARU_AGENT_ID,
    ),
    apiUrl: getArgument("--api-url") ?? process.env.KITARU_API_URL,
    onChunk: (chunk) => process.stdout.write(chunk),
  });
  process.stdout.write(`\n${RESULT_PREFIX}${JSON.stringify(result)}\n`);
}

const invokedPath = process.argv[1];
if (invokedPath && resolve(invokedPath) === fileURLToPath(import.meta.url)) {
  await main();
}
