import type { JsonValue, SessionNodeCreateRequest } from "@zenml-io/kitaru";
import {
  type RecordingLimits,
  ROOT_NODE_EXTERNAL_ID,
  type SecretKeyClassifier,
} from "@zenml-io/kitaru/adapter";
import { getMemoryModelId } from "./memory-replay.js";
import { decodeMemoryValue, encodeMemoryEvidence } from "./memory-snapshot.js";
import type { OMLiveCall } from "./om-result-tape.js";

/**
 * Build the `llm_call` node for an OM call that a replay answered live.
 *
 * The prompt keeps captured files as their references, so the node does not
 * repeat file bytes the replay input already names.
 */
export function createLiveOMCallNode(
  invocationId: string,
  index: number,
  call: OMLiveCall,
  sanitize: (value: unknown) => unknown,
  limits: RecordingLimits | undefined,
  isSecretKey: SecretKeyClassifier,
): SessionNodeCreateRequest {
  const lossReasons: string[] = [];
  const encode = (value: unknown, label: string): JsonValue => {
    try {
      const evidence = encodeMemoryEvidence(
        sanitize(value),
        label,
        limits,
        isSecretKey,
      );
      if (evidence.lossReason) lossReasons.push(evidence.lossReason);
      return evidence.value;
    } catch {
      lossReasons.push(`${label} could not be recorded.`);
      return null;
    }
  };
  // The tape keeps the result in codec form; encoding that form again would
  // reject its own reserved keys.
  let output: unknown = null;
  try {
    output = decodeMemoryValue(call.output, isSecretKey);
  } catch {
    lossReasons.push("Live observational-memory result could not be recorded.");
  }
  let model: string | null = null;
  try {
    model = getMemoryModelId(call.model);
  } catch {
    // The node still shows the call; only its model name is unknown.
  }
  return {
    external_id: `${invocationId}:om-live-call:${index}`,
    parent_external_id: ROOT_NODE_EXTERNAL_ID,
    node_type: "llm_call",
    name: `om_${call.phase}_live_call`,
    status: call.failed ? "failed" : "completed",
    ...(call.failed ? { error: "Observational-memory model call failed" } : {}),
    inputs: encode(
      { prompt: call.prompt },
      "Live observational-memory request",
    ),
    outputs: encode(output, "Live observational-memory result"),
    model,
    started_at: call.startedAt,
    ended_at: call.endedAt,
    attributes: {
      invocation_id: invocationId,
      om_phase: call.phase,
      om_method: call.method,
      om_live: true,
      evidence_complete: call.captured && lossReasons.length === 0,
      evidence_loss_reasons: lossReasons,
    },
  };
}
