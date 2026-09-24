import {
  MastraReplayBudgetError,
  RecordedSensitiveKeyError,
} from "@zenml-io/kitaru/adapter";

/**
 * Why a recorded Mastra memory turn cannot be replayed.
 *
 * A baseline session stores the code in its `mastra_replay_reason` metadata,
 * and `onRecordingError` receives it as `reason`.
 */
export type MastraReplayReason =
  /** The initial memory or another part of the replay input exceeds the replay size budget. */
  | "replay_input_too_large"
  /** Memory, request context, or evidence holds a credential-named key such as `token` or `password`. */
  | "credential_key_unsupported"
  /** Observational memory uses custom extractors or models without a static identity. */
  | "om_config_unsupported"
  /** The memory configuration uses options outside isolated replay support. */
  | "memory_config_unsupported"
  /** The agent or its run options use features outside isolated replay support. */
  | "agent_config_unsupported"
  /** The actor model has no static identity, for example a function or a fallback array. */
  | "model_identity_unsupported"
  /** The memory store returned records Kitaru cannot represent or validate. */
  | "memory_store_shape_unsupported"
  /** Stored observational-memory records show work that was still running or never cleared. */
  | "om_work_unjoined"
  /** Reading the initial memory state from storage failed. */
  | "memory_read_failed"
  /** Reading the initial memory state did not finish in time. */
  | "memory_capture_timeout"
  /** Another writer overlapped this turn, or a write happened without the lease. */
  | "memory_lease_conflict"
  /** The lease backend failed or did not answer in time. */
  | "memory_lease_unavailable"
  /** A native memory storage write failed. */
  | "memory_mutation_failed"
  /** An observational-memory model result could not be recorded. */
  | "om_tape_incomplete"
  /** Observational-memory work did not settle before the finalization deadline. */
  | "om_settle_timeout"
  /** The model request sent to the provider could not be recorded faithfully. */
  | "request_evidence_incomplete"
  /** Recorded evidence contains a value the replay codec cannot represent. */
  | "recorded_evidence_unsupported"
  /** Request context changed after the turn's memory was captured. */
  | "context_mutated_after_capture"
  /** Request context cannot be captured or conflicts with the memory selectors. */
  | "context_unsupported"
  /** The installed Mastra packages are not the versions memory replay supports. */
  | "version_mismatch"
  /** Declared files did not download before the setup wait ended. */
  | "file_capture_timeout"
  /** Replay setup failed for a reason no other code describes. */
  | "capture_setup_failed"
  /** The replay input could not be assembled for a reason no other code describes. */
  | "capture_prerequisite_failed"
  /** Memory evidence is incomplete for a reason no other code describes. */
  | "memory_evidence_incomplete"
  /** Kitaru did not open the recording session before the setup wait ended. */
  | "recording_setup_timeout"
  /** Kitaru could not open the recording session. */
  | "recording_setup_failed"
  /** Kitaru did not accept a step's evidence. */
  | "recording_step_failed"
  /** Kitaru did not accept the evidence before the flush deadline. */
  | "recording_flush_timeout"
  /** Kitaru could not store a memory change as evidence. */
  | "recording_evidence_failed"
  /** Finalizing the recording failed for a reason no other code describes. */
  | "recording_finalization_failed"
  /** The final session update failed. */
  | "recording_completion_failed"
  /** The server refused the replay inputs, usually because it predates memory replay. */
  | "server_rejected_finalization"
  /** The native turn itself failed. */
  | "native_run_failed";

/** A replay failure whose message the adapter wrote, optionally naming its reason code. */
export class MastraReplayReasonError extends Error {
  constructor(
    message: string,
    readonly reason?: MastraReplayReason,
  ) {
    super(message);
  }
}

/** Build the error for input outside memory replay support. */
export function unsupportedMemoryReplay(
  message: string,
  reason?: MastraReplayReason,
): MastraReplayReasonError {
  return new MastraReplayReasonError(
    `Unsupported Mastra memory replay: ${message}`,
    reason,
  );
}

/** Return the reason code an error names, or `fallback` for any other error. */
export function getReplayReason(
  error: unknown,
  fallback: MastraReplayReason,
): MastraReplayReason {
  if (error instanceof MastraReplayReasonError) return error.reason ?? fallback;
  if (error instanceof MastraReplayBudgetError) return "replay_input_too_large";
  if (error instanceof RecordedSensitiveKeyError)
    return "credential_key_unsupported";
  return fallback;
}

/**
 * Describe a replay failure without copying text from storage or providers.
 *
 * Only messages the adapter or its codec wrote are returned; any other error
 * is described by `fallback`.
 */
export function describeReplayFailure(
  error: unknown,
  fallback: string,
): string {
  return error instanceof MastraReplayReasonError ||
    error instanceof MastraReplayBudgetError ||
    error instanceof RecordedSensitiveKeyError
    ? error.message
    : fallback;
}
