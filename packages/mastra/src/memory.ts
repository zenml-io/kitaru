// Re-exported so an application can opt back into name-based credential
// detection without importing the core adapter entry point.
export { isCredentialKeyName } from "@zenml-io/kitaru/adapter";
export type {
  MastraExclusiveMemoryAccess,
  MastraMemoryCaptureBinding,
  MastraMemoryCaptureOptions,
  MastraMemoryLease,
  MastraMemoryLeaseOptions,
  MastraMemoryMutation,
  MastraMemorySelector,
} from "./memory-binding.js";
export {
  createMemoryCaptureBinding,
  createProcessLocalMemoryAccess,
} from "./memory-binding.js";
export type {
  MastraFileManifestEntry,
  MastraMemoryReplayEnvelope,
  MastraMemoryReplayInput,
  MastraMemorySnapshot,
  MastraRecordedFile,
} from "./memory-snapshot.js";
export {
  createMemoryReplayEnvelope,
  decodeMemoryReplayEnvelope,
  decodeMemoryValue,
  encodeMemoryValue,
  MEMORY_REPLAY_KEY,
  restoreMemoryReplayEnvelope,
  validateMemorySnapshot,
} from "./memory-snapshot.js";
export type { MastraReplayReason } from "./replay-reasons.js";
export type {
  DeclareMemoryReplayFiles,
  MemoryReplayAgentBindings,
  MemoryReplayAgentFactory,
  MemoryReplayAgentOptions,
  MemoryReplayFileCall,
} from "./stateful-agent.js";
export { createMemoryReplayAgent } from "./stateful-agent.js";
