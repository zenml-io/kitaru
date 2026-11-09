import type { MemoryStorage } from "@mastra/core/storage";

// The pinned public MemoryStorage mutation inventory. Delegation binds `this` to
// the original domain, so a native method's own helper calls record only once.
export const MEMORY_MUTATIONS: ReadonlySet<PropertyKey> = new Set<
  | keyof MemoryStorage
  | "commitActiveObservations"
  | "appendBufferedObservations"
>([
  "dangerouslyClearAll",
  "prune",
  "saveThread",
  "updateThread",
  "patchThread",
  "deleteThread",
  "saveMessages",
  "updateMessages",
  "deleteMessages",
  "copyThread",
  "cloneThread",
  "updateThreadResourceId",
  "saveResource",
  "updateResource",
  "initializeObservationalMemory",
  "updateActiveObservations",
  "commitActiveObservations",
  "updateBufferedObservations",
  "appendBufferedObservations",
  "swapBufferedToActive",
  "createReflectionGeneration",
  "updateBufferedReflection",
  "swapBufferedReflectionToActive",
  "setReflectingFlag",
  "setObservingFlag",
  "setBufferingObservationFlag",
  "setBufferingReflectionFlag",
  "insertObservationalMemoryRecord",
  "clearObservationalMemory",
  "setPendingMessageTokens",
  "updateObservationalMemoryConfig",
]);
