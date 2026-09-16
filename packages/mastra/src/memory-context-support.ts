import { parseMemoryRequestContext } from "@mastra/core/memory";
import type { RequestContext } from "@mastra/core/request-context";

/** Check whether a single message snapshot covers the enabled memory behavior. */
export function canCaptureMemoryContext(
  requestContext?: RequestContext,
): boolean {
  try {
    const config = parseMemoryRequestContext(requestContext)?.memoryConfig;
    // These modes can add memory tools or change context between model steps.
    // Removing live memory during replay would remove that behavior as well.
    return (
      config?.workingMemory?.enabled !== true &&
      (config?.semanticRecall === undefined ||
        config.semanticRecall === false) &&
      (config?.observationalMemory === undefined ||
        config.observationalMemory === false)
    );
  } catch {
    // An unrecognized memory context must not claim a replayable snapshot.
    return false;
  }
}
