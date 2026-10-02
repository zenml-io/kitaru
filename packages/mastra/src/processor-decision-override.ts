import type { ReplayOverride } from "@zenml-io/kitaru";
import { parseModelSettings } from "@zenml-io/kitaru/adapter";

/** Read the Mastra-only setting without forwarding it to the actor model. */
export function parseProcessorDecisionOverride(override?: ReplayOverride): {
  mode: "live" | "pinned";
  modelSettings: ReturnType<typeof parseModelSettings>;
} {
  const { mastraProcessorDecisions, ...modelParams } =
    override?.model_params ?? {};
  const mode =
    mastraProcessorDecisions === undefined ? "live" : mastraProcessorDecisions;
  if (mode !== "live" && mode !== "pinned") {
    throw new TypeError("mastraProcessorDecisions must be 'live' or 'pinned'");
  }
  return {
    mode,
    modelSettings: parseModelSettings(modelParams),
  };
}
