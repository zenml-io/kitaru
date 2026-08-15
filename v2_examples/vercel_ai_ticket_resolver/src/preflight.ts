import type { KitaruEnvironmentVariables } from "@zenml-io/kitaru";

export type ModelProvider = "deterministic" | "openai";

const SUPPORTED_NODE = "Node >=22.22.0 <23 is required";

export function assertSupportedNodeVersion(
  version = process.versions.node,
): void {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:-|$)/.exec(version);
  if (match === null || Number(match[1]) !== 22 || Number(match[2]) < 22) {
    throw new Error(`${SUPPORTED_NODE}; found ${version}`);
  }
}

export function validateWorkflowEnvironment(
  provider: ModelProvider,
  environment: KitaruEnvironmentVariables,
): void {
  if (provider === "deterministic") {
    return;
  }
  if (environment.RETURNS_ALLOW_PAID_MODEL !== "1") {
    throw new Error(
      "Set RETURNS_ALLOW_PAID_MODEL=1 to approve the optional paid model call",
    );
  }
  if (!environment.OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY is required");
  }
}
