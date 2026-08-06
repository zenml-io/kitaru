import type { components } from "./generated/openapi.js";

export type JsonPrimitive = null | boolean | number | string;
export type JsonValue =
  | JsonPrimitive
  | JsonValue[]
  | { [key: string]: JsonValue };

export type ReplayOverride = components["schemas"]["ReplayOverride"];
export type ReplaySpec = components["schemas"]["ReplayResponse"];
export type ToolPolicyConfig = components["schemas"]["ToolPolicy"];
export type ToolPolicy = ToolPolicyConfig["default"];
export type SessionNodeCreateRequest =
  components["schemas"]["SessionNodeCreateRequest"];
