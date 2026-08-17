import type { components } from "./generated/openapi.js";

export type JsonPrimitive = null | boolean | number | string;
export type JsonValue =
  | JsonPrimitive
  | JsonValue[]
  | { [key: string]: JsonValue };

export type ReplayOverride = components["schemas"]["ReplayOverride"];
export type ToolPolicyConfig = components["schemas"]["ToolPolicy"];
export type ToolPolicy = ToolPolicyConfig["default"];
export type SessionNodeCreateRequest =
  components["schemas"]["SessionNodeCreateRequest"];

export type AccountResponse = components["schemas"]["AccountResponse"];
export type AgentCreateRequest = components["schemas"]["AgentCreateRequest"];
export type AgentResponse = components["schemas"]["AgentResponse"];
export type AgentUpdateRequest = components["schemas"]["AgentUpdateRequest"];
export type AgentVersionCreateRequest =
  components["schemas"]["AgentVersionCreateRequest"];
export type AgentVersionResponse =
  components["schemas"]["AgentVersionResponse"];
export type AgentVersionUpdateRequest =
  components["schemas"]["AgentVersionUpdateRequest"];
export type BlobResponse = components["schemas"]["BlobResponse"];
export type InvestigationCreateRequest =
  components["schemas"]["InvestigationCreateRequest"];
export type InvestigationResponse =
  components["schemas"]["InvestigationResponse"];
export type InvestigationUpdateRequest =
  components["schemas"]["InvestigationUpdateRequest"];
export type InvestigationSessionResponse =
  components["schemas"]["InvestigationSessionResponse"];
export type InvestigationSessionUpdateRequest =
  components["schemas"]["InvestigationSessionUpdateRequest"];
export type ManualAnnotationCreateRequest =
  components["schemas"]["ManualAnnotationCreateRequest"];
export type InvestigationAnswerCreateRequest =
  components["schemas"]["InvestigationAnswerCreateRequest"];
export type AnnotationCreateRequest =
  | ManualAnnotationCreateRequest
  | InvestigationAnswerCreateRequest;
export type AnnotationResponse = components["schemas"]["AnnotationResponse"];
export type AnnotationUpdateRequest =
  components["schemas"]["AnnotationUpdateRequest"];
export type EvaluatorCreateRequest =
  components["schemas"]["EvaluatorCreateRequest"];
export type EvaluatorResponse = components["schemas"]["EvaluatorResponse"];
export type EvaluatorUpdateRequest =
  components["schemas"]["EvaluatorUpdateRequest"];
export type EvaluatorVersionCreateRequest =
  components["schemas"]["EvaluatorVersionCreateRequest"];
export type EvaluatorVersionResponse =
  components["schemas"]["EvaluatorVersionResponse"];
export type EvaluatorVersionUpdateRequest =
  components["schemas"]["EvaluatorVersionUpdateRequest"];
export type EvaluationBatchCreateRequest =
  components["schemas"]["EvaluationBatchCreateRequest"];
export type EvaluationResponse = components["schemas"]["EvaluationResponse"];
export type ExperimentCreateRequest =
  components["schemas"]["ExperimentCreateRequest"];
export type ExperimentResponse = components["schemas"]["ExperimentResponse"];
export type ExperimentUpdateRequest =
  components["schemas"]["ExperimentUpdateRequest"];
export type ExperimentRunCreateRequest =
  components["schemas"]["ExperimentRunCreateRequest"];
export type ExperimentRunResponse =
  components["schemas"]["ExperimentRunResponse"];
export type CohortCreateRequest = components["schemas"]["CohortCreateRequest"];
export type CohortResponse = components["schemas"]["CohortResponse"];
export type CohortUpdateRequest = components["schemas"]["CohortUpdateRequest"];
export type CohortVersionCreateRequest =
  components["schemas"]["CohortVersionCreateRequest"];
export type CohortVersionResponse =
  components["schemas"]["CohortVersionResponse"];
export type CohortVersionUpdateRequest =
  components["schemas"]["CohortVersionUpdateRequest"];
export type JobResponse = components["schemas"]["JobResponse"];
export type ReplayCreateRequest = components["schemas"]["ReplayCreateRequest"];
export type ReplayResponse = components["schemas"]["ReplayResponse"];
export type ReplaySpec = ReplayResponse;
export type ServerInfoResponse = components["schemas"]["ServerInfoResponse"];
export type SessionCreateRequest =
  components["schemas"]["SessionCreateRequest"];
export type SessionNodeBatchRequest =
  components["schemas"]["SessionNodeBatchRequest"];
export type SessionNodeResponse = components["schemas"]["SessionNodeResponse"];
export type SessionResponse = components["schemas"]["SessionResponse"];
export type SessionRunCreateRequest =
  components["schemas"]["SessionRunCreateRequest"];
export type SessionUpdateRequest =
  components["schemas"]["SessionUpdateRequest"];
export type SessionWithNodesResponse =
  components["schemas"]["SessionWithNodesResponse"];
export type TaskResponse = components["schemas"]["TaskResponse"];
export type TaskSpecResponse = components["schemas"]["TaskSpecResponse"];
export type ToolLookupRequest = components["schemas"]["ToolLookupRequest"];
export type ToolLookupResponse = components["schemas"]["ToolLookupResponse"];

export type Filter =
  | components["schemas"]["FilterCondition"]
  | components["schemas"]["AndFilter"]
  | components["schemas"]["OrFilter"]
  | components["schemas"]["NotFilter"];

export interface Page<T> {
  items: T[];
  next_cursor: string | null;
}

export interface ListParams {
  cursor?: string | null;
  size?: number;
  sort?: string;
  filter?: Filter | null;
}

export interface UnfilteredListParams {
  cursor?: string | null;
  size?: number;
  sort?: string;
}

export interface InvestigationSessionListParams {
  cursor?: string | null;
  size?: number;
}

export interface SessionNodeListParams {
  cursor?: string | null;
  size?: number;
  includePayloads?: boolean;
}
