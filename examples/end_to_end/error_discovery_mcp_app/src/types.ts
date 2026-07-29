export type Judgment = "acceptable" | "problematic" | "uncertain";
export type BinaryLabel = "Pass" | "Fail";
export type ReviewPhase =
  | "discovery"
  | "hypotheses"
  | "definition"
  | "rubric"
  | "validation"
  | "revealed";

export interface JsonObject {
  [key: string]: unknown;
}

export interface TraceStep {
  index: number;
  name: string;
  kind: string;
  arguments: JsonObject;
  result: unknown;
  blocked: boolean;
  wroteState: boolean;
  evidenceIds: string[];
}

export interface TraceOutcome {
  policyLabel: string;
  requiredAction: string;
  riskStatus: string;
  evidenceIds: string[];
  toolNames: string[];
}

export interface PublicTrace {
  id: string;
  scenarioId: string;
  scenarioTitle: string;
  configuration: string;
  model: string;
  userRequest: string;
  finalResponse: string;
  outcome: TraceOutcome;
  steps: TraceStep[];
  provenance: {
    sourceTraceId: string;
    sourceFixture: string;
    generationRunId: string;
    agentVersion: string;
  };
}

export interface PrivateHeldoutLabel {
  family: "permission-boundary" | "insufficient-evidence";
  traceId: string;
  label: BinaryLabel;
  rationale: string;
}

export interface Annotation {
  traceId: string;
  note: string;
  judgment?: Judgment;
  updatedAt: string;
}

export interface Hypothesis {
  id: string;
  title: string;
  definition: string;
  evidenceTraceIds: string[];
  counterexampleTraceIds: string[];
  ambiguity: string;
}

export interface FailureModeDraft {
  name: string;
  definition: string;
  inScope: Array<{ traceId: string; why: string }>;
  outOfScope: Array<{ traceId: string; why: string }>;
}

export interface AcceptedFailureMode extends FailureModeDraft {
  id: string;
  hash: string;
  acceptedAt: string;
}

export interface ScorerDemonstration {
  traceId: string;
  label: BinaryLabel;
  why: string;
}

export interface ScorerRubricDraft {
  name: string;
  criterion: string;
  passDefinition: string;
  failDefinition: string;
  demonstrations: ScorerDemonstration[];
}

export interface AcceptedScorerRubric extends ScorerRubricDraft {
  id: string;
  failureModeId: string;
  failureModeHash: string;
  hash: string;
  acceptedAt: string;
}

export interface Prediction {
  traceId: string;
  prediction: BinaryLabel;
  rationale: string;
}

export interface ScorerRun {
  id: string;
  scorerId: string;
  scorerHash: string;
  failureModeHash: string;
  predictions: Prediction[];
  recordedAt: string;
}

export interface HeldoutBinding {
  scorerId: string;
  scorerHash: string;
  failureModeHash: string;
  family: PrivateHeldoutLabel["family"];
  retrievedAt: string;
}

export interface ValidationRow extends Prediction {
  humanLabel: BinaryLabel;
  humanRationale: string;
  result: "agreement" | "false-pass" | "false-fail";
}

export interface ProvisionalSuggestion {
  id: string;
  sourceTraceId: string;
  candidateTraceId: string;
  reason: string;
  status: "provisional" | "compare" | "dismissed";
}

export interface PendingConfirmation<T> {
  token: string;
  hash: string;
  draft: T;
  expiresAt: number;
}

export interface ReviewSession {
  id: string;
  revision: number;
  phase: ReviewPhase;
  createdAt: string;
  discoveryOrder: string[];
  currentTraceId: string;
  comparisonTraceId?: string;
  annotations: Record<string, Annotation>;
  suggestions: Record<string, ProvisionalSuggestion>;
  hypotheses: Hypothesis[];
  reReviewRequired: string[];
  batchReviewedAt?: string;
  pendingFailureMode?: PendingConfirmation<FailureModeDraft>;
  acceptedFailureMode?: AcceptedFailureMode;
  pendingScorer?: PendingConfirmation<ScorerRubricDraft>;
  acceptedScorer?: AcceptedScorerRubric;
  heldoutBinding?: HeldoutBinding;
  scorerRun?: ScorerRun;
  validationRows?: ValidationRow[];
}

export interface ReviewSnapshot {
  schemaVersion: "baby-vp.review.v1";
  sessionId: string;
  revision: number;
  phase: ReviewPhase;
  progress: {
    reviewed: number;
    total: number;
    scenarios: number;
    minimumReached: boolean;
  };
  queue: Array<{
    traceId: string;
    scenarioTitle: string;
    configuration: string;
    reviewed: boolean;
    judgment?: Judgment;
  }>;
  currentTrace: PublicTrace;
  comparisonTrace?: PublicTrace;
  annotations: Annotation[];
  suggestions: ProvisionalSuggestion[];
  hypotheses: Hypothesis[];
  reReviewRequired: string[];
  acceptedFailureMode?: AcceptedFailureMode;
  acceptedScorer?: AcceptedScorerRubric;
  validationRows?: ValidationRow[];
  nextAction: string;
}
