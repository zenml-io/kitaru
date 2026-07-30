import type { components } from "./schema";

type Schemas = components["schemas"];

export type Agent = Schemas["AgentResponse"];
export type AgentVersion = Schemas["AgentVersionResponse"];
export type Session = Schemas["SessionResponse"];
export type SessionNode = Schemas["SessionNodeResponse"];
export type Cohort = Schemas["CohortResponse"];
export type Experiment = Schemas["ExperimentResponse"];
export type ExperimentRun = Schemas["ExperimentRunResponse"];
export type ExperimentRunProgress = Schemas["ExperimentRunProgress"];
export type Replay = Schemas["ReplayResponse"];
export type Evaluation = Schemas["EvaluationResponse"];
export type Evaluator = Schemas["EvaluatorResponse"];
export type EvaluatorVersion = Schemas["EvaluatorVersionResponse"];
export type Job = Schemas["JobResponse"];
export type Task = Schemas["TaskResponse"];
export type Worker = Schemas["WorkerResponse"];

export interface Page<T> {
  items: T[];
  next_cursor: string | null;
}

export type SessionOrigin = Session["origin"];
export type SessionStatus = Session["status"];
export type NodeType = SessionNode["node_type"];
export type ReplayStatus = Replay["status"];
export type JobStatus = Job["status"];
export type TaskStatus = Task["status"];
export type TaskKind = Task["kind"];

// Runs, replays, and jobs share the same terminal vocabulary.
const SETTLED_STATUSES: ReadonlySet<string> = new Set([
  "completed",
  "failed",
  "canceled",
]);

export function isSettled(resource: { status: string }): boolean {
  return SETTLED_STATUSES.has(resource.status);
}
