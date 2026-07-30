import type { components } from "./schema";

type Schemas = components["schemas"];

export type ServerInfo = Schemas["ServerInfoResponse"];
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
export type Importer = Schemas["ImporterResponse"];
export type Job = Schemas["JobResponse"];
export type Task = Schemas["TaskResponse"];
export type Worker = Schemas["WorkerResponse"];
export type Tag = Schemas["TagResponse"];

export interface Page<T> {
  items: T[];
  next_cursor: string | null;
}

export type SessionOrigin = Session["origin"];
export type SessionStatus = Session["status"];
export type NodeType = SessionNode["node_type"];
export type ExperimentRunStatus = ExperimentRun["status"];
export type ReplayStatus = Replay["status"];
export type JobStatus = Job["status"];
export type TaskStatus = Task["status"];
export type TaskKind = Task["kind"];

const TERMINAL_RUN_STATUSES: readonly ExperimentRunStatus[] = [
  "completed",
  "failed",
  "canceled",
];

const TERMINAL_REPLAY_STATUSES: readonly ReplayStatus[] = [
  "completed",
  "failed",
  "canceled",
];

const TERMINAL_JOB_STATUSES: readonly JobStatus[] = [
  "completed",
  "failed",
  "canceled",
];

export function isTerminalRun(run: ExperimentRun): boolean {
  return TERMINAL_RUN_STATUSES.includes(run.status);
}

export function isTerminalReplay(replay: Replay): boolean {
  return TERMINAL_REPLAY_STATUSES.includes(replay.status);
}

export function isTerminalJob(job: Job): boolean {
  return TERMINAL_JOB_STATUSES.includes(job.status);
}
