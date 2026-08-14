export type WorkerHandoffPhase = "baseline_evaluation" | "experiment_runs";

export interface WorkerHandoffJob {
  agent_version_id: string | null;
  job_id: string;
  job_kind: "evaluation" | "replay";
}

interface WorkerHandoffInput {
  evidenceSetId: string;
  jobs: readonly WorkerHandoffJob[];
  phase: WorkerHandoffPhase;
}

export interface WorkflowCounts {
  baseline_failures: number;
  baseline_passes: number;
  baseline_sessions: number;
  control_sessions: number;
  experiment_runs: number;
  replay_passes: number;
  replays: number;
  target_sessions: number;
}

export function createWorkerHandoffEvent(input: WorkerHandoffInput) {
  return {
    event: "kitaru.worker_handoff" as const,
    schema_version: 1 as const,
    evidence_set_id: input.evidenceSetId,
    phase: input.phase,
    manifest_relative_path: ".state/workflow.json" as const,
    jobs: [...input.jobs].sort((left, right) =>
      left.job_id.localeCompare(right.job_id),
    ),
  };
}

export function createCompletedEvent(
  evidenceSetId: string,
  counts: WorkflowCounts,
) {
  return {
    event: "kitaru.workflow_completed" as const,
    schema_version: 1 as const,
    evidence_set_id: evidenceSetId,
    counts,
  };
}
