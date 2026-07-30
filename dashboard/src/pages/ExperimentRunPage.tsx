import { useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, usePolledOne } from "../api/hooks";
import { type ExperimentRunProgress, isSettled } from "../api/types";
import { StatusBadge } from "../components/Badge";
import { IdLink, IdText } from "../components/IdLink";
import { KeyValue } from "../components/KeyValue";
import { PageHeader } from "../components/PageHeader";
import { ErrorBanner, ErrorNote, Loading } from "../components/States";
import { DataTable } from "../components/Table";
import { Tabs } from "../components/Tabs";
import { formatDate, formatDuration, shortId } from "../lib/format";
import { JOB_COLUMNS } from "./OpsPage";
import { REPLAY_COLUMNS } from "./ReplaysPage";

const PROGRESS_SEGMENTS: {
  key: keyof ExperimentRunProgress;
  label: string;
  className: string;
}[] = [
  { key: "completed", label: "completed", className: "bg-emerald-500" },
  { key: "evaluating", label: "evaluating", className: "bg-amber-400" },
  { key: "failed", label: "failed", className: "bg-red-500" },
  { key: "canceled", label: "canceled", className: "bg-zinc-400" },
  { key: "pending", label: "pending", className: "bg-zinc-200" },
];

function ProgressBar({ progress }: { progress: ExperimentRunProgress }) {
  const total = progress.total;
  return (
    <div>
      <div className="flex h-2.5 w-full overflow-hidden rounded-full bg-zinc-100">
        {total > 0 &&
          PROGRESS_SEGMENTS.map((segment) => {
            const count = progress[segment.key];
            if (count === 0) {
              return null;
            }
            return (
              <div
                key={segment.key}
                className={segment.className}
                style={{ width: `${(count / total) * 100}%` }}
                title={`${segment.label}: ${count}`}
              />
            );
          })}
      </div>
      <div className="mt-1.5 flex flex-wrap gap-3 text-xs text-zinc-500">
        <span>{total} replays</span>
        {PROGRESS_SEGMENTS.map((segment) => (
          <span key={segment.key} className="flex items-center gap-1">
            <span
              className={`inline-block h-2 w-2 rounded-full ${segment.className}`}
            />
            {segment.label}: {progress[segment.key]}
          </span>
        ))}
      </div>
    </div>
  );
}

function RunReplaysTab({ runId }: { runId: string }) {
  const list = useList(["replays", { experiment_run_id: runId }], (cursor) =>
    unwrap(
      client.GET("/v1/replays", {
        params: { query: { cursor, size: 50, experiment_run_id: runId } },
      }),
    ),
  );
  return (
    <DataTable
      list={list}
      columns={REPLAY_COLUMNS}
      rowKey={(replay) => replay.id}
      rowLink={(replay) => `/replays/${replay.id}`}
      emptyMessage="No replays in this run."
    />
  );
}

function RunJobsTab({ runId }: { runId: string }) {
  const list = useList(["experiment-runs", runId, "jobs"], (cursor) =>
    unwrap(
      client.GET("/v1/experiment-runs/{experiment_run_id}/jobs", {
        params: {
          path: { experiment_run_id: runId },
          query: { cursor, size: 50 },
        },
      }),
    ),
  );
  return (
    <DataTable
      list={list}
      columns={JOB_COLUMNS}
      rowKey={(job) => job.id}
      rowLink={(job) => `/jobs/${job.id}`}
      emptyMessage="No jobs in this run."
    />
  );
}

export function ExperimentRunPage() {
  const { id } = useParams<{ id: string }>();
  const runId = id ?? "";

  const run = usePolledOne(
    ["experiment-runs", runId],
    () =>
      unwrap(
        client.GET("/v1/experiment-runs/{experiment_run_id}", {
          params: { path: { experiment_run_id: runId } },
        }),
      ),
    isSettled,
  );

  if (run.isPending) {
    return <Loading />;
  }
  if (run.isError) {
    return <ErrorNote error={run.error} />;
  }
  const data = run.data;

  return (
    <div>
      <PageHeader
        title={`Run #${data.number} · ${shortId(data.id)}`}
        subtitle={
          <span className="flex items-center gap-2">
            <StatusBadge status={data.status} />
            <IdText id={data.id} />
            {!isSettled(data) && (
              <span className="text-xs text-zinc-400">(auto-refreshing)</span>
            )}
          </span>
        }
      />
      <div className="mb-4">
        <ProgressBar progress={data.progress} />
      </div>
      <div className="mb-4">
        <KeyValue
          entries={[
            {
              label: "Experiment",
              value: (
                <IdLink
                  id={data.experiment_id}
                  to={`/experiments/${data.experiment_id}`}
                />
              ),
            },
            {
              label: "Cohort",
              value: (
                <IdLink id={data.cohort_id} to={`/cohorts/${data.cohort_id}`} />
              ),
            },
            {
              label: "Agent version",
              value: <IdText id={data.agent_version_id} />,
            },
            {
              label: "Evaluate baselines",
              value: String(data.evaluate_baselines),
            },
            { label: "Started", value: formatDate(data.started_at) },
            { label: "Ended", value: formatDate(data.ended_at) },
            {
              label: "Duration",
              value: formatDuration(data.started_at, data.ended_at),
            },
          ]}
        />
      </div>
      {data.error && <ErrorBanner message={data.error} />}
      <Tabs
        tabs={[
          {
            id: "replays",
            label: "Replays",
            content: <RunReplaysTab runId={runId} />,
          },
          { id: "jobs", label: "Jobs", content: <RunJobsTab runId={runId} /> },
        ]}
      />
    </div>
  );
}
