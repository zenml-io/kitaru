import { useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, usePolledOne } from "../api/hooks";
import { isTerminalReplay, type Replay, type ReplayStatus } from "../api/types";
import { StatusBadge } from "../components/Badge";
import {
  FilterBar,
  type FilterDef,
  useFilterValues,
} from "../components/FilterBar";
import { IdLink, IdText } from "../components/IdLink";
import { JsonViewer } from "../components/JsonViewer";
import { KeyValue } from "../components/KeyValue";
import { PageHeader } from "../components/PageHeader";
import { ErrorNote, Loading } from "../components/States";
import type { Column } from "../components/Table";
import { DataTable } from "../components/Table";
import { formatDate, shortId } from "../lib/format";

const FILTERS: FilterDef[] = [
  {
    key: "status",
    label: "Status",
    type: "select",
    options: [
      { value: "pending", label: "pending" },
      { value: "evaluating", label: "evaluating" },
      { value: "completed", label: "completed" },
      { value: "failed", label: "failed" },
      { value: "canceled", label: "canceled" },
    ],
  },
  { key: "experiment_run_id", label: "Experiment run ID", type: "text" },
  { key: "baseline_session_id", label: "Baseline session ID", type: "text" },
];

export const REPLAY_COLUMNS: Column<Replay>[] = [
  {
    header: "ID",
    cell: (replay) => <IdLink id={replay.id} to={`/replays/${replay.id}`} />,
  },
  {
    header: "Status",
    cell: (replay) => <StatusBadge status={replay.status} />,
  },
  {
    header: "Baseline",
    cell: (replay) => (
      <IdLink
        id={replay.baseline_session_id}
        to={`/sessions/${replay.baseline_session_id}`}
      />
    ),
  },
  {
    header: "Result",
    cell: (replay) =>
      replay.result_session_id ? (
        <IdLink
          id={replay.result_session_id}
          to={`/sessions/${replay.result_session_id}`}
        />
      ) : (
        "—"
      ),
  },
  {
    header: "Experiment run",
    cell: (replay) =>
      replay.experiment_run_id ? (
        <IdLink
          id={replay.experiment_run_id}
          to={`/runs/${replay.experiment_run_id}`}
        />
      ) : (
        "standalone"
      ),
  },
  {
    header: "Created",
    cell: (replay) => formatDate(replay.created),
    className: "whitespace-nowrap text-zinc-500",
  },
];

export function ReplaysPage() {
  const filters = useFilterValues([
    "status",
    "experiment_run_id",
    "baseline_session_id",
  ]);
  const list = useList(["replays", filters], (cursor) =>
    unwrap(
      client.GET("/v1/replays", {
        params: {
          query: {
            cursor,
            size: 50,
            status: filters.status as ReplayStatus | undefined,
            experiment_run_id: filters.experiment_run_id,
            baseline_session_id: filters.baseline_session_id,
          },
        },
      }),
    ),
  );

  return (
    <div>
      <PageHeader
        title="Replays"
        subtitle="Each replay re-executes a baseline session and records the result as a new session."
      />
      <FilterBar filters={FILTERS} />
      <DataTable
        list={list}
        columns={REPLAY_COLUMNS}
        rowKey={(replay) => replay.id}
        rowLink={(replay) => `/replays/${replay.id}`}
        emptyMessage="No replays match the current filters."
      />
    </div>
  );
}

export function ReplayDetailPage() {
  const { id } = useParams<{ id: string }>();
  const replayId = id ?? "";

  const replay = usePolledOne(
    [`replays/${replayId}`],
    () =>
      unwrap(
        client.GET("/v1/replays/{replay_id}", {
          params: { path: { replay_id: replayId } },
        }),
      ),
    isTerminalReplay,
  );

  if (replay.isPending) {
    return <Loading />;
  }
  if (replay.isError) {
    return <ErrorNote error={replay.error} />;
  }
  const data = replay.data;

  return (
    <div>
      <PageHeader
        title={`Replay ${shortId(data.id)}`}
        subtitle={
          <span className="flex items-center gap-2">
            <StatusBadge status={data.status} />
            <IdText id={data.id} />
          </span>
        }
      />
      <div className="mb-4">
        <KeyValue
          entries={[
            {
              label: "Baseline session",
              value: (
                <IdLink
                  id={data.baseline_session_id}
                  to={`/sessions/${data.baseline_session_id}`}
                />
              ),
            },
            {
              label: "Result session",
              value: data.result_session_id ? (
                <IdLink
                  id={data.result_session_id}
                  to={`/sessions/${data.result_session_id}`}
                />
              ) : null,
            },
            {
              label: "Experiment run",
              value: data.experiment_run_id ? (
                <IdLink
                  id={data.experiment_run_id}
                  to={`/runs/${data.experiment_run_id}`}
                />
              ) : (
                "standalone"
              ),
            },
            {
              label: "Job",
              value: <IdLink id={data.job_id} to={`/jobs/${data.job_id}`} />,
            },
            {
              label: "Evaluate baselines",
              value: String(data.evaluate_baselines),
            },
            { label: "Created", value: formatDate(data.created) },
          ]}
        />
      </div>
      {data.error && (
        <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-red-700 text-sm">
          {data.error}
        </div>
      )}
      <div className="grid gap-3 lg:grid-cols-2">
        <JsonViewer
          value={data.tool_policy}
          label="Tool policy"
          defaultOpenDepth={2}
        />
        {data.override != null && (
          <JsonViewer
            value={data.override}
            label="Override"
            defaultOpenDepth={2}
          />
        )}
        <JsonViewer
          value={data.evaluators}
          label="Evaluators"
          defaultOpenDepth={2}
        />
      </div>
    </div>
  );
}
