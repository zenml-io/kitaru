import { useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, usePolledOne } from "../api/hooks";
import {
  isSettled,
  type Job,
  type JobStatus,
  type Task,
  type TaskKind,
  type TaskStatus,
  type Worker,
} from "../api/types";
import { StatusBadge } from "../components/Badge";
import {
  FilterBar,
  type FilterDef,
  useFilterValues,
} from "../components/FilterBar";
import { IdLink, IdText } from "../components/IdLink";
import { KeyValue } from "../components/KeyValue";
import { PageHeader, SectionHeading } from "../components/PageHeader";
import { ErrorBanner, ErrorNote, Loading } from "../components/States";
import type { Column } from "../components/Table";
import { DataTable } from "../components/Table";
import { Tabs } from "../components/Tabs";
import { formatDate, formatDuration, shortId } from "../lib/format";

// Deliberately no link to GET /v1/tasks/{id}/spec anywhere on this page —
// the spec payload contains decrypted secret environment values.

function LiveDot({ live }: { live: boolean }) {
  return (
    <span className="flex items-center gap-1.5">
      <span
        className={`inline-block h-2 w-2 rounded-full ${
          live ? "bg-emerald-500" : "bg-zinc-300"
        }`}
      />
      {live ? "live" : "offline"}
    </span>
  );
}

const WORKER_COLUMNS: Column<Worker>[] = [
  { header: "Name", cell: (worker) => worker.name },
  { header: "Status", cell: (worker) => <LiveDot live={worker.live} /> },
  {
    header: "Platform",
    cell: (worker) =>
      [worker.runtime.platform, worker.runtime.hostname]
        .filter(Boolean)
        .join(" · "),
    className: "text-zinc-500",
  },
  {
    header: "Kitaru",
    cell: (worker) => worker.runtime.kitaru_version ?? "—",
    className: "text-zinc-500",
  },
  {
    header: "Task kinds",
    cell: (worker) => worker.scope.kinds?.join(", ") ?? "all",
    className: "text-zinc-500",
  },
  {
    header: "Last seen",
    cell: (worker) => formatDate(worker.last_seen_at),
    className: "whitespace-nowrap text-zinc-500",
  },
];

function WorkersTab() {
  const list = useList(
    ["workers"],
    (cursor) =>
      unwrap(
        client.GET("/v1/workers", {
          params: { query: { cursor, size: 50 } },
        }),
      ),
    // Refetch so the live flag (derived server-side from last_seen_at vs the
    // liveness timeout) stays current without a manual reload. Note a refetch
    // reloads every loaded page, so the interval stays modest.
    { refetchInterval: 30_000 },
  );
  return (
    <DataTable
      list={list}
      columns={WORKER_COLUMNS}
      rowKey={(worker) => worker.id}
      emptyMessage="No workers registered."
    />
  );
}

export const JOB_COLUMNS: Column<Job>[] = [
  {
    header: "ID",
    cell: (job) => <IdLink id={job.id} to={`/jobs/${job.id}`} />,
  },
  { header: "Status", cell: (job) => <StatusBadge status={job.status} /> },
  {
    header: "Created",
    cell: (job) => formatDate(job.created),
    className: "whitespace-nowrap text-zinc-500",
  },
  {
    header: "Started",
    cell: (job) => formatDate(job.started_at),
    className: "whitespace-nowrap text-zinc-500",
  },
  {
    header: "Duration",
    cell: (job) => formatDuration(job.started_at, job.ended_at),
    className: "whitespace-nowrap",
  },
  {
    header: "Error",
    cell: (job) => job.error ?? "—",
    className: "max-w-96 truncate text-zinc-500",
  },
];

const JOB_FILTERS: FilterDef[] = [
  {
    key: "job_status",
    label: "Status",
    type: "select",
    options: ["pending", "running", "completed", "failed", "canceled"].map(
      (status) => ({ value: status, label: status }),
    ),
  },
];

function JobsTab() {
  const filters = useFilterValues(JOB_FILTERS);
  const list = useList(["jobs", filters], (cursor) =>
    unwrap(
      client.GET("/v1/jobs", {
        params: {
          query: {
            cursor,
            size: 50,
            status: filters.job_status as JobStatus | undefined,
          },
        },
      }),
    ),
  );
  return (
    <div>
      <FilterBar filters={JOB_FILTERS} />
      <DataTable
        list={list}
        columns={JOB_COLUMNS}
        rowKey={(job) => job.id}
        rowLink={(job) => `/jobs/${job.id}`}
        emptyMessage="No jobs."
      />
    </div>
  );
}

const TASK_COLUMNS: Column<Task>[] = [
  { header: "ID", cell: (task) => <IdText id={task.id} /> },
  { header: "Kind", cell: (task) => task.kind },
  { header: "Status", cell: (task) => <StatusBadge status={task.status} /> },
  {
    header: "Job",
    cell: (task) => <IdLink id={task.job_id} to={`/jobs/${task.job_id}`} />,
  },
  {
    header: "Worker",
    cell: (task) => (task.worker_id ? <IdText id={task.worker_id} /> : "—"),
  },
  {
    header: "Result session",
    cell: (task) =>
      task.result_session_id ? (
        <IdLink
          id={task.result_session_id}
          to={`/sessions/${task.result_session_id}`}
        />
      ) : (
        "—"
      ),
  },
  { header: "Attempt", cell: (task) => String(task.attempt) },
  {
    header: "Duration",
    cell: (task) => formatDuration(task.started_at, task.ended_at),
    className: "whitespace-nowrap",
  },
  {
    header: "Error",
    cell: (task) => task.error ?? "—",
    className: "max-w-72 truncate text-zinc-500",
  },
];

const TASK_FILTERS: FilterDef[] = [
  {
    key: "kind",
    label: "Kind",
    type: "select",
    options: ["agent", "evaluator", "importer"].map((kind) => ({
      value: kind,
      label: kind,
    })),
  },
  {
    key: "task_status",
    label: "Status",
    type: "select",
    options: [
      "pending",
      "claimed",
      "running",
      "completed",
      "failed",
      "timed_out",
      "canceled",
      "abandoned",
    ].map((status) => ({ value: status, label: status })),
  },
  { key: "job_id", label: "Job ID", type: "text" },
  { key: "worker_id", label: "Worker ID", type: "text" },
];

function TasksTab() {
  const filters = useFilterValues(TASK_FILTERS);
  const list = useList(["tasks", filters], (cursor) =>
    unwrap(
      client.GET("/v1/tasks", {
        params: {
          query: {
            cursor,
            size: 50,
            kind: filters.kind as TaskKind | undefined,
            status: filters.task_status as TaskStatus | undefined,
            job_id: filters.job_id,
            worker_id: filters.worker_id,
          },
        },
      }),
    ),
  );
  return (
    <div>
      <FilterBar filters={TASK_FILTERS} />
      <DataTable
        list={list}
        columns={TASK_COLUMNS}
        rowKey={(task) => task.id}
        emptyMessage="No tasks."
      />
    </div>
  );
}

export function OpsPage() {
  return (
    <div>
      <PageHeader
        title="Ops"
        subtitle="Workers, jobs, and the tasks they execute."
      />
      <Tabs
        tabs={[
          { id: "workers", label: "Workers", content: <WorkersTab /> },
          { id: "jobs", label: "Jobs", content: <JobsTab /> },
          { id: "tasks", label: "Tasks", content: <TasksTab /> },
        ]}
      />
    </div>
  );
}

function JobTasks({ jobId }: { jobId: string }) {
  const list = useList(["jobs", jobId, "tasks"], (cursor) =>
    unwrap(
      client.GET("/v1/jobs/{job_id}/tasks", {
        params: { path: { job_id: jobId }, query: { cursor, size: 50 } },
      }),
    ),
  );
  return (
    <DataTable
      list={list}
      columns={TASK_COLUMNS}
      rowKey={(task) => task.id}
      emptyMessage="No tasks in this job."
    />
  );
}

export function JobDetailPage() {
  const { id } = useParams<{ id: string }>();
  const jobId = id ?? "";

  const job = usePolledOne(
    ["jobs", jobId],
    () =>
      unwrap(
        client.GET("/v1/jobs/{job_id}", {
          params: { path: { job_id: jobId } },
        }),
      ),
    isSettled,
  );

  if (job.isPending) {
    return <Loading />;
  }
  if (job.isError) {
    return <ErrorNote error={job.error} />;
  }
  const data = job.data;

  return (
    <div>
      <PageHeader
        title={`Job ${shortId(data.id)}`}
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
            { label: "Created", value: formatDate(data.created) },
            { label: "Started", value: formatDate(data.started_at) },
            { label: "Ended", value: formatDate(data.ended_at) },
            {
              label: "Duration",
              value: formatDuration(data.started_at, data.ended_at),
            },
            {
              label: "Cancel requested",
              value: data.cancel_requested_at
                ? formatDate(data.cancel_requested_at)
                : null,
            },
          ]}
        />
      </div>
      {data.error && <ErrorBanner message={data.error} />}
      <SectionHeading>Tasks</SectionHeading>
      <JobTasks jobId={jobId} />
    </div>
  );
}
