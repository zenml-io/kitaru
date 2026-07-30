import { useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, useOne } from "../api/hooks";
import type { Experiment, ExperimentRun } from "../api/types";
import { StatusBadge } from "../components/Badge";
import {
  FilterBar,
  type FilterDef,
  useFilterValues,
} from "../components/FilterBar";
import { IdLink, IdText } from "../components/IdLink";
import { KeyValue } from "../components/KeyValue";
import { PageHeader, SectionHeading } from "../components/PageHeader";
import { ReplayConfig } from "../components/ReplayConfig";
import { ErrorNote, Loading } from "../components/States";
import type { Column } from "../components/Table";
import { DataTable } from "../components/Table";
import { formatDate, formatDuration } from "../lib/format";

const FILTERS: FilterDef[] = [
  { key: "name", label: "Name", type: "text" },
  { key: "tag", label: "Tag", type: "text" },
];

const EXPERIMENT_COLUMNS: Column<Experiment>[] = [
  {
    header: "ID",
    cell: (experiment) => (
      <IdLink id={experiment.id} to={`/experiments/${experiment.id}`} />
    ),
  },
  { header: "Name", cell: (experiment) => experiment.name },
  {
    header: "Description",
    cell: (experiment) => experiment.description ?? "—",
    className: "max-w-96 truncate text-zinc-500",
  },
  {
    header: "Evaluators",
    cell: (experiment) => String(experiment.evaluators.length),
  },
  {
    header: "Created",
    cell: (experiment) => formatDate(experiment.created),
    className: "whitespace-nowrap text-zinc-500",
  },
];

export function ExperimentsPage() {
  const filters = useFilterValues(FILTERS);
  const list = useList(["experiments", filters], (cursor) =>
    unwrap(
      client.GET("/v1/experiments", {
        params: {
          query: { cursor, size: 50, name: filters.name, tag: filters.tag },
        },
      }),
    ),
  );

  return (
    <div>
      <PageHeader
        title="Experiments"
        subtitle="Pure replay configuration — tool policy, override, evaluators. Runs bind a cohort and an agent version."
      />
      <FilterBar filters={FILTERS} />
      <DataTable
        list={list}
        columns={EXPERIMENT_COLUMNS}
        rowKey={(experiment) => experiment.id}
        rowLink={(experiment) => `/experiments/${experiment.id}`}
        emptyMessage="No experiments created."
      />
    </div>
  );
}

function runProgressSummary(run: ExperimentRun): string {
  const progress = run.progress;
  return `${progress.completed}/${progress.total} done · ${progress.failed} failed`;
}

const RUN_COLUMNS: Column<ExperimentRun>[] = [
  {
    header: "Run",
    cell: (run) => <IdLink id={run.id} to={`/runs/${run.id}`} />,
  },
  { header: "#", cell: (run) => String(run.number) },
  { header: "Status", cell: (run) => <StatusBadge status={run.status} /> },
  {
    header: "Progress",
    cell: (run) => runProgressSummary(run),
    className: "whitespace-nowrap",
  },
  {
    header: "Cohort",
    cell: (run) => (
      <IdLink id={run.cohort_id} to={`/cohorts/${run.cohort_id}`} />
    ),
  },
  {
    header: "Agent version",
    cell: (run) => <IdText id={run.agent_version_id} />,
  },
  {
    header: "Started",
    cell: (run) => formatDate(run.started_at),
    className: "whitespace-nowrap text-zinc-500",
  },
  {
    header: "Duration",
    cell: (run) => formatDuration(run.started_at, run.ended_at),
    className: "whitespace-nowrap",
  },
];

export function ExperimentDetailPage() {
  const { id } = useParams<{ id: string }>();
  const experimentId = id ?? "";

  const experiment = useOne(["experiments", experimentId], () =>
    unwrap(
      client.GET("/v1/experiments/{experiment_id}", {
        params: { path: { experiment_id: experimentId } },
      }),
    ),
  );
  const runs = useList(
    ["experiment-runs", { experiment_id: experimentId }],
    (cursor) =>
      unwrap(
        client.GET("/v1/experiment-runs", {
          params: {
            query: { cursor, size: 50, experiment_id: experimentId },
          },
        }),
      ),
  );

  if (experiment.isPending) {
    return <Loading />;
  }
  if (experiment.isError) {
    return <ErrorNote error={experiment.error} />;
  }
  const data = experiment.data;

  return (
    <div>
      <PageHeader title={data.name} subtitle={<IdText id={data.id} />} />
      <div className="mb-4">
        <KeyValue
          entries={[
            { label: "Description", value: data.description },
            { label: "Created", value: formatDate(data.created) },
          ]}
        />
      </div>
      <div className="mb-4">
        <ReplayConfig
          toolPolicy={data.tool_policy}
          override={data.override}
          evaluators={data.evaluators}
        />
      </div>
      <SectionHeading>Runs</SectionHeading>
      <DataTable
        list={runs}
        columns={RUN_COLUMNS}
        rowKey={(run) => run.id}
        rowLink={(run) => `/runs/${run.id}`}
        emptyMessage="This experiment has not been run."
      />
    </div>
  );
}
