import { useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, useOne } from "../api/hooks";
import type { Cohort, Session } from "../api/types";
import { StatusBadge } from "../components/Badge";
import {
  FilterBar,
  type FilterDef,
  useFilterValues,
} from "../components/FilterBar";
import { IdLink, IdText } from "../components/IdLink";
import { KeyValue } from "../components/KeyValue";
import { PageHeader } from "../components/PageHeader";
import { ErrorNote, Loading } from "../components/States";
import type { Column } from "../components/Table";
import { DataTable } from "../components/Table";
import { formatCost, formatDate, formatDuration } from "../lib/format";

const FILTERS: FilterDef[] = [
  { key: "name", label: "Name", type: "text" },
  { key: "tag", label: "Tag", type: "text" },
];

const COHORT_COLUMNS: Column<Cohort>[] = [
  {
    header: "ID",
    cell: (cohort) => <IdLink id={cohort.id} to={`/cohorts/${cohort.id}`} />,
  },
  { header: "Name", cell: (cohort) => cohort.name },
  {
    header: "Description",
    cell: (cohort) => cohort.description ?? "—",
    className: "max-w-96 truncate text-zinc-500",
  },
  {
    header: "Agent",
    cell: (cohort) => (
      <IdLink id={cohort.agent_id} to={`/agents/${cohort.agent_id}`} />
    ),
  },
  { header: "Sessions", cell: (cohort) => String(cohort.session_count) },
  {
    header: "Created",
    cell: (cohort) => formatDate(cohort.created),
    className: "whitespace-nowrap text-zinc-500",
  },
];

export function CohortsPage() {
  const filters = useFilterValues(["name", "tag"]);
  const list = useList(["cohorts", filters], (cursor) =>
    unwrap(
      client.GET("/v1/cohorts", {
        params: {
          query: { cursor, size: 50, name: filters.name, tag: filters.tag },
        },
      }),
    ),
  );

  return (
    <div>
      <PageHeader
        title="Cohorts"
        subtitle="Immutable, ordered snapshots of sessions belonging to one agent."
      />
      <FilterBar filters={FILTERS} />
      <DataTable
        list={list}
        columns={COHORT_COLUMNS}
        rowKey={(cohort) => cohort.id}
        rowLink={(cohort) => `/cohorts/${cohort.id}`}
        emptyMessage="No cohorts created."
      />
    </div>
  );
}

const MEMBER_COLUMNS: Column<Session>[] = [
  {
    header: "ID",
    cell: (session) => (
      <IdLink id={session.id} to={`/sessions/${session.id}`} />
    ),
  },
  {
    header: "Name",
    cell: (session) => session.name ?? "—",
    className: "max-w-56 truncate",
  },
  {
    header: "Origin",
    cell: (session) => <StatusBadge status={session.origin} />,
  },
  {
    header: "Status",
    cell: (session) => <StatusBadge status={session.status} />,
  },
  {
    header: "Started",
    cell: (session) => formatDate(session.started_at),
    className: "whitespace-nowrap text-zinc-500",
  },
  {
    header: "Duration",
    cell: (session) => formatDuration(session.started_at, session.ended_at),
    className: "whitespace-nowrap",
  },
  {
    header: "Cost",
    cell: (session) => formatCost(session.cost),
    className: "whitespace-nowrap",
  },
];

export function CohortDetailPage() {
  const { id } = useParams<{ id: string }>();
  const cohortId = id ?? "";

  const cohort = useOne([`cohorts/${cohortId}`], () =>
    unwrap(
      client.GET("/v1/cohorts/{cohort_id}", {
        params: { path: { cohort_id: cohortId } },
      }),
    ),
  );
  const members = useList([`cohorts/${cohortId}/sessions`], (cursor) =>
    unwrap(
      client.GET("/v1/cohorts/{cohort_id}/sessions", {
        params: { path: { cohort_id: cohortId }, query: { cursor, size: 50 } },
      }),
    ),
  );

  if (cohort.isPending) {
    return <Loading />;
  }
  if (cohort.isError) {
    return <ErrorNote error={cohort.error} />;
  }
  const data = cohort.data;

  return (
    <div>
      <PageHeader title={data.name} subtitle={<IdText id={data.id} />} />
      <div className="mb-4">
        <KeyValue
          entries={[
            { label: "Description", value: data.description },
            {
              label: "Agent",
              value: (
                <IdLink id={data.agent_id} to={`/agents/${data.agent_id}`} />
              ),
            },
            { label: "Sessions", value: String(data.session_count) },
            { label: "Created", value: formatDate(data.created) },
          ]}
        />
      </div>
      <h2 className="mb-2 font-medium text-sm text-zinc-700">
        Member sessions (cohort order)
      </h2>
      <DataTable
        list={members}
        columns={MEMBER_COLUMNS}
        rowKey={(session) => session.id}
        rowLink={(session) => `/sessions/${session.id}`}
        emptyMessage="No member sessions."
      />
    </div>
  );
}
