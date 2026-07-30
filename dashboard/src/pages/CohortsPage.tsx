import { useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, useOne } from "../api/hooks";
import type { Cohort } from "../api/types";
import {
  FilterBar,
  type FilterDef,
  useFilterValues,
} from "../components/FilterBar";
import { IdLink, IdText } from "../components/IdLink";
import { KeyValue } from "../components/KeyValue";
import { PageHeader, SectionHeading } from "../components/PageHeader";
import { ErrorNote, Loading } from "../components/States";
import { type Column, DataTable } from "../components/Table";
import { formatDate } from "../lib/format";
import { SESSION_COLUMNS } from "./SessionsPage";

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
  const filters = useFilterValues(FILTERS);
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

// Cohort members reuse the sessions table's columns, minus the agent-scoped
// and call-volume ones that add little inside a single-agent cohort.
const MEMBER_HEADERS = new Set([
  "ID",
  "Name",
  "Origin",
  "Status",
  "Started",
  "Duration",
  "Cost",
]);
const MEMBER_COLUMNS = SESSION_COLUMNS.filter((column) =>
  MEMBER_HEADERS.has(column.header),
);

export function CohortDetailPage() {
  const { id } = useParams<{ id: string }>();
  const cohortId = id ?? "";

  const cohort = useOne(["cohorts", cohortId], () =>
    unwrap(
      client.GET("/v1/cohorts/{cohort_id}", {
        params: { path: { cohort_id: cohortId } },
      }),
    ),
  );
  const members = useList(["cohorts", cohortId, "sessions"], (cursor) =>
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
      <SectionHeading>Member sessions (cohort order)</SectionHeading>
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
