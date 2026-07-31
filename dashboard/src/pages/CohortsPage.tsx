import { Link, useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, useOne } from "../api/hooks";
import type { Cohort, CohortVersion } from "../api/types";
import {
  FilterBar,
  type FilterDef,
  useFilterValues,
} from "../components/FilterBar";
import { IdLink, IdText } from "../components/IdLink";
import { JsonViewer } from "../components/JsonViewer";
import { KeyValue } from "../components/KeyValue";
import { PageHeader, SectionHeading } from "../components/PageHeader";
import { ErrorNote, Loading } from "../components/States";
import type { Column } from "../components/Table";
import { DataTable, LoadMore } from "../components/Table";
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
  { header: "Latest version", cell: (cohort) => `v${cohort.latest_version}` },
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
        subtitle="Named groups of sessions belonging to one agent, versioned over time."
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

function VersionRow({ version }: { version: CohortVersion }) {
  return (
    <div className="border-zinc-100 border-b px-4 py-3 last:border-b-0">
      <div className="flex items-center gap-3">
        <Link
          to={`/cohort-versions/${version.id}`}
          className="font-medium text-indigo-600 text-sm hover:underline"
        >
          v{version.version}
          {version.display_version ? ` · ${version.display_version}` : ""}
        </Link>
        <IdText id={version.id} />
        <span className="text-xs text-zinc-400">
          {version.session_count} sessions
        </span>
        <span className="text-xs text-zinc-400">
          {formatDate(version.created)}
        </span>
      </div>
    </div>
  );
}

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
  const versions = useList(["cohorts", cohortId, "versions"], (cursor) =>
    unwrap(
      client.GET("/v1/cohorts/{cohort_id}/versions", {
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
            { label: "Latest version", value: `v${data.latest_version}` },
            { label: "Created", value: formatDate(data.created) },
          ]}
        />
      </div>
      {Object.keys(data.metadata).length > 0 && (
        <div className="mb-4">
          <JsonViewer value={data.metadata} label="Metadata" />
        </div>
      )}
      <SectionHeading>Versions</SectionHeading>
      {versions.isLoading ? (
        <Loading />
      ) : versions.error ? (
        <ErrorNote error={versions.error} />
      ) : (
        <div className="rounded-lg border border-zinc-200 bg-white">
          {versions.items.length === 0 && (
            <div className="px-4 py-3 text-sm text-zinc-400">No versions.</div>
          )}
          {versions.items.map((version) => (
            <VersionRow key={version.id} version={version} />
          ))}
          {versions.hasNextPage && (
            <div className="px-4 py-2">
              <LoadMore list={versions} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// Cohort members reuse the sessions table's columns, minus the agent-scoped
// and call-volume ones that add little inside a single-agent cohort.
// Why: the sessions endpoint filters on membership but sorts by the standard
// session sort, so member order is not recoverable here.
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

export function CohortVersionDetailPage() {
  const { id } = useParams<{ id: string }>();
  const versionId = id ?? "";

  const version = useOne(["cohort-versions", versionId], () =>
    unwrap(
      client.GET("/v1/cohort-versions/{cohort_version_id}", {
        params: { path: { cohort_version_id: versionId } },
      }),
    ),
  );
  const members = useList(
    ["cohort-versions", versionId, "sessions"],
    (cursor) =>
      unwrap(
        client.GET("/v1/sessions", {
          params: { query: { cursor, size: 50, cohort_version_id: versionId } },
        }),
      ),
  );

  if (version.isPending) {
    return <Loading />;
  }
  if (version.isError) {
    return <ErrorNote error={version.error} />;
  }
  const data = version.data;

  return (
    <div>
      <PageHeader
        title={`v${data.version}${data.display_version ? ` · ${data.display_version}` : ""}`}
        subtitle={<IdText id={data.id} />}
      />
      <div className="mb-4">
        <KeyValue
          entries={[
            {
              label: "Cohort",
              value: (
                <IdLink id={data.cohort_id} to={`/cohorts/${data.cohort_id}`} />
              ),
            },
            { label: "Display version", value: data.display_version },
            { label: "Sessions", value: String(data.session_count) },
            { label: "Created", value: formatDate(data.created) },
          ]}
        />
      </div>
      <SectionHeading>Member sessions</SectionHeading>
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
