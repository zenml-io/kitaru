import { Link, useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, useOne } from "../api/hooks";
import type { Agent, AgentVersion } from "../api/types";
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

const FILTERS: FilterDef[] = [{ key: "name", label: "Name", type: "text" }];

const AGENT_COLUMNS: Column<Agent>[] = [
  {
    header: "ID",
    cell: (agent) => <IdLink id={agent.id} to={`/agents/${agent.id}`} />,
  },
  { header: "Name", cell: (agent) => agent.name },
  {
    header: "Description",
    cell: (agent) => agent.description ?? "—",
    className: "max-w-96 truncate text-zinc-500",
  },
  { header: "Latest version", cell: (agent) => `v${agent.latest_version}` },
  {
    header: "Created",
    cell: (agent) => formatDate(agent.created),
    className: "whitespace-nowrap text-zinc-500",
  },
];

export function AgentsPage() {
  const filters = useFilterValues(FILTERS);
  const list = useList(["agents", filters], (cursor) =>
    unwrap(
      client.GET("/v1/agents", {
        params: { query: { cursor, size: 50, name: filters.name } },
      }),
    ),
  );

  return (
    <div>
      <PageHeader
        title="Agents"
        subtitle="Registered agents and their versions."
      />
      <FilterBar filters={FILTERS} />
      <DataTable
        list={list}
        columns={AGENT_COLUMNS}
        rowKey={(agent) => agent.id}
        rowLink={(agent) => `/agents/${agent.id}`}
        emptyMessage="No agents registered."
      />
    </div>
  );
}

function VersionRow({ version }: { version: AgentVersion }) {
  return (
    <div className="border-zinc-100 border-b px-4 py-3 last:border-b-0">
      <div className="flex items-center gap-3">
        <span className="font-medium text-sm text-zinc-800">
          v{version.version}
          {version.display_version ? ` · ${version.display_version}` : ""}
        </span>
        <IdText id={version.id} />
        <span className="text-xs text-zinc-400">
          {formatDate(version.created)}
        </span>
        <Link
          to={`/sessions?agent_version_id=${version.id}`}
          className="text-indigo-600 text-xs hover:underline"
        >
          Sessions →
        </Link>
      </div>
      {version.description && (
        <div className="mt-1 text-sm text-zinc-500">{version.description}</div>
      )}
      <div className="mt-2 grid gap-3 lg:grid-cols-2">
        <JsonViewer
          value={version.run_spec}
          label="Run spec"
          defaultOpenDepth={1}
        />
        <JsonViewer
          value={version.capabilities}
          label="Capabilities"
          defaultOpenDepth={1}
        />
      </div>
    </div>
  );
}

export function AgentDetailPage() {
  const { id } = useParams<{ id: string }>();
  const agentId = id ?? "";

  const agent = useOne(["agents", agentId], () =>
    unwrap(
      client.GET("/v1/agents/{agent_id}", {
        params: { path: { agent_id: agentId } },
      }),
    ),
  );
  const versions = useList(["agents", agentId, "versions"], (cursor) =>
    unwrap(
      client.GET("/v1/agents/{agent_id}/versions", {
        params: { path: { agent_id: agentId }, query: { cursor, size: 50 } },
      }),
    ),
  );

  if (agent.isPending) {
    return <Loading />;
  }
  if (agent.isError) {
    return <ErrorNote error={agent.error} />;
  }
  const data = agent.data;

  return (
    <div>
      <PageHeader title={data.name} subtitle={<IdText id={data.id} />} />
      <div className="mb-4">
        <KeyValue
          entries={[
            { label: "Description", value: data.description },
            { label: "Latest version", value: `v${data.latest_version}` },
            { label: "Created", value: formatDate(data.created) },
            {
              label: "Sessions",
              value: (
                <Link
                  to={`/sessions?agent_id=${data.id}`}
                  className="text-indigo-600 hover:underline"
                >
                  View sessions for this agent →
                </Link>
              ),
            },
          ]}
        />
      </div>
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
