import { client, unwrap } from "../api/client";
import { useList } from "../api/hooks";
import type { Session, SessionOrigin, SessionStatus } from "../api/types";
import { StatusBadge } from "../components/Badge";
import {
  FilterBar,
  type FilterDef,
  useFilterValues,
} from "../components/FilterBar";
import { IdLink } from "../components/IdLink";
import { PageHeader } from "../components/PageHeader";
import type { Column } from "../components/Table";
import { DataTable } from "../components/Table";
import {
  formatCost,
  formatDate,
  formatDuration,
  formatTokens,
} from "../lib/format";

const FILTERS: FilterDef[] = [
  {
    key: "origin",
    label: "Origin",
    type: "select",
    options: [
      { value: "recorded", label: "recorded" },
      { value: "imported", label: "imported" },
      { value: "replay", label: "replay" },
    ],
  },
  {
    key: "status",
    label: "Status",
    type: "select",
    options: [
      { value: "in_progress", label: "in_progress" },
      { value: "completed", label: "completed" },
      { value: "failed", label: "failed" },
    ],
  },
  { key: "name", label: "Name", type: "text" },
  { key: "agent_id", label: "Agent ID", type: "text" },
  { key: "agent_version_id", label: "Agent version ID", type: "text" },
  { key: "provider", label: "Provider", type: "text" },
  { key: "tag", label: "Tag", type: "text" },
  {
    key: "has_evaluation",
    label: "Evaluated?",
    type: "select",
    options: [
      { value: "true", label: "has evaluations" },
      { value: "false", label: "no evaluations" },
    ],
  },
  { key: "min_cost", label: "Min cost", type: "text", placeholder: "Min cost" },
  { key: "max_cost", label: "Max cost", type: "text", placeholder: "Max cost" },
];

const FILTER_KEYS = FILTERS.map((filter) => filter.key);

const COLUMNS: Column<Session>[] = [
  {
    header: "ID",
    cell: (session) => (
      <IdLink id={session.id} to={`/sessions/${session.id}`} />
    ),
  },
  {
    header: "Name",
    cell: (session) => session.name ?? <span className="text-zinc-400">—</span>,
    className: "max-w-56 truncate",
  },
  {
    header: "Agent",
    cell: (session) => (
      <IdLink id={session.agent_id} to={`/agents/${session.agent_id}`} />
    ),
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
    header: "LLM / tool calls",
    cell: (session) => `${session.llm_call_count} / ${session.tool_call_count}`,
    className: "whitespace-nowrap",
  },
  {
    header: "Tokens",
    cell: (session) => formatTokens(session.tokens),
    className: "whitespace-nowrap",
  },
  {
    header: "Cost",
    cell: (session) => formatCost(session.cost),
    className: "whitespace-nowrap",
  },
];

export function SessionsPage() {
  const filters = useFilterValues(FILTER_KEYS);

  const list = useList(["sessions", filters], (cursor) =>
    unwrap(
      client.GET("/v1/sessions", {
        params: {
          query: {
            cursor,
            size: 50,
            agent_id: filters.agent_id,
            agent_version_id: filters.agent_version_id,
            origin: filters.origin as SessionOrigin | undefined,
            status: filters.status as SessionStatus | undefined,
            provider: filters.provider,
            name: filters.name,
            tag: filters.tag,
            has_evaluation:
              filters.has_evaluation === undefined
                ? undefined
                : filters.has_evaluation === "true",
            min_cost: filters.min_cost,
            max_cost: filters.max_cost,
          },
        },
      }),
    ),
  );

  return (
    <div>
      <PageHeader
        title="Sessions"
        subtitle="Every recorded, imported, and replayed agent execution."
      />
      <FilterBar filters={FILTERS} />
      <DataTable
        list={list}
        columns={COLUMNS}
        rowKey={(session) => session.id}
        rowLink={(session) => `/sessions/${session.id}`}
        emptyMessage="No sessions match the current filters."
      />
    </div>
  );
}
