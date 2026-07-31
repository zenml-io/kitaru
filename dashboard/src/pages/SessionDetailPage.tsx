import { useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, useOne } from "../api/hooks";
import type { Evaluation } from "../api/types";
import { Badge, StatusBadge } from "../components/Badge";
import { IdLink, IdText } from "../components/IdLink";
import { JsonViewer } from "../components/JsonViewer";
import { KeyValue } from "../components/KeyValue";
import { PageHeader } from "../components/PageHeader";
import { ErrorBanner, ErrorNote, Loading } from "../components/States";
import type { Column } from "../components/Table";
import { DataTable } from "../components/Table";
import { Tabs } from "../components/Tabs";
import {
  formatCost,
  formatDate,
  formatDuration,
  formatScore,
  formatTokens,
  shortId,
} from "../lib/format";
import { REPLAY_COLUMNS } from "./ReplaysPage";
import { TraceTree } from "./TraceTree";

const EVALUATION_COLUMNS: Column<Evaluation>[] = [
  { header: "Name", cell: (evaluation) => evaluation.name },
  {
    header: "Evaluator",
    cell: (evaluation) =>
      evaluation.evaluator_name
        ? `${evaluation.evaluator_name} v${evaluation.evaluator_version ?? "?"}`
        : "—",
  },
  { header: "Type", cell: (evaluation) => evaluation.data_type },
  {
    header: "Score",
    cell: (evaluation) => formatScore(evaluation.score),
  },
  { header: "Value", cell: (evaluation) => evaluation.value ?? "—" },
  {
    header: "Passed",
    cell: (evaluation) =>
      evaluation.passed == null ? (
        "—"
      ) : (
        <Badge tone={evaluation.passed ? "green" : "red"}>
          {evaluation.passed ? "Passed" : "Failed"}
        </Badge>
      ),
  },
  {
    header: "Explanation",
    cell: (evaluation) => evaluation.explanation ?? "—",
    className: "max-w-96 text-zinc-500",
  },
];

function EvaluationsTab({ sessionId }: { sessionId: string }) {
  const list = useList(["evaluations", { session_id: sessionId }], (cursor) =>
    unwrap(
      client.GET("/v1/evaluations", {
        params: { query: { cursor, size: 50, session_id: sessionId } },
      }),
    ),
  );
  return (
    <DataTable
      list={list}
      columns={EVALUATION_COLUMNS}
      rowKey={(evaluation) => evaluation.id}
      emptyMessage="No evaluations recorded for this session."
    />
  );
}

function ReplaysTab({ sessionId }: { sessionId: string }) {
  const list = useList(
    ["replays", { baseline_session_id: sessionId }],
    (cursor) =>
      unwrap(
        client.GET("/v1/replays", {
          params: {
            query: { cursor, size: 50, baseline_session_id: sessionId },
          },
        }),
      ),
  );
  return (
    <DataTable
      list={list}
      columns={REPLAY_COLUMNS}
      rowKey={(replay) => replay.id}
      rowLink={(replay) => `/replays/${replay.id}`}
      emptyMessage="This session has not been replayed."
    />
  );
}

export function SessionDetailPage() {
  const { id } = useParams<{ id: string }>();
  const sessionId = id ?? "";

  const session = useOne(["sessions", sessionId], () =>
    unwrap(
      client.GET("/v1/sessions/{session_id}", {
        params: { path: { session_id: sessionId } },
      }),
    ),
  );

  if (session.isPending) {
    return <Loading />;
  }
  if (session.isError) {
    return <ErrorNote error={session.error} />;
  }
  const data = session.data;

  return (
    <div>
      <PageHeader
        title={data.name ?? `Session ${shortId(data.id)}`}
        subtitle={
          <span className="flex items-center gap-2">
            <StatusBadge status={data.origin} />
            <StatusBadge status={data.status} />
            <IdText id={data.id} />
          </span>
        }
      />
      <div className="mb-4">
        <KeyValue
          entries={[
            {
              label: "Agent",
              value: (
                <IdLink id={data.agent_id} to={`/agents/${data.agent_id}`} />
              ),
            },
            {
              label: "Agent version",
              value: data.agent_version_id ? (
                <IdText id={data.agent_version_id} />
              ) : null,
            },
            {
              label: "Task",
              value: data.task_id ? <IdText id={data.task_id} /> : null,
            },
            { label: "Provider", value: data.provider },
            { label: "Framework", value: data.framework },
            { label: "External ID", value: data.external_id },
            { label: "Started", value: formatDate(data.started_at) },
            { label: "Ended", value: formatDate(data.ended_at) },
            {
              label: "Duration",
              value: formatDuration(data.started_at, data.ended_at),
            },
            {
              label: "Calls",
              value: `${data.llm_call_count} LLM / ${data.tool_call_count} tool`,
            },
            { label: "Tokens", value: formatTokens(data.tokens) },
            { label: "Cost", value: formatCost(data.cost) },
          ]}
        />
      </div>
      {data.error && <ErrorBanner message={data.error} />}
      <div className="mb-4 grid gap-3 lg:grid-cols-2">
        <JsonViewer value={data.inputs} label="Inputs" defaultOpenDepth={1} />
        <JsonViewer value={data.outputs} label="Outputs" defaultOpenDepth={1} />
        <JsonViewer
          value={data.expected}
          label="Expected"
          defaultOpenDepth={1}
        />
        <JsonViewer
          value={data.metadata}
          label="Metadata"
          defaultOpenDepth={1}
        />
      </div>
      <Tabs
        tabs={[
          {
            id: "trace",
            label: "Trace",
            content: <TraceTree sessionId={sessionId} />,
          },
          {
            id: "evaluations",
            label: "Evaluations",
            content: <EvaluationsTab sessionId={sessionId} />,
          },
          {
            id: "replays",
            label: "Replays",
            content: <ReplaysTab sessionId={sessionId} />,
          },
        ]}
      />
    </div>
  );
}
