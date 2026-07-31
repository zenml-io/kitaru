import { useParams } from "react-router";
import { client, unwrap } from "../api/client";
import { useList, useOne } from "../api/hooks";
import type { Evaluation, Evaluator, EvaluatorVersion } from "../api/types";
import { Badge } from "../components/Badge";
import {
  FilterBar,
  type FilterDef,
  useFilterValues,
} from "../components/FilterBar";
import { IdLink, IdText } from "../components/IdLink";
import { JsonViewer } from "../components/JsonViewer";
import { KeyValue } from "../components/KeyValue";
import { PageHeader, SectionHeading } from "../components/PageHeader";
import { EmptyState, ErrorNote, Loading } from "../components/States";
import type { Column } from "../components/Table";
import { DataTable, LoadMore } from "../components/Table";
import { formatDate, formatScore } from "../lib/format";

const FILTERS: FilterDef[] = [{ key: "name", label: "Name", type: "text" }];

const EVALUATOR_COLUMNS: Column<Evaluator>[] = [
  {
    header: "ID",
    cell: (evaluator) => (
      <IdLink id={evaluator.id} to={`/evaluators/${evaluator.id}`} />
    ),
  },
  { header: "Name", cell: (evaluator) => evaluator.name },
  {
    header: "Description",
    cell: (evaluator) => evaluator.description ?? "—",
    className: "max-w-96 truncate text-zinc-500",
  },
  {
    header: "Latest version",
    cell: (evaluator) => `v${evaluator.latest_version}`,
  },
  {
    header: "Created",
    cell: (evaluator) => formatDate(evaluator.created),
    className: "whitespace-nowrap text-zinc-500",
  },
];

export function EvaluatorsPage() {
  const filters = useFilterValues(FILTERS);
  const list = useList(["evaluators", filters], (cursor) =>
    unwrap(
      client.GET("/v1/evaluators", {
        params: { query: { cursor, size: 50, name: filters.name } },
      }),
    ),
  );

  return (
    <div>
      <PageHeader
        title="Evaluators"
        subtitle="Workspace-level scoring functions, referenced by experiments and evaluation rows."
      />
      <FilterBar filters={FILTERS} />
      <DataTable
        list={list}
        columns={EVALUATOR_COLUMNS}
        rowKey={(evaluator) => evaluator.id}
        rowLink={(evaluator) => `/evaluators/${evaluator.id}`}
        emptyMessage="No evaluators registered."
      />
    </div>
  );
}

const EVALUATION_COLUMNS: Column<Evaluation>[] = [
  {
    header: "Session",
    cell: (evaluation) => (
      <IdLink
        id={evaluation.session_id}
        to={`/sessions/${evaluation.session_id}`}
      />
    ),
  },
  { header: "Name", cell: (evaluation) => evaluation.name },
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
    header: "Created",
    cell: (evaluation) => formatDate(evaluation.created),
    className: "whitespace-nowrap text-zinc-500",
  },
];

function VersionEvaluations({ versionId }: { versionId: string }) {
  const list = useList(
    ["evaluations", { evaluator_version_id: versionId }],
    (cursor) =>
      unwrap(
        client.GET("/v1/evaluations", {
          params: {
            query: { cursor, size: 50, evaluator_version_id: versionId },
          },
        }),
      ),
  );
  return (
    <DataTable
      list={list}
      columns={EVALUATION_COLUMNS}
      rowKey={(evaluation) => evaluation.id}
      emptyMessage="No evaluations produced by this version."
    />
  );
}

function VersionSection({ version }: { version: EvaluatorVersion }) {
  return (
    <div className="mb-4">
      <div className="mb-2 flex items-center gap-3">
        <span className="font-medium text-sm text-zinc-800">
          v{version.version}
          {version.display_version ? ` · ${version.display_version}` : ""}
        </span>
        <IdText id={version.id} />
        <span className="text-xs text-zinc-400">
          {formatDate(version.created)}
        </span>
      </div>
      <div className="mb-2">
        <JsonViewer
          value={version.source}
          label="Source"
          defaultOpenDepth={1}
        />
      </div>
      <VersionEvaluations versionId={version.id} />
    </div>
  );
}

export function EvaluatorDetailPage() {
  const { id } = useParams<{ id: string }>();
  const evaluatorId = id ?? "";

  const evaluator = useOne(["evaluators", evaluatorId], () =>
    unwrap(
      client.GET("/v1/evaluators/{evaluator_id}", {
        params: { path: { evaluator_id: evaluatorId } },
      }),
    ),
  );
  const versions = useList(["evaluators", evaluatorId, "versions"], (cursor) =>
    unwrap(
      client.GET("/v1/evaluators/{evaluator_id}/versions", {
        params: {
          path: { evaluator_id: evaluatorId },
          query: { cursor, size: 50 },
        },
      }),
    ),
  );

  if (evaluator.isPending) {
    return <Loading />;
  }
  if (evaluator.isError) {
    return <ErrorNote error={evaluator.error} />;
  }
  const data = evaluator.data;

  return (
    <div>
      <PageHeader title={data.name} subtitle={<IdText id={data.id} />} />
      <div className="mb-4">
        <KeyValue
          entries={[
            { label: "Description", value: data.description },
            { label: "Latest version", value: `v${data.latest_version}` },
            { label: "Created", value: formatDate(data.created) },
          ]}
        />
      </div>
      <div className="mb-4">
        <JsonViewer
          value={data.metadata}
          label="Metadata"
          defaultOpenDepth={1}
        />
      </div>
      <SectionHeading>Versions and their evaluations</SectionHeading>
      {versions.isLoading ? (
        <Loading />
      ) : versions.error ? (
        <ErrorNote error={versions.error} />
      ) : (
        <>
          {versions.items.length === 0 && (
            <EmptyState message="This evaluator has no versions." />
          )}
          {versions.items.map((version) => (
            <VersionSection key={version.id} version={version} />
          ))}
          <LoadMore list={versions} label="Load more versions" />
        </>
      )}
    </div>
  );
}
