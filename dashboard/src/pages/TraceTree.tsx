import {
  Bot,
  Braces,
  ChevronDown,
  ChevronRight,
  Sparkles,
  Wrench,
} from "lucide-react";
import { type ReactNode, useMemo, useState } from "react";
import { client, unwrap } from "../api/client";
import { useList } from "../api/hooks";
import type { NodeType, SessionNode } from "../api/types";
import { StatusBadge } from "../components/Badge";
import { JsonViewer } from "../components/JsonViewer";
import { EmptyState, ErrorNote, Loading } from "../components/States";
import { formatCost, formatDuration, formatTokens } from "../lib/format";

const NODE_ICONS: Record<NodeType, ReactNode> = {
  llm_call: <Sparkles size={14} className="text-violet-500" />,
  tool_call: <Wrench size={14} className="text-sky-600" />,
  subagent_call: <Bot size={14} className="text-amber-600" />,
  span: <Braces size={14} className="text-zinc-400" />,
};

interface TreeRow {
  node: SessionNode;
  depth: number;
  hasChildren: boolean;
}

/**
 * Nodes arrive as a flat index-ascending list where parents always precede
 * children, so depth and child flags fall out of a single pass.
 */
function buildRows(nodes: SessionNode[]): TreeRow[] {
  const depthById = new Map<string, number>();
  const parentsWithChildren = new Set<string>();
  const rows: TreeRow[] = [];

  for (const node of nodes) {
    const depth = node.parent_id ? (depthById.get(node.parent_id) ?? 0) + 1 : 0;
    depthById.set(node.id, depth);
    if (node.parent_id) {
      parentsWithChildren.add(node.parent_id);
    }
    rows.push({ node, depth, hasChildren: false });
  }
  for (const row of rows) {
    row.hasChildren = parentsWithChildren.has(row.node.id);
  }
  return rows;
}

function NodeSummary({ node }: { node: SessionNode }) {
  if (node.node_type === "llm_call") {
    const parts = [
      node.model ?? node.requested_model,
      formatTokens(node.tokens) !== "—" ? formatTokens(node.tokens) : null,
      node.cost != null ? formatCost(node.cost) : null,
    ].filter((part): part is string => part != null);
    return <span className="text-xs text-zinc-400">{parts.join(" · ")}</span>;
  }
  if (node.node_type === "tool_call" && node.cache_key) {
    return (
      <span className="font-mono text-xs text-zinc-300" title={node.cache_key}>
        cached
      </span>
    );
  }
  return null;
}

function NodeDetail({ node }: { node: SessionNode }) {
  return (
    <div className="mt-1 mb-2 ml-6 space-y-2">
      {node.error && (
        <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-red-700 text-xs">
          {node.error}
        </div>
      )}
      {node.inputs !== null && node.inputs !== undefined && (
        <JsonViewer value={node.inputs} label="Inputs" defaultOpenDepth={1} />
      )}
      {node.outputs !== null && node.outputs !== undefined && (
        <JsonViewer value={node.outputs} label="Outputs" defaultOpenDepth={1} />
      )}
      {node.attributes != null && Object.keys(node.attributes).length > 0 && (
        <JsonViewer
          value={node.attributes}
          label="Attributes"
          defaultOpenDepth={1}
        />
      )}
      {node.metadata != null && Object.keys(node.metadata).length > 0 && (
        <JsonViewer
          value={node.metadata}
          label="Metadata"
          defaultOpenDepth={1}
        />
      )}
      {node.cache_key && (
        <div className="font-mono text-xs text-zinc-400">
          cache_key: {node.cache_key}
        </div>
      )}
    </div>
  );
}

export function TraceTree({ sessionId }: { sessionId: string }) {
  const list = useList([`sessions/${sessionId}/nodes`], (cursor) =>
    unwrap(
      client.GET("/v1/sessions/{session_id}/nodes", {
        params: {
          path: { session_id: sessionId },
          query: { cursor, size: 1000, include_payloads: true },
        },
      }),
    ),
  );

  const rows = useMemo(() => buildRows(list.items), [list.items]);
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set());
  const [selectedId, setSelectedId] = useState<string | null>(null);

  if (list.isLoading) {
    return <Loading />;
  }
  if (list.error) {
    return <ErrorNote error={list.error} />;
  }
  if (rows.length === 0) {
    return <EmptyState message="This session has no recorded nodes." />;
  }

  const toggleCollapsed = (nodeId: string) => {
    setCollapsed((current) => {
      const next = new Set(current);
      if (next.has(nodeId)) {
        next.delete(nodeId);
      } else {
        next.add(nodeId);
      }
      return next;
    });
  };

  // Skip rows hidden under a collapsed ancestor: once we collapse at depth d,
  // everything deeper is hidden until a row at depth <= d appears again.
  const visibleRows: TreeRow[] = [];
  let skipDeeperThan: number | null = null;
  for (const row of rows) {
    if (skipDeeperThan !== null && row.depth > skipDeeperThan) {
      continue;
    }
    skipDeeperThan = null;
    visibleRows.push(row);
    if (row.hasChildren && collapsed.has(row.node.id)) {
      skipDeeperThan = row.depth;
    }
  }

  return (
    <div>
      <div className="mb-2 flex items-center gap-3 text-xs text-zinc-500">
        <span>{rows.length} nodes</span>
        <button
          type="button"
          onClick={() =>
            setCollapsed(
              new Set(
                rows.filter((row) => row.hasChildren).map((row) => row.node.id),
              ),
            )
          }
          className="text-indigo-600 hover:underline"
        >
          Collapse all
        </button>
        <button
          type="button"
          onClick={() => setCollapsed(new Set())}
          className="text-indigo-600 hover:underline"
        >
          Expand all
        </button>
      </div>
      <div className="rounded-lg border border-zinc-200 bg-white py-1">
        {visibleRows.map((row) => (
          <div key={row.node.id}>
            <div
              className={`flex items-center gap-2 px-3 py-1 text-sm hover:bg-zinc-50 ${
                selectedId === row.node.id ? "bg-indigo-50/60" : ""
              }`}
              style={{ paddingLeft: `${12 + row.depth * 16}px` }}
            >
              {row.hasChildren ? (
                <button
                  type="button"
                  onClick={() => toggleCollapsed(row.node.id)}
                  className="text-zinc-400 hover:text-zinc-700"
                >
                  {collapsed.has(row.node.id) ? (
                    <ChevronRight size={13} />
                  ) : (
                    <ChevronDown size={13} />
                  )}
                </button>
              ) : (
                <span className="w-[13px]" />
              )}
              {NODE_ICONS[row.node.node_type]}
              <button
                type="button"
                onClick={() =>
                  setSelectedId(selectedId === row.node.id ? null : row.node.id)
                }
                className="min-w-0 truncate text-left font-medium text-zinc-800 hover:text-indigo-700"
                title={row.node.name}
              >
                {row.node.tool_name ?? row.node.name}
              </button>
              <StatusBadge status={row.node.status} />
              <span className="whitespace-nowrap text-xs text-zinc-400">
                {formatDuration(row.node.started_at, row.node.ended_at)}
              </span>
              <NodeSummary node={row.node} />
            </div>
            {selectedId === row.node.id && <NodeDetail node={row.node} />}
          </div>
        ))}
      </div>
      {list.hasNextPage && (
        <button
          type="button"
          onClick={() => list.fetchNextPage()}
          disabled={list.isFetchingNextPage}
          className="mt-2 rounded-md px-2 py-1 text-indigo-600 text-sm hover:bg-indigo-50 disabled:opacity-50"
        >
          Load more nodes
        </button>
      )}
    </div>
  );
}
