import { Loader2 } from "lucide-react";
import type { ReactNode } from "react";
import { useNavigate } from "react-router";
import type { useList } from "../api/hooks";
import { EmptyState, ErrorNote, Loading } from "./States";

export interface Column<T> {
  header: string;
  cell: (row: T) => ReactNode;
  className?: string;
}

interface DataTableProps<T> {
  list: ReturnType<typeof useList<T>>;
  columns: Column<T>[];
  rowKey: (row: T) => string;
  rowLink?: (row: T) => string;
  emptyMessage?: string;
}

/**
 * The one table component every list screen uses: renders loading, error,
 * and empty states from the useList result, and cursor pagination as a
 * "Load more" footer (the API has no total counts, so no numbered pages).
 */
export function DataTable<T>({
  list,
  columns,
  rowKey,
  rowLink,
  emptyMessage,
}: DataTableProps<T>) {
  const navigate = useNavigate();

  if (list.isLoading) {
    return <Loading />;
  }
  if (list.error) {
    return <ErrorNote error={list.error} />;
  }
  if (list.items.length === 0) {
    return <EmptyState message={emptyMessage} />;
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-zinc-200 border-b bg-zinc-50/60">
            {columns.map((column) => (
              <th
                key={column.header}
                className={`px-3 py-2 text-left font-medium text-xs text-zinc-500 ${column.className ?? ""}`}
              >
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {list.items.map((row) => (
            <tr
              key={rowKey(row)}
              className={`border-zinc-100 border-b last:border-b-0 ${
                rowLink ? "cursor-pointer hover:bg-indigo-50/40" : ""
              }`}
              onClick={rowLink ? () => navigate(rowLink(row)) : undefined}
            >
              {columns.map((column) => (
                <td
                  key={column.header}
                  className={`px-3 py-2 align-top ${column.className ?? ""}`}
                >
                  {column.cell(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {list.hasNextPage && (
        <div className="border-zinc-100 border-t px-3 py-2">
          <button
            type="button"
            onClick={() => list.fetchNextPage()}
            disabled={list.isFetchingNextPage}
            className="flex items-center gap-1.5 rounded-md px-2 py-1 text-indigo-600 text-sm hover:bg-indigo-50 disabled:opacity-50"
          >
            {list.isFetchingNextPage && (
              <Loader2 size={14} className="animate-spin" />
            )}
            Load more
          </button>
        </div>
      )}
    </div>
  );
}
