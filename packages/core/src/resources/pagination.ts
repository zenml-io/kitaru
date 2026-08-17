import type { QueryParameters } from "../query.js";
import type { ListParams, Page } from "../types.js";

export interface CursorParams {
  cursor?: string | null;
}

export interface ResourceRequestOptions {
  signal?: AbortSignal;
}

export function encodeListParams(params: ListParams = {}): QueryParameters {
  return {
    cursor: params.cursor,
    filter:
      params.filter === undefined || params.filter === null
        ? undefined
        : JSON.stringify(params.filter),
    size: params.size,
    sort: params.sort,
  };
}

export async function* iteratePages<T, P extends CursorParams>(
  params: P,
  loadPage: (params: P) => Promise<Page<T>>,
): AsyncGenerator<T, void, undefined> {
  let current = { ...params };
  const seenCursors = new Set<string>();
  if (current.cursor !== undefined && current.cursor !== null) {
    seenCursors.add(current.cursor);
  }
  while (true) {
    const page = await loadPage(current);
    for (const item of page.items) {
      yield item;
    }
    if (page.next_cursor === null) {
      return;
    }
    if (seenCursors.has(page.next_cursor)) {
      throw new Error("Pagination cursor did not advance");
    }
    seenCursors.add(page.next_cursor);
    current = { ...current, cursor: page.next_cursor };
  }
}
