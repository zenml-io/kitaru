import {
  type UseQueryOptions,
  useInfiniteQuery,
  useQuery,
} from "@tanstack/react-query";
import type { ApiError } from "./client";
import type { Page } from "./types";

/**
 * Cursor-paginated list. The query key must include every filter value: the
 * server rejects cursor reuse across changed filters (the cursor embeds a
 * filter hash), so a filter change has to start a fresh pagination — which
 * a key change does automatically.
 */
export function useList<T>(
  key: readonly unknown[],
  fetchPage: (cursor: string | undefined) => Promise<Page<T>>,
  options?: { refetchInterval?: number },
) {
  const query = useInfiniteQuery<
    Page<T>,
    ApiError,
    T[],
    readonly unknown[],
    string | undefined
  >({
    queryKey: key,
    queryFn: ({ pageParam }) => fetchPage(pageParam),
    initialPageParam: undefined,
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    select: (data) => data.pages.flatMap((page) => page.items),
    refetchInterval: options?.refetchInterval,
  });

  return {
    items: query.data ?? [],
    isLoading: query.isPending,
    error: query.error,
    hasNextPage: query.hasNextPage,
    isFetchingNextPage: query.isFetchingNextPage,
    fetchNextPage: query.fetchNextPage,
  };
}

export function useOne<T>(
  key: readonly unknown[],
  fetchOne: () => Promise<T>,
  options?: Partial<UseQueryOptions<T, ApiError>>,
) {
  return useQuery<T, ApiError>({
    queryKey: key,
    queryFn: fetchOne,
    ...options,
  });
}

/**
 * Detail fetch that polls until the resource reaches a terminal state
 * (there is no push channel on the server — polling is the sanctioned
 * way to render live progress).
 */
export function usePolledOne<T>(
  key: readonly unknown[],
  fetchOne: () => Promise<T>,
  isTerminal: (value: T) => boolean,
  intervalMs = 3_000,
) {
  return useQuery<T, ApiError>({
    queryKey: key,
    queryFn: fetchOne,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data !== undefined && isTerminal(data)) {
        return false;
      }
      return intervalMs;
    },
  });
}
