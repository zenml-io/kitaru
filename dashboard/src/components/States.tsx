import { CircleAlert, Loader2 } from "lucide-react";
import type { ApiError } from "../api/client";

export function Loading() {
  return (
    <div className="flex items-center gap-2 py-8 text-sm text-zinc-400">
      <Loader2 size={16} className="animate-spin" />
      Loading…
    </div>
  );
}

export function ErrorNote({ error }: { error: ApiError | Error }) {
  const isAuthError = "status" in error && error.status === 401;
  const detail = "detail" in error ? error.detail : error.message;
  return (
    <div className="my-4 flex items-start gap-2 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-red-700 text-sm">
      <CircleAlert size={16} className="mt-0.5 shrink-0" />
      <div>
        <div>{detail}</div>
        {isAuthError && (
          <div className="mt-1 text-red-600 text-xs">
            The server requires authentication — set an API key via the gear
            icon in the top-right corner.
          </div>
        )}
      </div>
    </div>
  );
}

export function EmptyState({
  message = "No items found.",
}: {
  message?: string;
}) {
  return <div className="py-8 text-sm text-zinc-400">{message}</div>;
}
