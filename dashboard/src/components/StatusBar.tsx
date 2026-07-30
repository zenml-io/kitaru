import { useQuery } from "@tanstack/react-query";
import { CircleAlert, CircleCheck } from "lucide-react";

interface ServerInfo {
  id?: string | null;
  version: string;
  auth_scheme: string;
}

async function fetchServerInfo(): Promise<ServerInfo> {
  const response = await fetch("/v1/info");
  if (!response.ok) {
    throw new Error(`Server responded ${response.status}`);
  }
  return (await response.json()) as ServerInfo;
}

export function StatusBar() {
  const info = useQuery({
    queryKey: ["server-info"],
    queryFn: fetchServerInfo,
    refetchInterval: 30_000,
  });

  return (
    <header className="flex h-12 items-center justify-between border-zinc-200 border-b bg-white px-6">
      <div className="text-sm text-zinc-500">
        Read-only view of a Kitaru server
      </div>
      <div className="flex items-center gap-3 text-sm">
        {info.isPending ? (
          <span className="text-zinc-400">Connecting…</span>
        ) : info.isError ? (
          <span className="flex items-center gap-1.5 text-red-600">
            <CircleAlert size={15} />
            Server unreachable
          </span>
        ) : (
          <>
            <span className="flex items-center gap-1.5 text-emerald-700">
              <CircleCheck size={15} />
              kitaru {info.data.version}
            </span>
            <span className="rounded-full bg-zinc-100 px-2 py-0.5 text-xs text-zinc-600">
              auth: {info.data.auth_scheme}
            </span>
          </>
        )}
      </div>
    </header>
  );
}
