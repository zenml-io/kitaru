import { CircleAlert, CircleCheck } from "lucide-react";
import { client, unwrap } from "../api/client";
import { useOne } from "../api/hooks";
import { SettingsPanel } from "./SettingsPanel";

export function StatusBar() {
  const info = useOne(["server-info"], () => unwrap(client.GET("/v1/info")), {
    refetchInterval: 60_000,
    retry: 1,
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
        <SettingsPanel />
      </div>
    </header>
  );
}
