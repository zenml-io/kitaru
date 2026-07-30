import { useQueryClient } from "@tanstack/react-query";
import { Settings } from "lucide-react";
import { useId, useState } from "react";
import { setApiKey, useApiKey } from "../lib/settings";

export function SettingsPanel() {
  const [open, setOpen] = useState(false);
  const apiKey = useApiKey();
  const [draft, setDraft] = useState("");
  const queryClient = useQueryClient();
  const inputId = useId();

  const applyKey = (value: string | null) => {
    setApiKey(value);
    setDraft("");
    setOpen(false);
    queryClient.invalidateQueries();
  };

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => {
          setDraft(apiKey ?? "");
          setOpen((current) => !current);
        }}
        className="flex items-center rounded-md p-1.5 text-zinc-500 hover:bg-zinc-100 hover:text-zinc-800"
        title="Settings"
      >
        <Settings size={16} />
      </button>
      {open && (
        <div className="absolute right-0 z-20 mt-2 w-80 rounded-lg border border-zinc-200 bg-white p-4 shadow-lg">
          <label
            htmlFor={inputId}
            className="block font-medium text-sm text-zinc-800"
          >
            API key
          </label>
          <p className="mt-1 text-xs text-zinc-500">
            Only needed when the server runs with{" "}
            <code className="font-mono">auth_scheme=local</code>. Sent as a
            bearer token and stored in this browser only.
          </p>
          <input
            id={inputId}
            type="password"
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            placeholder="KITKEY_…"
            className="mt-2 w-full rounded-md border border-zinc-300 px-2.5 py-1.5 font-mono text-sm focus:border-indigo-400 focus:outline-none"
          />
          <div className="mt-3 flex justify-end gap-2">
            {apiKey && (
              <button
                type="button"
                onClick={() => applyKey(null)}
                className="rounded-md px-2.5 py-1.5 text-sm text-zinc-600 hover:bg-zinc-100"
              >
                Clear
              </button>
            )}
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="rounded-md px-2.5 py-1.5 text-sm text-zinc-600 hover:bg-zinc-100"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={() => applyKey(draft)}
              className="rounded-md bg-indigo-600 px-2.5 py-1.5 text-sm text-white hover:bg-indigo-700"
            >
              Save
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
