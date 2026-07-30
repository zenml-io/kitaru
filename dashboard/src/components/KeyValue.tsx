import type { ReactNode } from "react";

export interface KeyValueEntry {
  label: string;
  value: ReactNode;
}

export function KeyValue({ entries }: { entries: KeyValueEntry[] }) {
  return (
    <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1.5 rounded-lg border border-zinc-200 bg-white px-4 py-3 text-sm">
      {entries.map((entry) => (
        <div key={entry.label} className="contents">
          <dt className="text-zinc-500">{entry.label}</dt>
          <dd className="min-w-0 break-words text-zinc-800">
            {entry.value ?? <span className="text-zinc-400">—</span>}
          </dd>
        </div>
      ))}
    </dl>
  );
}
