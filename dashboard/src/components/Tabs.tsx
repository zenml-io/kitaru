import type { ReactNode } from "react";
import { useSearchParams } from "react-router";

export interface TabDef {
  id: string;
  label: string;
  content: ReactNode;
}

/** Search-param-backed tabs so the active tab survives reload and is linkable. */
export function Tabs({ tabs }: { tabs: TabDef[] }) {
  const [searchParams, setSearchParams] = useSearchParams();
  const firstTab = tabs[0];
  if (!firstTab) {
    return null;
  }
  const requested = searchParams.get("tab");
  const active = tabs.find((tab) => tab.id === requested) ?? firstTab;

  return (
    <div>
      <div className="flex gap-1 border-zinc-200 border-b">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            onClick={() => {
              setSearchParams(
                (params) => {
                  params.set("tab", tab.id);
                  return params;
                },
                { replace: true },
              );
            }}
            className={`-mb-px border-b-2 px-3 py-2 text-sm ${
              tab.id === active.id
                ? "border-indigo-600 font-medium text-indigo-700"
                : "border-transparent text-zinc-500 hover:text-zinc-800"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>
      <div className="pt-4">{active.content}</div>
    </div>
  );
}
