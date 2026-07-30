import { useEffect, useState } from "react";
import { useSearchParams } from "react-router";

export interface FilterOption {
  value: string;
  label: string;
}

export interface FilterDef {
  key: string;
  label: string;
  type: "text" | "select";
  options?: FilterOption[];
  placeholder?: string;
}

/**
 * Read the current values of the given filter keys from the URL. Pages pass
 * these straight into the list query key, which is what resets cursor
 * pagination whenever a filter changes.
 */
export function useFilterValues(
  keys: readonly string[],
): Record<string, string> {
  const [searchParams] = useSearchParams();
  const values: Record<string, string> = {};
  for (const key of keys) {
    const value = searchParams.get(key);
    if (value !== null && value !== "") {
      values[key] = value;
    }
  }
  return values;
}

function TextFilter({ def }: { def: FilterDef }) {
  const [searchParams, setSearchParams] = useSearchParams();
  const urlValue = searchParams.get(def.key) ?? "";
  const [draft, setDraft] = useState(urlValue);

  // Sync the draft when the URL changes from elsewhere (back button,
  // cleared filters); keystrokes below debounce back into the URL.
  useEffect(() => {
    setDraft(urlValue);
  }, [urlValue]);

  useEffect(() => {
    if (draft === urlValue) {
      return;
    }
    const timer = setTimeout(() => {
      setSearchParams(
        (params) => {
          if (draft === "") {
            params.delete(def.key);
          } else {
            params.set(def.key, draft);
          }
          return params;
        },
        { replace: true },
      );
    }, 300);
    return () => clearTimeout(timer);
  }, [draft, urlValue, def.key, setSearchParams]);

  return (
    <input
      type="text"
      value={draft}
      onChange={(event) => setDraft(event.target.value)}
      placeholder={def.placeholder ?? def.label}
      className="w-36 rounded-md border border-zinc-300 bg-white px-2 py-1 text-sm placeholder:text-zinc-400 focus:border-indigo-400 focus:outline-none"
    />
  );
}

function SelectFilter({ def }: { def: FilterDef }) {
  const [searchParams, setSearchParams] = useSearchParams();
  const value = searchParams.get(def.key) ?? "";

  return (
    <select
      value={value}
      onChange={(event) => {
        setSearchParams(
          (params) => {
            if (event.target.value === "") {
              params.delete(def.key);
            } else {
              params.set(def.key, event.target.value);
            }
            return params;
          },
          { replace: true },
        );
      }}
      className={`rounded-md border border-zinc-300 bg-white px-2 py-1 text-sm focus:border-indigo-400 focus:outline-none ${
        value === "" ? "text-zinc-400" : "text-zinc-800"
      }`}
    >
      <option value="">{def.label}</option>
      {def.options?.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </select>
  );
}

export function FilterBar({ filters }: { filters: FilterDef[] }) {
  const [searchParams, setSearchParams] = useSearchParams();
  const hasActiveFilter = filters.some((def) => searchParams.has(def.key));

  return (
    <div className="mb-4 flex flex-wrap items-center gap-2">
      {filters.map((def) =>
        def.type === "select" ? (
          <SelectFilter key={def.key} def={def} />
        ) : (
          <TextFilter key={def.key} def={def} />
        ),
      )}
      {hasActiveFilter && (
        <button
          type="button"
          onClick={() => {
            setSearchParams(
              (params) => {
                for (const def of filters) {
                  params.delete(def.key);
                }
                return params;
              },
              { replace: true },
            );
          }}
          className="rounded-md px-2 py-1 text-sm text-zinc-500 hover:bg-zinc-100 hover:text-zinc-800"
        >
          Clear filters
        </button>
      )}
    </div>
  );
}
