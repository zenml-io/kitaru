import { Check, Copy } from "lucide-react";
import { useState } from "react";

const MAX_INLINE_STRING = 200;

function Primitive({ value }: { value: unknown }) {
  const [expanded, setExpanded] = useState(false);

  if (value === null) {
    return <span className="text-zinc-400">null</span>;
  }
  if (typeof value === "boolean") {
    return <span className="text-violet-600">{String(value)}</span>;
  }
  if (typeof value === "number") {
    return <span className="text-sky-700">{String(value)}</span>;
  }
  if (typeof value === "string") {
    if (value.length > MAX_INLINE_STRING && !expanded) {
      return (
        <span className="text-emerald-700">
          "{value.slice(0, MAX_INLINE_STRING)}…"{" "}
          <button
            type="button"
            onClick={() => setExpanded(true)}
            className="text-indigo-500 text-xs hover:underline"
          >
            +{value.length - MAX_INLINE_STRING} chars
          </button>
        </span>
      );
    }
    return (
      <span className="whitespace-pre-wrap text-emerald-700">"{value}"</span>
    );
  }
  return <span>{String(value)}</span>;
}

function JsonNode({
  value,
  depth,
  defaultOpenDepth,
}: {
  value: unknown;
  depth: number;
  defaultOpenDepth: number;
}) {
  const isObject = typeof value === "object" && value !== null;
  const isArray = Array.isArray(value);
  let entries: [string, unknown][] = [];
  if (isArray) {
    entries = value.map((item, index) => [String(index), item]);
  } else if (isObject) {
    entries = Object.entries(value);
  }
  const startOpen = depth < defaultOpenDepth || entries.length <= 3;
  const [open, setOpen] = useState(startOpen);

  if (!isObject) {
    return <Primitive value={value} />;
  }

  const brackets = isArray ? "[]" : "{}";
  if (entries.length === 0) {
    return <span className="text-zinc-500">{brackets}</span>;
  }

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="text-zinc-500 hover:text-indigo-600"
      >
        {brackets[0]}… {entries.length} {isArray ? "items" : "keys"} …
        {brackets[1]}
      </button>
    );
  }

  return (
    <span>
      <button
        type="button"
        onClick={() => setOpen(false)}
        className="text-zinc-500 hover:text-indigo-600"
      >
        {brackets[0]}
      </button>
      <div className="ml-4 border-zinc-100 border-l pl-2">
        {entries.map(([key, entryValue]) => (
          <div key={key}>
            <span className="text-zinc-500">{isArray ? key : `"${key}"`}:</span>{" "}
            <JsonNode
              value={entryValue}
              depth={depth + 1}
              defaultOpenDepth={defaultOpenDepth}
            />
          </div>
        ))}
      </div>
      <span className="text-zinc-500">{brackets[1]}</span>
    </span>
  );
}

export function JsonViewer({
  value,
  label,
  defaultOpenDepth = 2,
}: {
  value: unknown;
  label?: string;
  defaultOpenDepth?: number;
}) {
  const [copied, setCopied] = useState(false);

  // Owning the "is this worth showing" rule here keeps call sites guard-free.
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value === "object" && Object.keys(value).length === 0) {
    return null;
  }

  return (
    <div className="rounded-lg border border-zinc-200 bg-white">
      <div className="flex items-center justify-between border-zinc-100 border-b px-3 py-1.5">
        <span className="font-medium text-xs text-zinc-500">
          {label ?? "JSON"}
        </span>
        <button
          type="button"
          title="Copy JSON"
          onClick={() => {
            navigator.clipboard.writeText(JSON.stringify(value, null, 2));
            setCopied(true);
            setTimeout(() => setCopied(false), 1_500);
          }}
          className="text-zinc-400 hover:text-zinc-700"
        >
          {copied ? <Check size={13} /> : <Copy size={13} />}
        </button>
      </div>
      <div className="overflow-x-auto px-3 py-2 font-mono text-xs leading-relaxed">
        <JsonNode value={value} depth={0} defaultOpenDepth={defaultOpenDepth} />
      </div>
    </div>
  );
}
