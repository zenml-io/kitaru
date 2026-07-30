import { Link } from "react-router";
import { shortId } from "../lib/format";

/** Monospace, truncated UUID that links to the entity's detail page. */
export function IdLink({ id, to }: { id: string; to: string }) {
  return (
    <Link
      to={to}
      title={id}
      className="font-mono text-indigo-600 text-xs hover:text-indigo-800 hover:underline"
    >
      {shortId(id)}
    </Link>
  );
}

export function IdText({ id }: { id: string }) {
  return (
    <span title={id} className="font-mono text-xs text-zinc-500">
      {shortId(id)}
    </span>
  );
}
