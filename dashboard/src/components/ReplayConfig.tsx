import { JsonViewer } from "./JsonViewer";

/**
 * The replay configuration triple as it appears on both experiments and
 * replays (which carry a flattened copy of the same config).
 */
export function ReplayConfig({
  toolPolicy,
  override,
  evaluators,
}: {
  toolPolicy: unknown;
  override: unknown;
  evaluators: unknown;
}) {
  return (
    <div className="grid gap-3 lg:grid-cols-2">
      <JsonViewer value={toolPolicy} label="Tool policy" defaultOpenDepth={2} />
      <JsonViewer value={override} label="Override" defaultOpenDepth={2} />
      <JsonViewer value={evaluators} label="Evaluators" defaultOpenDepth={2} />
    </div>
  );
}
