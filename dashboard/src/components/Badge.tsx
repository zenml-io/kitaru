import type { ReactNode } from "react";

export type BadgeTone = "green" | "red" | "amber" | "gray" | "blue" | "purple";

const TONE_CLASSES: Record<BadgeTone, string> = {
  green: "bg-emerald-50 text-emerald-700 ring-emerald-600/20",
  red: "bg-red-50 text-red-700 ring-red-600/20",
  amber: "bg-amber-50 text-amber-700 ring-amber-600/20",
  gray: "bg-zinc-100 text-zinc-600 ring-zinc-500/20",
  blue: "bg-sky-50 text-sky-700 ring-sky-600/20",
  purple: "bg-violet-50 text-violet-700 ring-violet-600/20",
};

export function Badge({
  tone = "gray",
  children,
}: {
  tone?: BadgeTone;
  children: ReactNode;
}) {
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 font-medium text-xs ring-1 ring-inset ${TONE_CLASSES[tone]}`}
    >
      {children}
    </span>
  );
}

const STATUS_TONES: Record<string, BadgeTone> = {
  completed: "green",
  failed: "red",
  timed_out: "red",
  in_progress: "amber",
  running: "amber",
  pending: "amber",
  claimed: "amber",
  evaluating: "amber",
  canceling: "gray",
  canceled: "gray",
  abandoned: "gray",
  // Session origins get their own hues so they read as categories, not states.
  recorded: "blue",
  imported: "purple",
  replay: "amber",
};

export function StatusBadge({ status }: { status: string }) {
  return <Badge tone={STATUS_TONES[status] ?? "gray"}>{status}</Badge>;
}
