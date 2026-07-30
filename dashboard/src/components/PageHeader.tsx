import type { ReactNode } from "react";

export function PageHeader({
  title,
  subtitle,
  actions,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="mb-4 flex items-start justify-between gap-4">
      <div>
        <h1 className="font-semibold text-lg text-zinc-900">{title}</h1>
        {subtitle && (
          <div className="mt-0.5 text-sm text-zinc-500">{subtitle}</div>
        )}
      </div>
      {actions}
    </div>
  );
}
