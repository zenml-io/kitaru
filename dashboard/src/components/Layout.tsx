import {
  Activity,
  Bot,
  FlaskConical,
  Gauge,
  Layers,
  ListChecks,
  MessagesSquare,
  RotateCcw,
} from "lucide-react";
import type { ReactNode } from "react";
import { NavLink, Outlet } from "react-router";
import { StatusBar } from "./StatusBar";

const NAV_ITEMS: { to: string; label: string; icon: ReactNode }[] = [
  { to: "/sessions", label: "Sessions", icon: <MessagesSquare size={16} /> },
  { to: "/agents", label: "Agents", icon: <Bot size={16} /> },
  { to: "/cohorts", label: "Cohorts", icon: <Layers size={16} /> },
  {
    to: "/experiments",
    label: "Experiments",
    icon: <FlaskConical size={16} />,
  },
  { to: "/replays", label: "Replays", icon: <RotateCcw size={16} /> },
  { to: "/evaluators", label: "Evaluators", icon: <ListChecks size={16} /> },
  { to: "/ops", label: "Ops", icon: <Activity size={16} /> },
];

export function Layout() {
  return (
    <div className="flex min-h-screen">
      <aside className="flex w-52 shrink-0 flex-col border-zinc-200 border-r bg-white">
        <div className="flex items-center gap-2 px-4 py-4">
          <Gauge size={20} className="text-indigo-600" />
          <div>
            <div className="font-semibold text-sm leading-tight">Kitaru</div>
            <div className="text-xs text-zinc-500 leading-tight">
              Dev Dashboard
            </div>
          </div>
        </div>
        <nav className="flex flex-col gap-0.5 px-2">
          {NAV_ITEMS.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                `flex items-center gap-2.5 rounded-md px-2.5 py-1.5 text-sm ${
                  isActive
                    ? "bg-indigo-50 font-medium text-indigo-700"
                    : "text-zinc-600 hover:bg-zinc-100 hover:text-zinc-900"
                }`
              }
            >
              {item.icon}
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <div className="flex min-w-0 flex-1 flex-col">
        <StatusBar />
        <main className="min-w-0 flex-1 px-6 py-5">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
