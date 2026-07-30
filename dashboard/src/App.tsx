import { Navigate, Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { SessionDetailPage } from "./pages/SessionDetailPage";
import { SessionsPage } from "./pages/SessionsPage";

function Placeholder({ title }: { title: string }) {
  return (
    <div>
      <h1 className="font-semibold text-lg">{title}</h1>
      <p className="mt-2 text-sm text-zinc-500">Coming soon.</p>
    </div>
  );
}

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/sessions" replace />} />
        <Route path="/sessions" element={<SessionsPage />} />
        <Route path="/sessions/:id" element={<SessionDetailPage />} />
        <Route path="/agents" element={<Placeholder title="Agents" />} />
        <Route path="/cohorts" element={<Placeholder title="Cohorts" />} />
        <Route
          path="/experiments"
          element={<Placeholder title="Experiments" />}
        />
        <Route path="/replays" element={<Placeholder title="Replays" />} />
        <Route
          path="/evaluators"
          element={<Placeholder title="Evaluators" />}
        />
        <Route path="/ops" element={<Placeholder title="Ops" />} />
        <Route path="*" element={<Placeholder title="Not found" />} />
      </Route>
    </Routes>
  );
}
