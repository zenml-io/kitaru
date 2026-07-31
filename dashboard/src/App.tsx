import { Navigate, Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { AgentDetailPage, AgentsPage } from "./pages/AgentsPage";
import {
  CohortDetailPage,
  CohortsPage,
  CohortVersionDetailPage,
} from "./pages/CohortsPage";
import { EvaluatorDetailPage, EvaluatorsPage } from "./pages/EvaluatorsPage";
import { ExperimentRunPage } from "./pages/ExperimentRunPage";
import { ExperimentDetailPage, ExperimentsPage } from "./pages/ExperimentsPage";
import { JobDetailPage, OpsPage } from "./pages/OpsPage";
import { ReplayDetailPage, ReplaysPage } from "./pages/ReplaysPage";
import { SessionDetailPage } from "./pages/SessionDetailPage";
import { SessionsPage } from "./pages/SessionsPage";

function NotFound() {
  return (
    <div>
      <h1 className="font-semibold text-lg">Not found</h1>
      <p className="mt-2 text-sm text-zinc-500">
        This page does not exist. Pick a section from the sidebar.
      </p>
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
        <Route path="/agents" element={<AgentsPage />} />
        <Route path="/agents/:id" element={<AgentDetailPage />} />
        <Route path="/cohorts" element={<CohortsPage />} />
        <Route path="/cohorts/:id" element={<CohortDetailPage />} />
        <Route
          path="/cohort-versions/:id"
          element={<CohortVersionDetailPage />}
        />
        <Route path="/experiments" element={<ExperimentsPage />} />
        <Route path="/experiments/:id" element={<ExperimentDetailPage />} />
        <Route path="/runs/:id" element={<ExperimentRunPage />} />
        <Route path="/replays" element={<ReplaysPage />} />
        <Route path="/replays/:id" element={<ReplayDetailPage />} />
        <Route path="/evaluators" element={<EvaluatorsPage />} />
        <Route path="/evaluators/:id" element={<EvaluatorDetailPage />} />
        <Route path="/ops" element={<OpsPage />} />
        <Route path="/jobs/:id" element={<JobDetailPage />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
