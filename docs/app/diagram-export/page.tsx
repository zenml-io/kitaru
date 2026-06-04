// TEMPORARY ROUTE — used by scripts/render-diagrams.mjs to capture the React
// diagram components as static PNGs for the GitBook docs port. Safe to delete
// once all diagrams referenced by docs/book/ have been rendered to
// docs/book/.gitbook/assets/. Not linked from the docs nav.
import {
  AgentHarnessPlatformOverviewDiagram,
  BuyerMatrixDiagram,
  CheckpointReplayDiagram,
  ComponentsDiagram,
  CredentialProxyDiagram,
  DurableAgentReplayDiagram,
  ExecutionArchitectureDiagram,
  FailedCheckpointDiagram,
  FlowShapeDiagram,
  GatewayStackDiagram,
  HarnessRuntimePlatformDiagram,
  HitlPauseResumeDiagram,
  SandboxBoundaryDiagram,
  SandboxReplaySideEffectsDiagram,
  ServiceCallPathsDiagram,
  SkillProcedureFlowDiagram,
  ThreePlanesDiagram,
  WaitResumeDiagram,
} from "@/components/diagrams";

// Map of asset slug → diagram component. The slug becomes the PNG filename in
// docs/book/.gitbook/assets/<slug>.png and matches the <img src> in the .md.
const DIAGRAMS: Record<string, React.ReactNode> = {
  "flow-shape": <FlowShapeDiagram />,
  "checkpoint-replay": <CheckpointReplayDiagram />,
  "wait-resume": <WaitResumeDiagram />,
  "harness-runtime-platform": <HarnessRuntimePlatformDiagram />,
  "buyer-matrix": <BuyerMatrixDiagram />,
  components: <ComponentsDiagram />,
  "execution-architecture": <ExecutionArchitectureDiagram />,
  "three-planes": <ThreePlanesDiagram />,
  "failed-checkpoint": <FailedCheckpointDiagram />,
  "gateway-stack": <GatewayStackDiagram />,
  "agent-harness-platform-overview": <AgentHarnessPlatformOverviewDiagram />,
  "durable-agent-replay": <DurableAgentReplayDiagram />,
  "sandbox-boundary": <SandboxBoundaryDiagram />,
  "sandbox-replay-side-effects": <SandboxReplaySideEffectsDiagram />,
  "skill-procedure-flow": <SkillProcedureFlowDiagram />,
  "credential-proxy": <CredentialProxyDiagram />,
  "service-call-paths": <ServiceCallPathsDiagram />,
  "hitl-pause-resume": <HitlPauseResumeDiagram />,
};

export default function DiagramExportPage() {
  return (
    <main style={{ background: "#ffffff", padding: 24 }}>
      {Object.entries(DIAGRAMS).map(([slug, node]) => (
        <div
          key={slug}
          data-diagram={slug}
          style={{ width: 900, background: "#ffffff", padding: 8 }}
        >
          {node}
        </div>
      ))}
    </main>
  );
}
