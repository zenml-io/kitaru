import * as Python from "fumadocs-python/components";
import { Accordion, Accordions } from "fumadocs-ui/components/accordion";
import { Callout } from "fumadocs-ui/components/callout";
import { Step, Steps } from "fumadocs-ui/components/steps";
import { Tab, Tabs } from "fumadocs-ui/components/tabs";
import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";
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
import { PySourceCode } from "@/components/py-source-code";

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    ...Python,
    // Shadow the broken PySourceCode from fumadocs-python 0.1.1; see the
    // comment in components/py-source-code.tsx.
    PySourceCode,
    Accordion,
    Accordions,
    Callout,
    Step,
    Steps,
    Tab,
    Tabs,
    FlowShapeDiagram,
    CheckpointReplayDiagram,
    WaitResumeDiagram,
    ComponentsDiagram,
    GatewayStackDiagram,
    ExecutionArchitectureDiagram,
    ThreePlanesDiagram,
    FailedCheckpointDiagram,
    HarnessRuntimePlatformDiagram,
    BuyerMatrixDiagram,
    AgentHarnessPlatformOverviewDiagram,
    DurableAgentReplayDiagram,
    SandboxBoundaryDiagram,
    SandboxReplaySideEffectsDiagram,
    CredentialProxyDiagram,
    ServiceCallPathsDiagram,
    SkillProcedureFlowDiagram,
    HitlPauseResumeDiagram,
    ...components,
  };
}
