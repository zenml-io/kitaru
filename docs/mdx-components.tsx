import defaultMdxComponents from 'fumadocs-ui/mdx';
import * as Python from 'fumadocs-python/components';
import { Accordion, Accordions } from 'fumadocs-ui/components/accordion';
import { Callout } from 'fumadocs-ui/components/callout';
import { Step, Steps } from 'fumadocs-ui/components/steps';
import { Tab, Tabs } from 'fumadocs-ui/components/tabs';
import type { MDXComponents } from 'mdx/types';
import {
  AgentFactoryOverviewDiagram,
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
} from '@/components/diagrams';

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    ...Python,
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
    AgentFactoryOverviewDiagram,
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
