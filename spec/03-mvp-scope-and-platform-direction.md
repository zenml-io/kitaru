# 3. MVP Scope and Platform Direction

This reference describes both the **March MVP** and the broader shape of Kitaru, but the two should not be mixed together.

## OSS vs Pro considerations

This distinction is critical and should be kept in mind throughout the spec.

The polished demo experience — dashboard-triggered resume after compute is released, checkpoint visualization, snapshot execution — depends on Pro-backed server/workspace plumbing. Many features (replays, resume, triggers from the dashboard) are Pro-only in their full form, but local-first OSS versions of them will exist.

**OSS path:** Local-only or client-driven resume, manual retry, local replay with overrides. Works without Pro. Less polished, more manual.

**Pro path:** Dashboard-triggered resume, released-compute workflows, snapshot execution, checkpoint visualization. The full connected experience.

The spec should describe semantic contracts clearly, and note deployment-path dependencies where they exist, rather than overpromising a fully independent OSS dashboard experience. The MVP demo will use Pro capabilities.

## MVP

The March MVP is centered on the durable execution core:

- `@kitaru.flow`
- `@kitaru.checkpoint`
- local replay with overrides
- `kitaru.wait()` with a connected resume path
- manual and client-driven retry for failed executions
- typed checkpoints for dashboard rendering
- typed artifacts for dashboard rendering
- `kitaru.llm()` with LiteLLM backend, local model registry, and ZenML secrets for remote credentials
- `kitaru.log()`
- stack-first creation and selection (runner, artifact store, container registry)
- image/environment settings for remote execution
- sandbox stack component for isolated agent execution
- PydanticAI adapter
- `KitaruClient` basics (input/resume, retry, replay, list, get)
- core CLI for login, status, stack selection, execution inspection, retry, replay, and input

### What is explicitly NOT in the MVP

- `kitaru.toml` as a standalone config file (config nests under `pyproject.toml` `[tool.kitaru]` if needed)
- unified project-level configuration object as a polished surface
- automatic scheduler-driven retries/resume (broader orchestration work)
- user-facing snapshot management
- flow or checkpoint timeout as a decorator parameter
- fully independent OSS dashboard-triggered resume for released compute (requires Pro-backed server)
- OpenTelemetry-native observability (requires FastAPI middleware injection at ZenML level — deferred)

### MVP boundary restrictions

- flows are the outermost durable execution boundary
- no nested checkpoint-within-checkpoint semantics
- `wait()` is flow-only, not inside checkpoints
- adapters must not bypass these restrictions

## Broader platform direction

Kitaru may expand later with:

- automatic scheduler/orchestrator-driven retry and resume
- richer stack capabilities
- more adapters
- richer deployment and serving surfaces
- richer notifications and integrations
- more advanced scheduling and event sources
- more advanced dashboard and lineage tooling
- more ergonomic local and remote developer workflows
- user-facing snapshot inspection
- richer event sources for `wait()` (internal events, time-based triggers, third-party integrations)
- OpenTelemetry-native tracing and observability

## The rule for this doc

- first describe the **semantic contract**
- then describe what is in the **MVP**
- then, where useful, describe what may come **later**
- always note when a feature depends on Pro-backed capabilities vs being available in OSS
