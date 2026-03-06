# 3. MVP Scope and Platform Direction

This reference describes both the **March MVP** and the broader shape of Kitaru, but the two should not be mixed together.

## MVP

The March MVP is centered on the durable execution core:

- `@kitaru.flow`
- `@kitaru.checkpoint`
- local replay with overrides
- `kitaru.wait()` with a connected resume path
- manual and client-driven retry for failed executions
- typed checkpoints for dashboard rendering
- typed artifacts for dashboard rendering
- `kitaru.llm()` with provider abstraction
- `kitaru.log()`
- unified configuration object (connection, stack, image, execution behavior)
- stack-first creation and selection (runner, artifact store, container registry, LLM model)
- image/environment settings for remote execution
- sandbox stack component for isolated agent execution
- PydanticAI adapter
- `KitaruClient` basics (list, get, input/resume, retry, replay)
- core CLI for login, status, stack selection, execution inspection, retry, replay, and input

### MVP boundary restrictions

- flows are the outermost durable execution boundary
- no nested checkpoint-within-checkpoint semantics
- `wait()` is flow-only, not inside checkpoints
- adapters must not bypass these restrictions

### What is NOT in the MVP

- automatic scheduler-driven retries/resume (broader orchestration work)
- user-facing snapshot management
- flow or checkpoint timeout as a decorator parameter
- fully independent OSS dashboard-triggered resume for released compute (may require Pro-backed server)

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

## OSS vs Pro considerations

The polished demo experience — dashboard-triggered resume after compute is released — may depend on Pro-backed server/workspace plumbing.

Manual and client-driven resume exists for OSS workflows. But the full connected experience for released-compute resume requires server capabilities that may only be fully available in the Pro deployment path.

The spec should describe semantic contracts clearly, and note deployment-path dependencies where they exist, rather than overpromising a fully independent OSS dashboard experience.

## The rule for this doc

- first describe the **semantic contract**
- then describe what is in the **MVP**
- then, where useful, describe what may come **later**
