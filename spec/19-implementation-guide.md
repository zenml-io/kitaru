# 19. Implementation Guide

This section captures organizational, architectural, and sequencing decisions for building the Kitaru MVP. It is derived from team discussions and reflects the practical realities of shipping the product.

## Organizational deliverables

The MVP requires the following artifacts:

| Deliverable | Description |
| --- | --- |
| `kitaru` PyPI package | The published Python SDK |
| `kitaru` Docker image | Based on a special ZenML image (includes a ZenML branch + cloud plugins) |
| `kitaru-ui` dashboard | Bundled into both the PyPI package and the Docker image |
| `docs.kitaru.ai` | Documentation site |
| PydanticAI adapter | Framework adapter shipped with the SDK |
| Sandbox stack component | Compute sandbox for safe agent execution |

## Architecture

### Server reality

Kitaru does **not** have its own server or API endpoints.

Under the hood, the Kitaru server **is** the ZenML server. The user does not need to know this — from their perspective, they deploy an image called `kitaru` and interact with it through the Kitaru SDK and CLI.

The SDK is what gets built. The server, API endpoints, and backend execution machinery come from ZenML.

### Dashboard

The dashboard lives in the `kitaru-ui` repository. It communicates with ZenML endpoints. It is bundled into:

- the `kitaru` PyPI package
- the `kitaru` Docker image

Both serve the dashboard on the same port as the server.

### Cloud plugins and Pro features

The Docker image includes ZenML cloud plugins because:

- checkpoint visualization requires Pro-backed dashboard features
- dashboard-triggered resume requires snapshot execution (Pro capability)
- the demo path optimizes for the connected Pro experience

For the MVP demo, the image is deployed as a Pro workspace with:

- regular login (not full Pro RBAC)
- snapshot execution configured via environment variables
- the Kitaru UI bundled in

This means the MVP demo uses a Pro-capable ZenML server image presented as the Kitaru product. The OSS path (local-only, client-driven resume) works without Pro.

## Implementation order

The recommended build order, from the team discussion:

### Phase 1: Foundation

1. **Login / logout / status** — CLI auth against ZenML server. This unblocks everything else.
2. **`kitaru info`** — show current connection, stack, and project context.
3. **`@kitaru.flow`** — the outermost durable execution boundary. Maps to `@pipeline`.
4. **`@kitaru.checkpoint`** — the replayable work boundary. Maps to `@step`.

### Phase 2: Core primitives

5. **`kitaru.llm()`** — thin convenience wrapper for LLM calls with tracking.
6. **`kitaru.log()`** — metadata attachment.
7. **`kitaru.save()` / `kitaru.load()`** — explicit named artifacts.
8. **`kitaru.wait()`** — suspension and resume. This is the hardest primitive.
9. **`kitaru.configure()`** — project-level runtime defaults (narrow scope).

### Phase 3: Stack and replay

10. **Stack selection** — `stack list`, `stack use`, `stack current`. Close to ZenML primitives.
11. **Local replay with overrides** — replay from a checkpoint with artifact overrides.
12. **Manual retry** — same-execution recovery for failed executions.

### Phase 4: Adapters and CLI

13. **PydanticAI adapter** — wrap agents so model requests and tool calls become checkpoint child events.
14. **Full CLI** — executions list/get/logs/input/retry/replay/cancel, artifacts, config.

### Phase 5: Dashboard and polish

15. **Typed checkpoint rendering** — `type=` parameter drives dashboard visualization.
16. **`KitaruClient`** — programmatic API for list, get, input/resume, retry, replay.
17. **End-to-end demo flow** — a working agent that demonstrates the full lifecycle.

### General principles

- **SDK before CLI** (except login). The CLI wraps the SDK, so the SDK must exist first.
- **Login is the first thing to build.** Everything connected depends on it.
- **`wait()` is the hardest primitive.** It requires server-side support for suspend/resume. Plan extra time.
- **The PydanticAI adapter is marketing-critical** but not architecturally difficult if the core primitives work.

## Packaging

### PyPI package

The `kitaru` package is a standard Python package published to PyPI. It includes:

- the SDK (`src/kitaru/`)
- the CLI entry point
- the bundled dashboard assets (from `kitaru-ui`)

### Docker image

The Docker image is based on a ZenML image that includes:

- a specific ZenML branch (not merged to main) with replay/snapshot support
- ZenML cloud plugins (for Pro features like checkpoint visualization and snapshot execution)
- the Kitaru SDK
- the bundled dashboard

The Dockerfile and image build pipeline need to be set up by the infrastructure team, not the SDK developer.

### Version management

The version is defined in `pyproject.toml` and read at runtime via `importlib.metadata.version("kitaru")`. Never hardcode it.

## Deployment model

For the MVP demo:

1. Deploy the Pro-capable ZenML image with Kitaru branding
2. Configure snapshot execution via environment variables pointing to Kubernetes clusters
3. Use regular login (not full Pro RBAC)
4. The Kitaru UI is accessible through the same server port

This setup means:

- the server looks like an OSS Kitaru deployment to the user
- behind the scenes, it has Pro capabilities (snapshots, checkpoint visualization)
- no dependency on the cloud API user management system

## Sandbox stack component

The sandbox is a stack component that provides isolated compute for agent execution.

This is important for use cases like coding agents, where you do not want the agent running arbitrary code on your local machine or production infrastructure.

The sandbox provides:

- isolated execution environment
- resource limits
- safe code execution for tool calls

The exact shape of the sandbox component is still being defined, but it is an explicit MVP deliverable.

## Relationship to other spec sections

This implementation guide complements the semantic spec (sections 1-18) with practical build guidance:

- **Section 3** defines what is in the MVP semantically
- **This section** defines how to build and ship it
- **Section 4** defines the configuration model; this section describes the packaging that delivers it
- **Section 14** defines the CLI reference; this section describes the build order for implementing it
