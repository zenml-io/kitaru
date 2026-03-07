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

Under the hood, the Kitaru server **is** the ZenML server. All server URLs are ZenML server URLs. The user does not need to know this — from their perspective, they deploy an image called `kitaru` and interact with it through the Kitaru SDK and CLI.

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

## Critical dependency: ZenML wait/resume branch

A large portion of the replay / pause / continue / wait functionality is implemented in a ZenML branch:

- **Branch:** `feature/pause-pipeline-runs` on `github.com/zenml-io/zenml`
- **Status:** Wait/resume already works on this branch
- **Staging workspace:** Michael has a staging workspace in his org with the latest changes (https://staging.cloud.zenml.io/workspaces/pause-resume/projects) (`zenml login pause-resume --pro-api-url https://staging.cloudapi.zenml.io/`) with server URL: `https://b95f9b55-zenml.staging.cloudinfra.zenml.io`

**Implementation guidance:** Start coding everything **except** replay / pause / continue / wait. There is plenty to build outside those features. The wait/resume/replay implementation should be built against the ZenML branch once it is accessible.

When implementing wait/resume/replay, the Kitaru SDK should look at the ZenML SDK and **defer to / wrap** its logic rather than reimplementing from scratch.

## Implementation order

The recommended build order reflects two key principles:

1. **SDK before CLI** (except login). The CLI wraps the SDK, so the SDK must exist first.
2. **Start with everything except replay/pause/continue/wait.** Those depend on the ZenML branch and can be added once that dependency is resolved.

### Phase 1: Foundation (no ZenML branch dependency)

1. **Login / logout / status** — CLI auth against ZenML server. This unblocks everything else.
2. **`kitaru info`** — show current connection, stack, and project context.
3. **`@kitaru.flow`** — the outermost durable execution boundary. Maps to `@pipeline`.
4. **`@kitaru.checkpoint`** — the replayable work boundary. Maps to `@step`.

### Phase 2: Core primitives (no ZenML branch dependency)

5. **`kitaru.llm()`** — thin convenience wrapper for LLM calls with tracking. Uses LiteLLM as the backend engine with a local model registry for aliases and credentials (no ZenML stack component dependency).
6. **`kitaru.log()`** — structured metadata attachment.
7. **`kitaru.save()` / `kitaru.load()`** — explicit named artifacts.
8. **`kitaru.configure()`** — project-level runtime defaults (narrow scope).

### Phase 3: Wait/resume/replay (requires ZenML branch)

9. **`kitaru.wait()`** — suspension and resume. Wraps ZenML SDK behavior from `feature/pause-pipeline-runs`.
10. **Local replay with overrides** — replay from a checkpoint with artifact overrides.
11. **Manual retry** — same-execution recovery for failed executions.

### Phase 4: Stack and config

12. **Stack selection** — `stack list`, `stack use`, `stack current`. Close to ZenML primitives.
13. **Stack creation** — expose infra details/credentials mapped to ZenML service connectors.

### Phase 5: Client API and basic CLI

14. **`KitaruClient`** — programmatic API. Priority order: input/resume, replay, retry, then list/get/artifacts.
15. **Core CLI** — login/status first, then executions input/retry/get, then broader commands.

### Phase 6: Adapters, dashboard, and polish

16. **PydanticAI adapter** — wrap agents so model requests and tool calls become checkpoint child events.
17. **Typed checkpoint rendering** — `type=` parameter drives dashboard visualization.
18. **End-to-end demo flow** — a working agent that demonstrates the full lifecycle.

### General principles

- **SDK before CLI** (except login). The CLI wraps the SDK, so the SDK must exist first.
- **Login is the first thing to build.** Everything connected depends on it.
- **`wait()` is the hardest primitive.** It requires server-side support (ZenML branch). Plan for this dependency.
- **The PydanticAI adapter is marketing-critical** but not architecturally difficult if the core primitives work.
- **Start coding everything except replay/pause/continue/wait.** Lots to be done outside those features.

## Packaging

### PyPI package

The `kitaru` package is a standard Python package published to PyPI. It includes:

- the SDK (`src/kitaru/`)
- the CLI entry point
- the bundled dashboard assets (from `kitaru-ui`)

`pip install kitaru[local]` is effectively equivalent to `pip install zenml[local]` — it provides the local development experience.

### Docker image

The Docker image is based on a ZenML image that includes:

- a specific ZenML branch (`feature/pause-pipeline-runs`) with replay/snapshot support
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

The Helm chart / deployment should include default stack configuration (artifact store bucket, runner, container registry) so that a default remote stack is ready to use on first deploy. See chapter 4 for deploy-time stack defaults.

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
- **Section 13** defines the client API priority; this section describes the build order for implementing it
- **Section 14** defines the CLI reference; the CLI is built after the SDK (except login)
