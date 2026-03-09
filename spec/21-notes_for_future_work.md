# Future work

This document tracks open work items, grouped by domain. Items marked **RESOLVED** are kept for decision context but have no remaining action. Items marked **PARTIALLY RESOLVED** have some work done but still have outstanding tasks.

## Product: configuration and setup

Moved to **[22-configuration-and-setup.md](22-configuration-and-setup.md)** — covers config directory naming, projects, stack registration recipes, "stack" vs "runtime" naming, deploy-time defaults, and secrets/infra UX.

---

## Product: core primitives and data model

Runtime behavior, artifacts, models, sandboxes, and upstream ZenML dependencies.

### Artifacts are fundamentally different from ZenML artifacts

Hamza: "The notion of artifacts in kitaru needs to be meaningfully different from artifacts in zenml. In zenml artifacts usually are pandas dataframes, models etc, in kitaru they will be dicts/json/pydantic objects."

Because Kitaru artifacts are structured data (JSON/dicts/Pydantic models) rather than opaque blobs, the dashboard can:
- Show artifact contents inline by default
- Diff artifacts between executions or replay runs
- Enable structured search/filtering over artifact values
- Render artifacts without custom materializers

**Action:** Make this distinction explicit in the artifact system design, dashboard rendering spec, and materializer strategy. Default serialization should optimize for JSON-friendly types.

### Model registry — RESOLVED (with remaining extensions)

**Decision:** Models use a **local model registry** with LiteLLM as the backend. Model config is **not** stack-owned. See updated spec chapter 8. Remote credential resolution is addressed via `--secret` on model aliases.

**Remaining extensions:**
- Richer registry UX (`kitaru model show`, `kitaru model remove`, `kitaru model test`)
- Import/export or team-sharing of alias configurations
- Optional fallback to a future ZenML `llm_model` stack component for credential resolution

### Sandbox providers: register separately?

Current spec leans toward sandbox as part of the stack/runner concept. Hamza suggested sandboxes **should NOT** be part of the stack/runtime (too inflexible), and instead be a separate registered concept:

```
kitaru sandbox-provider register --type daytona ...
```

Hamza acknowledged this isn't fully thought through — particularly how sandboxes interface with framework adapters (PydanticAI) and the flow/checkpoint execution model.

**Action:** Decide whether sandbox is a stack component, a standalone registered concept, or something else.

### ZenML branch capability status (March 2026)

The `feature/pause-pipeline-runs` branch status:

| Capability | Status |
|---|---|
| `zenml.wait(...)` | Works, pauses in-progress runs |
| Resume (Pro/snapshot servers) | Auto-resume when wait condition resolved |
| Resume (non-Pro/local) | Manual resume via ZenML CLI (exists on branch) |
| Wait resolution | Human input only (no webhook/automated triggers) |
| Retry failed runs | CLI command exists but **does not work yet** |

Kitaru implications:
- `kitaru.wait()` is unblocked and can wrap the ZenML primitive
- Resume uses canonical `input` vocabulary (`client.executions.input(...)`, `kitaru executions input ...`)
- Kitaru still needs to handle both resume paths (auto vs manual) and expose a user-friendly CLI for the manual path
- `client.executions.retry(...)` and `kitaru executions retry` are implemented; continue validating against live backends
- `client.executions.replay(...)` and `kitaru executions replay` remain deferred
- `kitaru executions logs` remains deferred until Kitaru has a backend-agnostic log retrieval API
- Future: automated wait resolution via webhooks/events (currently human-only)

---

## Product: observability and terminal UX

What users see when running flows — log output, tracing, and dashboard rendering.

### Kitaru-branded terminal output

Hamza wants Kitaru's terminal output to have its own distinct look and feel: "I imagine a really sexy and more modern checkpoint by checkpoint interface." Key requirements:
- Hide the ZenML step abstraction completely
- Show progress checkpoint-by-checkpoint
- Different visual theme from ZenML

**Action:** Design and implement a checkpoint-oriented progress display with Rich.

### OTEL integration for log store

Hamza on log storage: "by default it goes where the runner stores its artifacts and they can configure maybe an entrypoint for OTEL... this is gonna be tricky to implement outside of a stack."

Basic log-store configuration (`kitaru log-store set/show/reset`) is implemented. Remaining:
- OTEL entrypoint configuration
- Making log export work well outside of a stack context

### Nice to haves

- Make step names look nicer or add metadata in step metadata extractable by the Kitaru UI
- Swallow or customize terminal logging when running a flow

---


---

## Blog

- Improve the overall design
- Fix OpenGraph image(s) for the blog index page and individual posts
- Add cover images to posts

All this is tracked already in https://github.com/zenml-io/kitaru/issues/14

---

## Skills (Claude Code)

- Move the skill(s) out to their own `zenml-io/kitaru-skills` repository (only here while iterating)
- ~~Add a scoping skill to be called by the authoring skill~~ — DONE. `kitaru-scoping` runs a structured interview to assess fit, identify checkpoint/wait boundaries, and produce a `flow_architecture.md`. The authoring skill now references it as a recommended first step.
