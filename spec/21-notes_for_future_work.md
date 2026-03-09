# Future work

This document tracks open work items, grouped by domain. Items marked **RESOLVED** are kept for decision context but have no remaining action. Items marked **PARTIALLY RESOLVED** have some work done but still have outstanding tasks.

---

## Product: API and developer experience

How users write Kitaru code — import style, invocation patterns, language support.

### Flow invocation API — RESOLVED

Direct call syntax (`my_agent("input")`) removed — `__call__` now raises a friendly `KitaruUsageError`. `.start()` removed entirely. `.run()` is the canonical verb. `.deploy()` remains as semantic sugar for `.run(..., stack=...)`.

Two invocation patterns:

```python
# Handle-based — returns a FlowHandle
handle = my_agent.run("Build a CLI tool")

# Deploy — signals remote/deployment intent
handle = my_agent.deploy("Build a CLI tool", stack="aws-sandbox")

# Block until complete
result = my_agent.run("Build a CLI tool").wait()
```

### Import style — RESOLVED

Canonical style: `from kitaru import flow, checkpoint` for decorators. `import kitaru` for runtime helpers (`kitaru.log()`, `kitaru.wait()`, etc.). Both `@flow` and `@kitaru.flow` work mechanically, but docs/examples use the direct import form.

**Terminology alignment — DONE.** All error messages, docstrings, test assertions, skill files, docs pages, and spec chapters (02–20) updated to use canonical `@flow` / `@checkpoint` style. Runtime helpers remain namespaced (`kitaru.log()`, `kitaru.wait()`, etc.).

### Python version support: eventually target 3.11+

Current: Python 3.12+ only. Hamza wanted 3.10+ (matching ZenML). Alex's rationale for 3.12: it's the typing dividing line (modern `type` statement, PEP 695 generics).

**Consensus:** Ship with 3.12+ for MVP, plan to add 3.11 support later. This requires auditing type annotations and any 3.12-specific syntax (mainly `type` statement and some PEP 695 features).

---

## Product: configuration and setup

How users connect to Kitaru, register infrastructure, and manage credentials.

### Config directory naming

The config directory is still named `zenml` in some places, and `active project` is shown but isn't a concept exposed in Kitaru.

**Action:** Use `kitaru` as the config path name. Either hide the config path or rename it. Default project should be used silently.

### Projects: hide from users

Projects should not be exposed to users directly. Kitaru should just use the default project.

- Hamza's rationale: "because of the UI burden" and "in OSS zenml we also don't have it"
- The Kitaru UI team also won't expose the project concept
- For internal testing (especially MVP stage), keep an env var escape hatch to override the project
- **Spec inconsistency:** chapters 4, 14, and 19 still reference project config in `pyproject.toml`, project context in `kitaru info`, and project-level config as part of the config model

**Action:** Decide whether `project` remains fully internal/defaulted. If so, clean up chapters 4/14/19 and CLI vocabulary.

### Stack registration recipe UX

Hamza's vision: expose stacks as first-class citizens, hide stack components and service connectors. Users pick from pre-built recipes (AWS, GCP, Cloudflare, Modal) and register with a single command:

```
kitaru stack register --type aws --aws-profile .. --aws-secret-key .. --artifact-store s3:// --container-registry something.ecr.aws
```

Key constraints:
- **No reusing of components** — each registration creates a fresh set
- Service connectors are set up behind the scenes, never exposed to users
- Eventually support this via Terraform too

Partially reflected in chapters 4/14/19 and plan phase 18, but the recipe syntax, "no component reuse" constraint, and Terraform aspiration aren't captured elsewhere.

### Revisit whether "stack" is the right user-facing term

Hamza's explicit scope: "a stack in kitaru simply defines the orchestrator, artifact store, and container URI (optionally). I would not include any other concept in it."

If model registration and sandbox registration become separate concepts, the current "stack" abstraction may be too broad. Hamza suggested renaming to **"runtime"**.

**Action:** Once model and sandbox registration decisions are made, revisit whether "stack" should be narrowed/renamed to "runtime".

### Deploy-time default stack with artifact store

Hamza proposed setting up a **default artifact store at deploy time** so users don't need to register a single stack to get started. Logs and artifacts would go to this default store automatically.

Mentioned in chapters 4/19 and the plan, but the specific mechanism (artifact store provisioned at deploy time) and the goal (zero-stack-setup experience) are worth tracking as a concrete UX requirement.

### Secrets and infra UX for new users — PARTIALLY RESOLVED

**Decision:** Kitaru wraps ZenML's centralized secret store with `kitaru secrets set/show/list/delete`. Secrets are private by default and use env-var-shaped keys for LiteLLM compatibility. Model aliases can reference ZenML secrets via `--secret` for remote credential resolution. See updated spec chapters 4, 8, and 14.

**Remaining work:**
- End-to-end cloud stack setup experience for users who have never touched ZenML
- Service connector creation integrated into stack creation UX
- Making the whole infra setup feel native to Kitaru rather than requiring ZenML knowledge

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

## Docs

### ~~Fix sidebar double nesting~~ — FIXED

Removed duplicate separator labels from the root `meta.json` and stopped listing `index` as an explicit child page in folder `meta.json` files (both manual and in generation scripts). Each section now appears once in the sidebar.

### ~~Code snippet contrast~~ — FIXED

Switched Shiki themes to `github-light` + `github-dark` and forced code blocks to use the dark variant via `var(--shiki-dark)`.

---

## Blog

- Improve the overall design
- Fix OpenGraph image(s) for the blog index page and individual posts
- Add cover images to posts

---

## Skills (Claude Code)

- Move the skill(s) out to their own `zenml-io/kitaru-skills` repository (only here while iterating)
- ~~Add a scoping skill to be called by the authoring skill~~ — DONE. `kitaru-scoping` runs a structured interview to assess fit, identify checkpoint/wait boundaries, and produce a `flow_architecture.md`. The authoring skill now references it as a recommended first step.
