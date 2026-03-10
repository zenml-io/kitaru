# 24. Consolidated roadmap

> **Compiled from:** Discord threads (March 2026, including Daytona Compute Conference notes), specs [21-notes_for_future_work.md](21-notes_for_future_work.md), [22-configuration-and-setup.md](22-configuration-and-setup.md), [23-completed-future-work.md](23-completed-future-work.md), and remaining phases from [plan.md](plan.md). Items already completed are omitted — see 23 for those.

---

## 1. Sandboxes

Sandboxes emerged as the single strongest theme from the Daytona Compute Conference and internal team discussions. Multiple speakers (Harrison Chase / LangChain, Writer CTO, Factory founder) converged on sandboxes as a critical primitive for agents.

### 1a. Sandbox as a first-class primitive

**Source:** internal discussion, spec 21

Position: sandboxes should NOT be tied to the stack. Instead, they should be a standalone registered concept — like `kitaru.llm()` is for LLM calls, there should be a `kitaru.sandbox()` call.

```
kitaru sandbox-provider register --type daytona ...
```

**Open design questions:**
- How does a sandbox primitive interface with framework adapters (PydanticAI)?
- How does it interface with the flow/checkpoint execution model?
- What does the `sandbox()` call signature look like? Does it return a handle? A context manager?
- How are file systems mounted and code pushed into the sandbox (image settings or something else)?

**Industry context (from Compute Conference):**
- Two usage patterns are emerging:
  1. The agent **runs inside** a sandbox (entire agent execution is sandboxed)
  2. The agent **uses** a sandbox to run code (tool-call level isolation)
  - Harrison Chase says it's roughly 50/50 split in practice
- Sandboxes need to support: self-hosted runtime (on-prem), Docker-in-Docker, spinning up databases and multiple services, large data volumes (e.g. 60GB databases)
- Killable sandboxes with proper data wiping + audit trails (Writer use case)
- Daytona runs 50K sandboxes per minute at scale

### 1b. Forking state from sandboxes

**Source:** internal discussion, Compute Conference

The ability to fork a sandbox's state and run two different experiments from the same state. This is "inherently possible with kitaru" because of durable execution — you replay from a checkpoint with different code/overrides.

**Key idea:** Snapshot a sandbox state at a point, then branch into multiple paths. This maps naturally to Kitaru's replay + override model, but needs explicit UX to make the connection obvious.

**Action:** Design how sandbox state forking maps to Kitaru replay semantics. Show this in the landing page and docs as a differentiator.

### 1c. Stack component vs standalone concept

**Source:** spec 21

Current spec leans toward sandbox as part of the stack/runner concept. Team consensus is that's too inflexible.

**Decision needed:** Is sandbox a stack component, a standalone registered concept, or both (stack provides default, explicit `sandbox()` call overrides)?

---

## 2. Stacks and infrastructure

### 2a. Stack creation UX

**Source:** spec 22 (decided, not yet implemented), plan.md Phase 18

Design is decided (see spec 22 for full details). Key points:
- Command is `kitaru stack create` (not `register`)
- Stack name is optional (auto-generates Docker-style names like `brave-falcon`)
- Auto-activates on creation
- Recipe types: AWS, GCP, Modal, Cloudflare
- Artifact stores CAN be shared across stacks
- Service connectors created behind the scenes
- Credentials validated at creation time
- `--verbose` shows what's happening under the hood

**Not yet implemented.** This is plan.md Phase 18.

### 2b. Deploy-time default stack

**Source:** spec 22 (partially decided)

When Kitaru is deployed remotely (Helm chart), a default stack should be created automatically. The Helm chart references an artifact store bucket as a Helm value.

**Open question:** Who creates the bucket? Pre-existing (admin creates) or provisioned by the Helm chart? Has lifecycle/IAM/cleanup implications.

### 2c. Stack composability as a selling point

**Source:** internal discussion

"Composability of the stack feels like could be yet again a selling point for enterprises. Swapping out sandbox layers, execution layers and even frameworks or tracing layers should be clearly shown in landing page."

**Action:** Once stack creation is implemented, show swappable components in landing page and marketing materials.

### 2d. Per-recipe auth flags

**Source:** spec 22 (open question)

What specific CLI flags should each recipe type (AWS, GCP, Modal, Cloudflare) accept? Depends on what auth methods ZenML service connectors support.

### 2e. AWS auth strategy

**Source:** spec 22

AWS has multiple auth methods (access keys, IAM roles, SSO profiles, instance profiles). Needs its own design discussion. Deferred from stack creation spec.

### 2f. Terraform support

**Source:** spec 22

Post-MVP. Terraform-based stack creation needs a provider or module. No design work now.

---

## 3. Frontend / Dashboard / UI

### 3a. Artifact rendering in dashboard

**Source:** spec 21

Kitaru artifacts are structured data (JSON/dicts/Pydantic models), NOT opaque blobs like ZenML artifacts (DataFrames, models). This means the dashboard can:
- Show artifact contents inline by default
- Diff artifacts between executions or replay runs
- Enable structured search/filtering over artifact values
- Render artifacts without custom materializers

**Action:** Make this distinction explicit in the dashboard rendering. Default serialization should optimize for JSON-friendly types.

### 3b. Checkpoint-by-checkpoint progress UI

**Source:** spec 21

The dashboard and terminal output should show progress checkpoint-by-checkpoint, hiding the ZenML step abstraction completely.

---

## 4. Observability and terminal UX

### 4a. Kitaru-branded terminal output

**Source:** spec 21

Requirements:
- Hide the ZenML step abstraction completely
- Show progress checkpoint-by-checkpoint
- Different visual theme from ZenML
- Use Rich for styled terminal output

**Action:** Design and implement a checkpoint-oriented progress display.

### 4b. OTEL integration for log store

**Source:** spec 21

Basic log-store configuration (`kitaru log-store set/show/reset`) is implemented. Remaining:
- OTEL entrypoint configuration
- Making log export work well outside of a stack context

### 4c. Step name cosmetics

**Source:** spec 21

- Make step names look nicer or add metadata extractable by the Kitaru UI
- Swallow or customize terminal logging when running a flow

---

## 5. Performance and scale

### 5a. Throughput testing

**Source:** internal discussion

"We should test how far our current architecture gets us in terms of throughput. Can it execute 100,000 flows a second? Can it keep that up for 10 seconds?"

**Action:** Build a throughput benchmark. Understand the ceiling of the current architecture before marketing claims.

### 5b. Cost at scale

**Source:** Compute Conference

Running 30 agents in production can cost $10-20M in token costs. Sandboxes need to be released quickly and infrastructure optimized, otherwise costs scale badly.

**Implication for Kitaru:** Cost tracking (`kitaru.log()` already captures token/cost metadata) and resource management (sandbox lifecycle, compute release on wait timeout) are important for production users.

---

## 6. Core primitives and data model

### 6a. Artifacts are different from ZenML artifacts

**Source:** spec 21

Kitaru artifacts are JSON/dicts/Pydantic models, not DataFrames/ML models. This affects:
- Default serialization strategy
- Dashboard rendering (inline display, diffing)
- Materializer approach (should need none for common cases)

**Action:** Make this distinction explicit in the artifact system design and ensure default serialization optimizes for JSON-friendly types.

### 6b. Model registry extensions

**Source:** spec 21

Core model registry is implemented (LiteLLM backend, local config, `--secret` on aliases). Remaining:
- `kitaru model show`, `kitaru model remove`, `kitaru model test`
- Import/export or team-sharing of alias configurations
- Optional fallback to a future ZenML `llm_model` stack component for credential resolution

### 6c. ZenML branch capabilities

**Source:** spec 21

Current status of `feature/pause-pipeline-runs`:
- `zenml.wait(...)` works
- Resume works (auto on Pro, manual on non-Pro)
- Wait resolution is human-only (no webhook/automated triggers yet)
- Retry CLI exists but may not work yet

**Future:** Automated wait resolution via webhooks/events.

---

## 7. Framework adapters and integrations

### 7a. Build Claude Code / Codex with Kitaru

**Source:** Discord

"You have to be able to build Claude Code or Codex with Kitaru basically."

Concrete plan:
- a) Claude Code running the main agent in Kitaru
- b) Spinning up sub-agents in Kitaru too

### 7b. Multi-language support

**Source:** internal discussion (from a Temporal user conversation)

A Temporal user said the most important things were:
1. Multi-language support
2. Deployability on customer interface
3. OTEL traces
4. Testing different code workflows on different states

Multi-language support is a major feature gap vs Temporal. Not MVP but worth tracking.

### 7c. Agent harnesses (Harrison Chase concept)

**Source:** Compute Conference

Harrison Chase's thinking: "agent harnesses" are the boundary between the agent framework and the end application. The harness defines the agent's system prompt, skills, memory, and tools. Key insight: you can zip all the markdown files that define an agent and that IS the agent — making agents portable between harnesses.

**Relevance to Kitaru:** Kitaru's stack + flow configuration could be thought of as part of the harness. Portable agent definitions that can run on different Kitaru stacks = portable harnesses.

### 7d. Memory (short-term, long-term, semantic, procedural)

**Source:** Compute Conference (Harrison Chase)

"Memory is what makes an agent." Types: short-term, long-term, semantic, procedural. The system prompt + skills + tools are procedural memory.

**Relevance to Kitaru:** `kitaru.save()` / `kitaru.load()` handle artifacts within executions. Cross-execution memory (learning from past runs) is not yet addressed. Could be a future primitive or adapter concern.

### 7e. Agentic payments and authentication

**Source:** Compute Conference (Harrison Chase)

Two authentication paradigms for agents:
1. Agents that authenticate **on behalf of a user**
2. Autonomous agents that have **their own identity** (separate paradigm)

Agentic payments might be a future consideration.

---

## 8. Configuration and setup

### 8a. Onboarding flow

**Source:** spec 22 (open question)

What's the minimum viable flow for a new user going from `kitaru login` to running a flow on a cloud stack? Interactive wizard vs single command vs docs-driven? Needs design work.

### 8b. Config directory and projects

**Source:** spec 22

Both completed. Config at `~/.config/kitaru/`, projects are a flat namespace using server default.

### 8c. Secrets

**Source:** spec 22

Decided and implemented. Secrets are always server-backed. Local dev uses env vars. `kitaru secrets set/show/list/delete` wraps ZenML's secret store.

---

## 9. Docs, blog, and site

### 9a. Blog improvements

**Source:** spec 21

- Improve the overall design
- Fix OpenGraph image(s) for the blog index page and individual posts
- Add cover images to posts

Tracked in https://github.com/zenml-io/kitaru/issues/14

### 9b. Landing page: show stack composability

**Source:** internal discussion

Swapping out sandbox layers, execution layers, frameworks, and tracing layers should be clearly shown on the landing page.

### 9c. Docs for new features

**Source:** plan.md Phase 20

Need docs pages for:
- MCP server usage
- Claude Code skill
- Sandbox component
- Deploy-time default stack config (Helm chart values)
- SDK reference (generated)

---

## 10. Skills (Claude Code)

**Source:** spec 21

- Move skill(s) to their own `zenml-io/kitaru-skills` repository (currently in-repo while iterating)
- Scoping skill (`kitaru-scoping`) is done

---

## 11. Remaining plan.md phases

Only two phases from plan.md are not yet complete:

### Phase 18: Stack creation + sandbox

**Status:** Not started. Large.

Covers: stack creation UX (designed in spec 22), sandbox stack component, deploy-time defaults. See sections 1 and 2 above for full details.

### Phase 20: Examples, docs, and polish

**Status:** Final phase. Medium.

Covers: end-to-end examples (research agent, HITL agent, LLM agent, replay demo, concurrent checkpoints, `.deploy()` example), docs site, packaging review (`kitaru-ui` bundling, MCP/skill docs, sandbox docs, Helm chart values).

---

## 12. Future vision and ideas

Items that are beyond current MVP but worth tracking for later.

### 12a. Long-running agents

**Source:** Compute Conference

A company ran one agent continuously for 16 days doing a migration. Long-running agent support (with proper sandbox lifecycle, checkpoint durability, and cost management) is a compelling use case.

### 12b. Agent-optimized documentation

**Source:** Compute Conference

Some companies serve slightly different, token-optimized content when agents hit their docs. Kitaru docs could do the same.

### 12c. Middleware / orchestration layer for sandboxes

**Source:** Compute Conference audience question

Will there be a middleware layer of sandbox managers, or will the sandbox layer absorb orchestration? Relevant to how Kitaru positions itself — it could BE that middleware layer.

### 12d. Automated wait resolution

**Source:** spec 21

Currently wait resolution is human-only. Future: webhooks, events, and automated triggers to resolve waits programmatically.

### 12e. Portable memory across executions

**Source:** Compute Conference (Harrison Chase)

Agents learning from past runs. Cross-execution memory that persists and improves agent behavior over time. Not addressed by current `save()`/`load()` which are execution-scoped.

### 12f. Agent-driven docs

**Source:** Compute Conference

A major docs platform reported that by end of 2025, >50% of their 100M views were from agents, up from 15% in 2024. Building documentation and APIs for agent consumption is increasingly important.
