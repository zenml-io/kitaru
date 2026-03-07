# Kitaru SDK Implementation Plan

This plan breaks the full SDK build into small, sequential phases.
Each phase produces something concrete you can see, run, or test.
Start at Phase 1 and work forward. Don't skip ahead.

**Two ground rules:**

1. **Easy stuff first.** The early phases are small wins that build momentum.
2. **SDK before CLI** (except login). The CLI wraps the SDK, so build the SDK first.

**External blocker:** Phases marked with a lock icon require the ZenML
`feature/pause-pipeline-runs` branch. Skip them until that branch is accessible,
then come back. There is plenty to build without them.

---

## Phase 1: Align the package with the spec --- DONE

**Goal:** Fix naming, create module skeleton, update exports.

**What to do:**
- Rename terminology in `src/kitaru/__init__.py` from `@workflow` to `@flow`
- Create empty module files for the SDK surface:
  - `src/kitaru/flow.py`
  - `src/kitaru/checkpoint.py`
  - `src/kitaru/wait.py`
  - `src/kitaru/llm.py`
  - `src/kitaru/artifacts.py` (for save/load)
  - `src/kitaru/logging.py` (for log)
  - `src/kitaru/config.py`
  - `src/kitaru/client.py`
  - `src/kitaru/runtime.py` (execution context tracking)
- Define placeholder exports in `__init__.py` (can raise `NotImplementedError` for now)
- Update existing tests if they reference old names

**Spec references:** [01-overview.md], [18-appendix-glossary.md] (for canonical terminology)

**Estimated size:** Small. A few hours at most.

---

## Phase 2: Login, logout, status CLI

**Goal:** First real CLI commands. This unblocks everything that needs a server connection.

**What to do:**
- Add `kitaru login` command (wraps ZenML's login/connect behavior)
- Add `kitaru logout` command
- Add `kitaru status` command (show connection state, active stack, SDK version)
- Add `kitaru info` command (richer version of status: server URL, stack details, project context)
- These wrap ZenML's existing auth machinery — look at `zenml/src/zenml/cli/login.py`

**Spec references:** [04-connection-stacks-and-configuration.md] (connection model),
[14-cli-reference.md] (CLI command list), [19-implementation-guide.md] (login first)

**Estimated size:** Medium. The ZenML login flow already exists; you're wrapping it.

---

## Phase 3: `@kitaru.flow` — the outer boundary

**Goal:** The flow decorator works and maps to a ZenML dynamic pipeline.

**What to do:**
- Implement `@kitaru.flow` in `src/kitaru/flow.py`
- Wrap `@pipeline(dynamic=True)` from ZenML
- Support basic decorator parameters: `stack`, `image`, `cache`, `retries`
- Direct call path: `result = my_flow(...)` should run the flow and return a result
  - This means wrapping ZenML's pipeline call to wait for completion and extract outputs
- Handle-start path: `handle = my_flow.start(...)` returns a `FlowHandle`
  - `FlowHandle` exposes: `exec_id`, `status`, `wait()`, `get()`
- Enforce: flows cannot nest (no `@kitaru.flow` inside another flow)

**Spec references:** [05-kitaru-flow.md] (full contract), [02-execution-model.md] (rerun-from-top model)

**Estimated size:** Medium-large. The core decorator is straightforward but the
sync result extraction and FlowHandle need careful design.

---

## Phase 4: `@kitaru.checkpoint` — the durable work unit

**Goal:** Checkpoints work inside flows and persist outputs.

**What to do:**
- Implement `@kitaru.checkpoint` in `src/kitaru/checkpoint.py`
- Wrap ZenML's `@step` decorator
- Support decorator parameters: `retries`, `type` (for dashboard visualization)
- Map `retries` to ZenML's `StepRetryConfig`
- Store `type` as step metadata for dashboard rendering
- Concurrency via `.submit()` / `.result()` (pass through ZenML futures)
- Enforce: checkpoints must run inside a flow

**Spec references:** [06-kitaru-checkpoint.md] (full contract),
[02-execution-model.md] (durable outcomes)

**Estimated size:** Medium. Similar pattern to Phase 3 — wrapping ZenML steps
with Kitaru semantics.

---

## Phase 5: First working example

**Goal:** A simple end-to-end example that actually runs. This is your first real milestone.

**What to do:**
- Create a minimal example (e.g., in `examples/` or as a test):
  ```python
  import kitaru

  @kitaru.checkpoint
  def fetch_data(url: str) -> str:
      return "some data"

  @kitaru.checkpoint
  def process(data: str) -> str:
      return data.upper()

  @kitaru.flow
  def my_agent(url: str) -> str:
      data = fetch_data(url)
      return process(data)

  result = my_agent("https://example.com")
  print(result)
  ```
- Get this running locally (against a local ZenML server or default stack)
- Add an integration test for this scenario
- Fix any rough edges you discover

**Spec references:** [17-end-to-end-examples.md] (example patterns)

**Estimated size:** Small if Phases 3-4 are solid. This is mostly validation.

---

## Phase 6: Runtime context

**Goal:** Track what's currently executing so that `log()`, `save()`, and `load()` know where they are.

**What to do:**
- Implement runtime context in `src/kitaru/runtime.py`
- Track: "am I inside a flow?", "am I inside a checkpoint?", current execution ID,
  current checkpoint ID
- Use Python contextvars or a thread-local approach
- The flow decorator sets flow context on entry, clears on exit
- The checkpoint decorator sets checkpoint context on entry, clears on exit
- Provide internal helpers: `get_current_flow()`, `get_current_checkpoint()`,
  `is_inside_flow()`, `is_inside_checkpoint()`
- These are internal APIs — not part of the public surface yet

**Spec references:** [09-artifacts-metadata-and-logging.md] (context-sensitive behavior),
[06-kitaru-checkpoint.md] (checkpoint restrictions)

**Estimated size:** Small-medium. Foundational plumbing.

---

## Phase 7: `kitaru.log()`

**Goal:** Attach structured metadata to checkpoints and executions.

**What to do:**
- Implement `kitaru.log(**kwargs)` in `src/kitaru/logging.py`
- Context-sensitive behavior:
  - Inside checkpoint: attach metadata to the current checkpoint/step
  - Inside flow (outside checkpoint): attach to the execution/run
  - Outside flow: raise an error
- Map to ZenML's `Client.create_run_metadata(...)` under the hood
- Support merging across multiple `log()` calls in the same scope
- Standard keys to handle: `cost`, `tokens`, `latency`, plus arbitrary user keys

**Spec references:** [09-artifacts-metadata-and-logging.md] (log contract, global log store),
[15-observability.md] (MVP observability = metadata + log store)

**Estimated size:** Small-medium. Clean mapping to ZenML metadata APIs.

---

## Phase 8: `kitaru.save()` and `kitaru.load()`

**Goal:** Explicit named artifacts inside checkpoints.

**What to do:**
- Implement `kitaru.save(name, value, type="output", tags=None)` in `src/kitaru/artifacts.py`
- Implement `kitaru.load(exec_id, name)` in `src/kitaru/artifacts.py`
- Enforce: both are only valid inside a checkpoint (use runtime context from Phase 6)
- `save()` creates a named artifact attached to the current checkpoint via ZenML APIs
- `load()` retrieves an artifact from a previous execution by name
- Define the artifact type taxonomy: `prompt`, `response`, `context`, `input`, `output`, `blob`

**Spec references:** [09-artifacts-metadata-and-logging.md] (save/load contract, artifact taxonomy)

**Estimated size:** Medium. Artifact creation is straightforward; cross-execution
loading needs care.

---

## Phase 9: Stack selection

**Goal:** Users can list, switch, and check their active stack.

**What to do:**
- SDK functions:
  - `kitaru.list_stacks()` or via `KitaruClient`
  - `kitaru.use_stack(name)`
  - `kitaru.current_stack()`
- CLI commands:
  - `kitaru stack list`
  - `kitaru stack use <name>`
  - `kitaru stack current`
- Wrap ZenML's `Client.list_stacks()`, `Client.activate_stack()`, `Client.active_stack_model`
- Keep this to selection only — stack creation comes later

**Spec references:** [04-connection-stacks-and-configuration.md] (stack model, selection),
[14-cli-reference.md] (stack CLI tier)

**Estimated size:** Small-medium. ZenML has this infrastructure already.

---

## Phase 10: Configuration

**Goal:** Project-level defaults and config precedence.

**What to do:**
- Implement `kitaru.configure(...)` in `src/kitaru/config.py`
- Define config models (`KitaruConfig`, `ImageSettings`)
- Implement the config precedence chain:
  1. Invocation-time overrides (passed to `flow()` or `checkpoint()` calls)
  2. Decorator defaults (set in `@kitaru.flow(...)` / `@kitaru.checkpoint(...)`)
  3. `kitaru.configure(...)` project-level
  4. Environment variables
  5. `pyproject.toml` under `[tool.kitaru]`
  6. Built-in defaults
- Frozen execution spec: snapshot resolved config at flow start time
- CLI: `kitaru config show`

**Spec references:** [04-connection-stacks-and-configuration.md] (full config model,
precedence, frozen spec), [11-per-flow-and-per-checkpoint-overrides.md] (override hierarchy)

**Estimated size:** Medium. The precedence chain needs thoughtful implementation.

---

## Phase 11: `KitaruClient` — read-only

**Goal:** Programmatic API for inspecting executions, artifacts, and status.

**What to do:**
- Implement `KitaruClient` in `src/kitaru/client.py`
- Domain models:
  - `Execution` (maps from ZenML `PipelineRunResponse`)
  - `ExecutionStatus` enum: `running`, `waiting`, `completed`, `failed`, `cancelled`
  - `CheckpointCall` (maps from ZenML `StepRunResponse`)
  - `ArtifactRef`
- Read-only client methods:
  - `client.executions.get(exec_id)`
  - `client.executions.list(flow=..., status=..., limit=...)`
  - `client.executions.latest(flow=...)`
  - `client.artifacts.list(exec_id, ...)`
  - `client.artifacts.get(artifact_id)`
  - `artifact.load()` — materialize to Python value
- Translate ZenML data models into Kitaru's cleaner domain models

**Spec references:** [13-client-api.md] (client API surface and priority order),
[18-appendix-glossary.md] (execution, call record, artifact definitions)

**Estimated size:** Medium-large. Lots of model translation work.

---

## Phase 12: `kitaru.llm()`

**Goal:** Tracked LLM calls with provider abstraction.

**What to do:**
- Implement `kitaru.llm(prompt, model=None, system=None, temperature=None, max_tokens=None, name=None)`
- Two modes based on context:
  - Inside flow (outside checkpoint): creates a synthetic durable call boundary
  - Inside checkpoint: creates a child event (tracked but not a replay boundary)
- Auto-create artifacts: prompt artifact + response artifact
- Auto-log metadata: token usage, cost, latency
- Model resolution: alias handling (`fast`, `smart`) and concrete `provider:model` format
- Resolve model against the frozen execution spec / stack config

**Spec references:** [08-kitaru-llm.md] (full contract),
[04-connection-stacks-and-configuration.md] (model aliases, llm_model stack component)

**External note:** The `llm_model` ZenML stack component may not exist yet.
You may need a temporary shim or design spike before this phase.

**Estimated size:** Medium-large. Provider abstraction and model resolution are
the complex parts.

---

## Phase 13: Error handling and failure journaling

**Goal:** Clean error behavior across the SDK.

**What to do:**
- Define Kitaru exception hierarchy
- Implement failure journaling: failed checkpoint attempts recorded with their errors
- Checkpoint retry behavior: record failed attempts, succeed on retry, expose attempt history
- Clear error messages for:
  - Calling checkpoint outside a flow
  - Calling save/load/log outside proper context
  - Wait validation failures
  - Divergence during replay
- Runtime vs user-code error distinction

**Spec references:** [12-error-handling.md] (full error model),
[02-execution-model.md] (durable outcomes include failures)

**Estimated size:** Medium. Mostly about defining clear boundaries and good messages.

---

## Phase 14: Execution CLI commands

**Goal:** CLI commands for inspecting and managing executions.

**What to do:**
- `kitaru executions list` — list recent executions
- `kitaru executions get <exec_id>` — show execution details
- `kitaru executions logs <exec_id> [--follow]` — stream execution logs
- `kitaru artifacts list <exec_id>` — list artifacts for an execution
- `kitaru artifacts get <artifact_id> [--download]` — inspect/download an artifact
- All commands wrap `KitaruClient` methods from Phase 11

**Spec references:** [14-cli-reference.md] (CLI command tiers)

**Estimated size:** Medium. Straightforward CLI wrapping of client API.

---

## Phase 15: `kitaru.wait()` + resume

**Goal:** Durable suspension and resume. **Requires ZenML `feature/pause-pipeline-runs` branch.**

**What to do:**
- Implement `kitaru.wait(schema=..., name=..., question=..., timeout=..., metadata=...)`
- Flow-only enforcement (not valid inside checkpoints)
- Waiting execution model:
  - Record pending wait info (name, question, schema)
  - Suspend the current execution
  - On resume: validate input against schema, return it
- Client method: `client.executions.input(exec_id, wait=..., value=...)`
- CLI: `kitaru executions input <exec_id> --wait <name> --value <json>`
- Wait timeout means resource-release timeout, not expiration

**Spec references:** [07-kitaru-wait.md] (full contract and lifecycle),
[19-implementation-guide.md] (blocked by ZenML branch)

**Estimated size:** Large. This is the hardest primitive. Wrap ZenML's implementation,
don't reimplement.

---

## Phase 16: Replay and overrides

**Goal:** Create new executions derived from previous ones. **Partially blocked by ZenML branch.**

**What to do:**
- Client methods:
  - `client.executions.replay(exec_id, from_=..., overrides=...)`
  - `client.executions.retry(exec_id)`
  - `client.executions.cancel(exec_id)`
- CLI commands:
  - `kitaru executions replay <exec_id> --from <call_name> [--override ...]`
  - `kitaru executions retry <exec_id>`
  - `kitaru executions cancel <exec_id>`
- Override grammar: `flow.input.*`, `checkpoint.*`, `wait.*`
- Replay-point resolution to durable call instance IDs
- Divergence detection: surface clear errors when call sequence doesn't match
- Map onto ZenML's `Pipeline.replay()` and `BaseStep.replay()` where possible

**Spec references:** [10-replay-and-overrides.md] (replay semantics),
[11-per-flow-and-per-checkpoint-overrides.md] (override hierarchy),
[18-appendix-glossary.md] (replay, retry, resume, divergence definitions)

**Estimated size:** Large. Complex interaction with ZenML replay machinery.

---

## Phase 17: PydanticAI adapter

**Goal:** Wrap PydanticAI agents so model/tool calls become tracked child events.

**What to do:**
- Implement `kitaru.adapters.pydantic_ai.wrap(agent)` in `src/kitaru/adapters/pydantic_ai.py`
- Behavior: when a wrapped agent runs inside a checkpoint:
  - Each model request becomes a child event (`type='llm_call'`)
  - Each tool call becomes a child event (`type='tool_call'`)
  - The enclosing checkpoint remains the replay boundary
- Adapter must NOT bypass Kitaru restrictions (no nested checkpoints, etc.)
- Capture prompt/response artifacts and usage metadata per child event
- Add `pydantic-ai` as an optional dependency

**Spec references:** [16-framework-adapters.md] (adapter philosophy and PydanticAI contract),
[03-mvp-scope-and-platform-direction.md] (adapter is marketing-critical)

**Estimated size:** Medium. Straightforward if core primitives work well.

---

## Phase 18: Stack creation

**Goal:** Users can create stacks with infrastructure details.

**What to do:**
- SDK: stack creation API
- CLI: `kitaru stack create <name> --runner ... --artifact-store ... [--llm-model ...]`
- This is NOT a thin ZenML wrapper — it needs a higher-level UX:
  - Map user-friendly flags to ZenML flavors + components + service connectors
  - Assemble the stack from those components
- Support default stack configuration at deploy time (Helm chart defaults)

**Spec references:** [04-connection-stacks-and-configuration.md] (stack creation,
deploy-time defaults, capability checks)

**Estimated size:** Large. Significant UX design work on top of ZenML's stack model.

---

## Phase 19: Agent-native integrations

**Goal:** MCP server and Claude Code skill for AI-assisted Kitaru development.

**What to do:**
- Add `kitaru[mcp]` optional extra in `pyproject.toml`
- Create `src/kitaru/mcp/` with MCP tools wrapping `KitaruClient`
- Register `kitaru-mcp` console script
- Create `src/kitaru/skills/kitaru-authoring.md` (Claude Code skill)
- Include skill as package data
- CI matrix: test both base install and `[mcp]` extra
- Lazy import guard for MCP dependencies

**Spec references:** [20-agent-native-integrations.md] (full MCP + skill spec)

**Estimated size:** Medium. Depends on stable `KitaruClient` and CLI.

---

## Phase 20: End-to-end examples, docs, and polish

**Goal:** Working examples that demonstrate the full lifecycle.

**What to do:**
- Research agent example (multi-checkpoint, artifacts, metadata)
- Agent with human-in-the-loop (wait + resume)
- Agent with LLM calls (kitaru.llm() or PydanticAI adapter)
- Replay/override demo
- Concurrent checkpoints demo
- Update docs site with SDK reference
- Final packaging review (version, extras, entry points)

**Spec references:** [17-end-to-end-examples.md] (all example patterns)

**Estimated size:** Medium. Integration work, not new primitives.

---

## Visual overview

```
Phase 1  ── Naming + skeleton ─────────────────────────── DONE
Phase 2  ── Login/logout/status CLI ───────────────────── Easy win
Phase 3  ── @kitaru.flow ──────────────────────────────── Core
Phase 4  ── @kitaru.checkpoint ────────────────────────── Core
Phase 5  ── First working example ─────────────────────── Milestone!
Phase 6  ── Runtime context ───────────────────────────── Plumbing
Phase 7  ── kitaru.log() ──────────────────────────────── Core
Phase 8  ── kitaru.save() / kitaru.load() ─────────────── Core
Phase 9  ── Stack selection ───────────────────────────── Easy win
Phase 10 ── Configuration ────────────────────────────── Medium
Phase 11 ── KitaruClient (read-only) ─────────────────── Medium
Phase 12 ── kitaru.llm() ─────────────────────────────── Medium (may need upstream)
Phase 13 ── Error handling ───────────────────────────── Medium
Phase 14 ── Execution CLI commands ───────────────────── Medium
Phase 15 ── kitaru.wait() + resume ───────────────────── BLOCKED (ZenML branch)
Phase 16 ── Replay + overrides ───────────────────────── BLOCKED (ZenML branch)
Phase 17 ── PydanticAI adapter ───────────────────────── Medium
Phase 18 ── Stack creation ───────────────────────────── Large
Phase 19 ── Agent-native integrations ────────────────── Medium
Phase 20 ── Examples, docs, polish ───────────────────── Final
```

## Suggested milestones

| Milestone | After phase | What you can show |
|---|---|---|
| "It runs" | 5 | A flow with checkpoints executes and returns a result |
| "It's useful" | 8 | Metadata, artifacts, and structured logging work |
| "It's connected" | 11 | Client can inspect executions and artifacts programmatically |
| "It's smart" | 12 | LLM calls are tracked with cost/token metadata |
| "It's durable" | 16 | Wait, resume, replay, and retry all work |
| "It's complete" | 20 | Full SDK with adapters, CLI, examples, and docs |

## What to do when you're blocked

Phases 15 and 16 require the ZenML `feature/pause-pipeline-runs` branch.
If that branch isn't accessible yet:

1. Build Phases 1-14 and 17 (plenty of work there)
2. Stub `wait()` with a clear `NotImplementedError("Requires ZenML wait/resume support")`
3. When the branch becomes available, come back and implement Phases 15-16
4. Phase 12 (`kitaru.llm()`) may also need an upstream `llm_model` stack component —
   if it's not ready, stub the model resolution and hard-code a direct provider call

## How to use this plan

- **Pick up Phase 1.** Do it. Commit. Move on.
- **Each phase should be its own PR** (or a small set of commits).
- **Write tests as you go** — each phase should include tests for what it builds.
- **Run `just check` and `just test` before every commit.**
- **When you finish a phase, check it off** and move to the next.
- **If something feels too big**, break it into sub-tasks. That's fine.
- **If you get stuck**, re-read the referenced spec docs. They're detailed.
