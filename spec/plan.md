# Kitaru SDK Implementation Plan

This plan breaks the full SDK build into small, sequential phases.
Each phase produces something concrete you can see, run, or test.
Start at Phase 1 and work forward. Don't skip ahead.

**Two ground rules:**

1. **Easy stuff first.** The early phases are small wins that build momentum.
2. **SDK before CLI** (except login). The CLI wraps the SDK, so build the SDK first.

**External blocker (updated March 2026):** The ZenML `feature/pause-pipeline-runs`
branch now has working wait/resume support and Kitaru has wrapped Phase 15.
Phase 16 remains **partially blocked** because replay wrappers are not yet
implemented end-to-end in Kitaru. See the "What to do when you're blocked"
section at the bottom for current status.

**Ownership boundary:** ZenML owns the hard durability machinery (retry, resume,
replay, snapshots, divergence detection). Kitaru defines the user-visible contract
and provides a simpler developer-facing model on top. When this plan says
"implement X", it means "wrap/surface the ZenML behavior through Kitaru's API",
not "reimplement from scratch."

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

## Phase 2: Login, logout, status CLI --- DONE

**Goal:** First real CLI commands. This unblocks everything that needs a server connection.

**What to do:**
- Add `kitaru login` command (wraps ZenML's login/connect behavior)
  - Login targets a ZenML server URL under the hood, but from the user's
    perspective they connect to "their Kitaru server"
- Add `kitaru logout` command
- Add `kitaru status` command (show connection state, active stack, SDK version)
- Add `kitaru info` command (richer version of status: server URL, stack details, project context)
- Auth/connection state is stored in **global user config** (on the machine, not
  in the repo) and participates in config precedence (see Phase 10)
- These wrap ZenML's existing auth machinery — look at `zenml/src/zenml/cli/login.py`

**Spec references:** [04-connection-stacks-and-configuration.md] (connection model, global user config),
[14-cli-reference.md] (CLI command list), [19-implementation-guide.md] (login first)

**Estimated size:** Medium. The ZenML login flow already exists; you're wrapping it.

---

## Phase 3: `@kitaru.flow` — the outer boundary --- DONE

**Goal:** The flow decorator works and maps to a ZenML dynamic pipeline.

**What to do:**
- Implement `@kitaru.flow` in `src/kitaru/flow.py`
- Wrap `@pipeline(dynamic=True)` from ZenML
- Support basic decorator parameters: `stack`, `image`, `cache`, `retries`
- The `@kitaru.flow` decorator returns a callable object that exposes three
  invocation patterns:
  - **Direct call:** `result = my_flow(...)` — blocks until complete, returns result
  - **Start:** `handle = my_flow.start(...)` — returns a `FlowHandle` for longer-running
    executions. `FlowHandle` exposes: `exec_id`, `status`, `wait()`, `get()`
  - **Deploy:** `handle = my_flow.deploy(..., stack="prod")` — sugar for
    `.start(..., stack=...)` that communicates remote-execution intent more clearly
- Enforce: flows cannot nest as one execution (no `@kitaru.flow` inside another
  flow's execution boundary). A flow *can* start another flow, but that creates
  a separate execution.
- The flow is the **main config surface** — all execution-relevant settings
  (infrastructure, image, behavior) flow through it

**Spec references:** [05-kitaru-flow.md] (full contract, including `.deploy()`),
[02-execution-model.md] (rerun-from-top model),
[04-connection-stacks-and-configuration.md] (flow as config surface)

**Estimated size:** Medium-large. The core decorator is straightforward but the
sync result extraction, FlowHandle, and `.deploy()` sugar need careful design.

---

## Phase 4: `@kitaru.checkpoint` — the durable outcome boundary --- DONE

**Goal:** Checkpoints work inside flows and persist durable outcomes (not just outputs).

**What to do:**
- Implement `@kitaru.checkpoint` in `src/kitaru/checkpoint.py`
- Wrap ZenML's `@step` decorator
- Support decorator parameters: `retries`, `type` (for dashboard visualization)
- Map `retries` to ZenML's `StepRetryConfig`
- Store `type` as step metadata for dashboard rendering
- Concurrency via `.submit()` / `.result()` (pass through ZenML futures)
- Enforce MVP restrictions:
  - Checkpoints must run inside a flow
  - No `wait()` inside checkpoints
  - No nested checkpoint-within-checkpoint semantics
  - `kitaru.llm()` inside a checkpoint is a **child event**, not a nested replay boundary

**Spec references:** [06-kitaru-checkpoint.md] (full contract),
[02-execution-model.md] (durable outcomes, not just outputs)

**Estimated size:** Medium. Similar pattern to Phase 3 — wrapping ZenML steps
with Kitaru semantics.

---

## Phase 5: First working example ✅ DONE

**Goal:** A simple end-to-end example that actually runs. This is your first real milestone.

**Status:** Completed.

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

## Phase 6: Runtime context --- DONE

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

## Phase 7: `kitaru.log()` --- DONE

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
- **Important distinction:** `kitaru.log()` is for structured metadata (cost, quality,
  usage annotations). It is separate from runtime logs, which go to the global
  log store (see Phase 7b)

**Spec references:** [09-artifacts-metadata-and-logging.md] (log contract, global log store),
[15-observability.md] (MVP observability = metadata + log store)

**Estimated size:** Small-medium. Clean mapping to ZenML metadata APIs.

---

## Phase 7b: Global log store --- DONE

**Goal:** Runtime logs have a configurable backend.

**What to do:**
- Runtime logs default to the **artifact store** (no extra infrastructure needed)
- Users can optionally switch the log backend to an OTel-compatible provider
  via `kitaru log-store set`
- CLI commands:
  - `kitaru log-store set <backend> --endpoint <url> --api-key {{secret_name.api_key}}`
  - `kitaru log-store show`
  - `kitaru log-store reset`
- This is a global setting — it switches the default log backend for all flows
- Log store configuration is part of the unified config model

**Spec references:** [09-artifacts-metadata-and-logging.md] (log store model),
[14-cli-reference.md] (log-store CLI in Tier 3),
[15-observability.md] (MVP observability = global log store + metadata)

**Estimated size:** Small-medium. The artifact store default path is simple;
the switchable backend adds a config layer.

---

## Phase 8: `kitaru.save()` and `kitaru.load()` --- DONE

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

## Phase 9: Stack selection --- DONE

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
- Stack selection precedence:
  1. `my_flow.deploy(..., stack="prod")` or `my_flow.start(..., stack="prod")`
  2. `@kitaru.flow(stack="prod")` — decorator default
  3. Environment variable override
  4. Active user-selected stack (from global config)
  5. Implicit `local`

**Spec references:** [04-connection-stacks-and-configuration.md] (stack model,
selection, stack selection precedence), [14-cli-reference.md] (stack CLI tier)

**Estimated size:** Small-medium. ZenML has this infrastructure already.

---

## Phase 10: Configuration --- DONE

**Goal:** Unified configuration model with clear precedence.

**What to do:**
- Implement `kitaru.configure(...)` in `src/kitaru/config.py`
- Define config models (`KitaruConfig`, `ImageSettings`)
- Separate **connection config** from **execution config**:
  - Connection config: server URL, auth token, workspace — resolved before any flow runs
  - Execution config: stack, image, cache, retries — resolved at flow start time
- Implement the config precedence chain:
  1. Invocation-time overrides (`my_flow.deploy(..., stack="prod")` or `.start(...)`)
  2. Decorator defaults (`@kitaru.flow(stack="prod")`)
  3. `kitaru.configure(...)` — explicit runtime configuration
  4. Environment variables (`KITARU_STACK`, `KITARU_SERVER_URL`, etc.)
  5. Project config — `pyproject.toml` under `[tool.kitaru]` (checked into the repo)
  6. Global user config — stored on the machine (active stack, auth token)
  7. Built-in defaults — implicit `local` stack, default settings
- Connection precedence (separate from execution config):
  1. Explicit `connect()` or client args
  2. Environment variables
  3. Global user config
  4. None (local-only mode)
- Frozen execution spec: snapshot resolved config at flow start time and persist
  with the execution (so config can't drift while an execution is waiting)
- Secrets: reference using ZenML syntax `{{secret_name.secret_key}}` — resolution handled by ZenML
- **Note:** Rich project-level config and `kitaru config show` are Tier 3 / later.
  The MVP config surface is global user config + decorator/invocation overrides.

**Spec references:** [04-connection-stacks-and-configuration.md] (full config model,
precedence, frozen spec, secrets), [11-per-flow-and-per-checkpoint-overrides.md]
(override hierarchy)

**Estimated size:** Medium. The precedence chain needs thoughtful implementation.

---

## Phase 11: `KitaruClient` — execution management first --- DONE

**Goal:** Programmatic API for managing and inspecting executions.

**What to do:**
- Implement `KitaruClient` in `src/kitaru/client.py`
- Domain models:
  - `Execution` (maps from ZenML `PipelineRunResponse`)
  - `ExecutionStatus` enum: `running`, `waiting`, `completed`, `failed`, `cancelled`
  - `CheckpointCall` (maps from ZenML `StepRunResponse`)
  - `ArtifactRef`
- Client methods in **priority order** (most-needed first):
  - **Tier 1 — core operations:**
    - `client.executions.input(exec_id, wait=..., value=...)` — provide input to a waiting execution (resume)
    - `client.executions.retry(exec_id)` — same-execution recovery
    - `client.executions.replay(exec_id, from_=..., **flow_inputs, overrides=...)` — new execution from a previous one
    - `client.executions.get(exec_id)` — inspect an execution
  - **Tier 2 — browsing and lookup:**
    - `client.executions.list(flow=..., status=..., limit=...)`
    - `client.executions.latest(flow=...)`
    - `client.executions.cancel(exec_id)`
  - **Tier 3 — artifacts (later):**
    - `client.artifacts.list(exec_id, ...)`
    - `client.artifacts.get(artifact_id)`
    - `artifact.load()` — materialize to Python value
- Translate ZenML data models into Kitaru's cleaner domain models
- **Updated March 2026:** `retry` is implemented in Kitaru (`client.executions.retry`).
  `input` (resume) and `replay` remain stubbed until Kitaru wraps wait/resume/replay
  end-to-end (Phase 15/16).

**Spec references:** [13-client-api.md] (client API surface and **priority order**),
[18-appendix-glossary.md] (execution, call record, artifact definitions)

**Estimated size:** Medium-large. Lots of model translation work. Priority order
matters — build the most-needed operations first, not the easiest.

---

## Phase 11.5: Secrets surface --- DONE

**Goal:** Wrap ZenML's centralized secret store with a Kitaru-shaped CLI surface. This unblocks remote credential resolution for `kitaru.llm()`.

**What to do:**
- Implement `kitaru secrets set <name> --KEY=value ...` — creates or updates a ZenML secret
- Implement `kitaru secrets show <name>` — display secret metadata (and optionally values)
- Implement `kitaru secrets list` — list all accessible secrets
- Implement `kitaru secrets delete <name>` — delete a secret
- Secrets are **private by default** (only the creating user can access)
- Under the hood, wrap ZenML's `Client().create_secret()` / `get_secret()` /
  `update_secret()` / `list_secrets()` / `delete_secret()`
- Secret keys should use actual env var names (e.g. `OPENAI_API_KEY`) so that
  ZenML's env injection and LiteLLM's env var reading work seamlessly
- No new server functionality needed — the Kitaru server is the ZenML server

**Spec references:** [04-connection-stacks-and-configuration.md] (secrets model),
[14-cli-reference.md] (secrets CLI)

**Estimated size:** Small-medium. Thin wrapper over existing ZenML secret CRUD.

---

## Phase 12: `kitaru.llm()` --- DONE

**Goal:** Tracked LLM calls using LiteLLM as the backend engine, with a local model registry for aliases and ZenML secrets for remote credentials.

**What to do:**
- Implement `kitaru.llm(prompt, model=None, system=None, temperature=None, max_tokens=None, name=None)`
- Two modes based on context:
  - Inside flow (outside checkpoint): creates a synthetic durable call boundary
  - Inside checkpoint: creates a child event (tracked but not a replay boundary)
- Backend: wrap `litellm.completion()` as the sole provider engine
- Auto-create artifacts: prompt artifact + response artifact
- Auto-log metadata via `kitaru.log()`: token usage, cost, latency, resolved model
- Model resolution: alias handling (`fast`, `smart`) via local model registry, or
  concrete LiteLLM identifiers (`openai/gpt-4o`, `anthropic/claude-sonnet-4-20250514`)
- Implement local model registry:
  - CLI: `kitaru model register <alias> --model <litellm_model_id> [--secret <secret_name>]`
  - CLI: `kitaru model list`
  - Storage: local user config (e.g. `~/.config/kitaru/models.json`)
  - Aliases with optional ZenML secret references, independent of stacks
- Credential resolution order:
  1. Process env vars already set → use them (LiteLLM reads natively)
  2. Alias has a `secret` field → fetch ZenML secret, inject as env vars, call LiteLLM
  3. No credentials found → fail with clear error
- Zero-config path: provider env vars (`OPENAI_API_KEY`, etc.) work without
  registration because LiteLLM reads them natively
- Always record the resolved concrete model as metadata for provenance

**Spec references:** [08-kitaru-llm.md] (full contract),
[14-cli-reference.md] (model registration CLI)

**Note:** This phase depends on Phase 11.5 (secrets surface) for the remote
credential path. LiteLLM + local registry + ZenML secrets is the full implementation.

**Estimated size:** Medium. The LiteLLM wrapping is straightforward; the local
registry, alias resolution, and credential fetching are the main work.

---

## Phase 13: Error handling and failure journaling --- DONE

**Goal:** Clean error behavior across the SDK.

**What to do:**
- Define Kitaru exception hierarchy
- Implement failure journaling: failed checkpoint attempts recorded with their errors
- Checkpoint retry behavior: record failed attempts, succeed on retry, expose attempt history
- Clear error messages for:
  - Calling checkpoint outside a flow
  - Calling save/load/log outside proper context
  - Wait validation failures (invalid input leaves execution in `waiting`)
  - Divergence during replay
- Runtime vs user-code error distinction
- **Divergence detection** is implemented in the ZenML backend — Kitaru surfaces
  the user-visible error and documents the contract, but does not independently
  implement the detection logic

**Spec references:** [12-error-handling.md] (full error model),
[02-execution-model.md] (durable outcomes include failures, divergence detection)

**Estimated size:** Medium. Mostly about defining clear boundaries and good messages.

---

## Phase 14: CLI commands (aligned with spec tiers) --- PARTIALLY DONE

**Goal:** CLI commands organized by priority tier.

**Implemented in this phase:**

- `kitaru run agent.py:content_pipeline --args '{"topic":"AI safety"}' [--stack prod]`
- `kitaru executions get <exec_id>`
- `kitaru executions list [--status waiting] [--flow content_pipeline] [--limit 20]`
- `kitaru executions retry <exec_id>`
- `kitaru executions cancel <exec_id>`

**Deferred to later phases:**

- `kitaru executions replay <exec_id> --from <call_name> [--input key=val] [--override ...]`
  - depends on replay machinery (Phase 16)
- `kitaru executions logs <exec_id> [--follow]`
  - depends on a backend-agnostic execution log retrieval API

**Tier 3 — stack authoring, artifacts, config (later):**
- `kitaru stack create ...` (see Phase 18)
- `kitaru log-store set/show/reset` (see Phase 7b)
- `kitaru artifacts list <exec_id>` / `kitaru artifacts get <artifact_id> [--download]`
- `kitaru config show`

All commands wrap SDK / `KitaruClient` methods.

**Spec references:** [14-cli-reference.md] (CLI tiers and command reference)

**Estimated size:** Medium. Straightforward CLI wrapping of client API, but
many commands to wire up.

---

## Phase 15: `kitaru.wait()` + resume --- DONE

**Goal:** Durable suspension and resume. **Wraps ZenML `feature/pause-pipeline-runs` branch.**

**Branch status (March 2026):** `zenml.wait(...)` works and pauses an in-progress
run. Resume works but has two paths:
- **Pro servers with snapshots:** run auto-resumes when the wait condition is resolved
- **Non-Pro / local:** users must manually resume via a ZenML CLI command (already exists on the branch)
- Wait resolution is currently **human input only** (no automated triggers yet)

**What to do:**
- Implement `kitaru.wait(schema=..., name=..., question=..., timeout=..., metadata=...)`
- Flow-only enforcement (not valid inside checkpoints)
- Waiting execution model:
  - Record pending wait info (name, question, schema)
  - Suspend the current execution
  - On resume: validate input against schema, return it
  - Invalid input fails validation and leaves the execution in `waiting`
- Resume happens via a single mechanism surfaced through multiple clients:
  dashboard, CLI, API, Python SDK — all go through `client.executions.input(...)`
- **Handle the two resume paths:** on Pro servers resume is automatic; on non-Pro /
  local, Kitaru must surface a manual resume command (wrapping the existing ZenML
  CLI command) so users don't need to interact with ZenML directly
- Wait timeout means **resource-release timeout**, not expiration — the execution
  stays waiting even after timeout, but compute may be released
- Wrap ZenML's wait/resume implementation, don't reimplement

**Spec references:** [07-kitaru-wait.md] (full contract and lifecycle),
[19-implementation-guide.md] (branch capabilities)

**Estimated size:** Large. This is the hardest primitive. Wrap ZenML's implementation,
don't reimplement.

---

## Phase 16: Replay and overrides

**Goal:** Create new executions derived from previous ones. **Partially blocked by ZenML branch.**

**Branch status (March 2026):**
- **Replay:** Kitaru replay API/CLI wrappers remain stubbed and need implementation
- **Retry:** implemented in Kitaru (`client.executions.retry(...)`, `kitaru executions retry`)
- **Resume/input:** implemented in Kitaru via `kitaru.wait()`, `client.executions.input(...)`, and `client.executions.resume(...)`

**What to do:**
- Client methods:
  - `client.executions.replay(exec_id, from_=..., **flow_inputs, overrides=...)`
  - Flow inputs are passed **directly as keyword arguments** (not via `flow.input.*` prefix)
  - Overrides target checkpoint outcomes and wait inputs:
    `overrides={"checkpoint.research": "Edited notes", "wait.approve": False}`
- Flow-object replay: `my_flow.replay(exec_id=..., from_=..., topic="New topic")`
- CLI commands:
  - `kitaru executions replay <exec_id> --from <call_name> [--input key=val] [--override ...]`
  - `kitaru executions cancel <exec_id>`
  - Retry is already available (`kitaru executions retry <exec_id>`) via Phase 14
- Replay-point resolution to durable call instance IDs
- **Divergence detection** is ZenML-backed — Kitaru surfaces clear errors when the
  call sequence doesn't match, but does not independently implement the detection
- Map onto ZenML's replay machinery from `feature/pause-pipeline-runs`

**Spec references:** [10-replay-and-overrides.md] (replay semantics, direct flow input kwargs),
[11-per-flow-and-per-checkpoint-overrides.md] (override hierarchy),
[13-client-api.md] (replay API examples),
[18-appendix-glossary.md] (replay, retry, resume, divergence definitions)

**Estimated size:** Large. Complex interaction with ZenML replay machinery.

---

## Phase 17: PydanticAI adapter

**Goal:** Wrap PydanticAI agents so model/tool calls become tracked child events,
with optional HITL via adapter tools.

**What to do:**
- Implement `kitaru.adapters.pydantic_ai.wrap(agent)` in `src/kitaru/adapters/pydantic_ai.py`
- **Child event capture:** when a wrapped agent runs inside a checkpoint:
  - Each model request becomes a child event (`type='llm_call'`)
  - Each tool call becomes a child event (`type='tool_call'`)
  - The enclosing checkpoint remains the replay boundary
- **Human-in-the-loop via adapter tools:** adapters can provide tools that
  trigger a **flow-level `wait()`** when invoked by the agent:
  - The tool does not call `wait()` inside a checkpoint — it signals the runtime
    to suspend at the flow level
  - The agent's reasoning loop pauses, the execution suspends, and the human
    provides input through the dashboard, CLI, or API
  - On replay, the recorded wait input replays like any other `wait()` call
  - This pattern is optional — flows can still use explicit `kitaru.wait()` directly
- Adapter must NOT bypass Kitaru restrictions (no nested checkpoints, no
  checkpoint-internal waits)
- Capture prompt/response artifacts and usage metadata per child event
- Add `pydantic-ai` as an optional dependency

**Spec references:** [16-framework-adapters.md] (adapter philosophy, PydanticAI
contract, and HITL via adapter tools),
[03-mvp-scope-and-platform-direction.md] (adapter is marketing-critical)

**Estimated size:** Medium. Child event capture is straightforward if core
primitives work. The HITL tool-to-wait translation adds complexity.

**⚠ Before starting this phase:**
1. **Clone Hamza's branch** with existing PydanticAI + ZenML durable execution
   code: https://github.com/htahir1/pydantic-ai/tree/feat/zenml-durable-execution-v2
   — this has reusable adapter code that should serve as the starting point.
   Make the cloned repo available in RepoPrompt as a workspace so the full
   context is accessible during implementation.
2. **HITL ergonomics are not covered** in Hamza's branch — that work still needs
   to be designed and built from scratch as part of this phase.

---

## Phase 18: Stack creation and sandbox

**Goal:** Users can create stacks with infrastructure details. Sandbox component
for isolated agent execution.

**What to do:**
- SDK: stack creation API
- CLI: `kitaru stack create <name> --runner ... --artifact-store ... --container-registry ...`
- This is NOT a thin ZenML wrapper — it needs a higher-level UX:
  - Map user-friendly flags to ZenML flavors + components + service connectors
  - Assemble the stack from those components
  - Expose infrastructure details (cluster credentials, namespace, resource limits)
    as part of stack creation, not separate prerequisite steps
- Support deploy-time default stack configuration (Helm chart values):
  - Default artifact store bucket, runner, container registry
  - Users should not need to manually create a stack before their first remote flow
- **Sandbox stack component:**
  - Provides isolated compute for agent execution (important for coding agents)
  - Resource limits and safe code execution for tool calls
  - Explicit MVP deliverable per spec

**Spec references:** [04-connection-stacks-and-configuration.md] (stack creation,
deploy-time defaults, capability checks),
[19-implementation-guide.md] (sandbox as MVP deliverable)

**Estimated size:** Large. Significant UX design work on top of ZenML's stack model.

---

## Phase 19: Agent-native integrations

**Goal:** MCP server and Claude Code skill for AI-assisted Kitaru development.

**What to do:**
- Add `kitaru[mcp]` optional extra in `pyproject.toml`
- Create `src/kitaru/mcp/` with MCP tools wrapping `KitaruClient` and CLI:
  - MVP tool set: executions list/get/latest/run/cancel/input/retry/replay,
    artifacts list/get, status, stacks list
  - No MCP resources in MVP
- Register `kitaru-mcp` console script
- Lazy import guard for MCP dependencies
- Create `src/kitaru/skills/__init__.py` and `src/kitaru/skills/kitaru-authoring.md`
  (Claude Code skill)
- Include skill as package data
- CI matrix: test both base install and `[mcp]` extra
- Tests: `tests/mcp/` for MCP tool tests, skill existence/content tests
- Docs pages for MCP server and Claude Code skill

**Spec references:** [20-agent-native-integrations.md] (full MCP + skill spec,
MVP tool set, packaging requirements)

**Estimated size:** Medium. Depends on stable `KitaruClient` and CLI.

---

## Phase 20: End-to-end examples, docs, and polish

**Goal:** Working examples that demonstrate the full lifecycle using current API vocabulary.

**What to do:**
- Research agent example (multi-checkpoint, artifacts, metadata)
- Agent with human-in-the-loop (wait + resume, including adapter HITL pattern)
- Agent with LLM calls (kitaru.llm() or PydanticAI adapter)
- Replay/override demo using **direct flow input kwargs** (not old `flow.input.*` syntax)
- Concurrent checkpoints demo
- `.deploy()` example showing remote execution on a named stack
- Update docs site with SDK reference
- Final packaging review:
  - `kitaru` PyPI package
  - `kitaru` Docker image (ZenML base + cloud plugins + bundled dashboard)
  - `kitaru-ui` bundled into both PyPI package and Docker image
  - MCP/skill docs
  - Sandbox component docs
  - Deploy-time default stack config (Helm chart values)

**Spec references:** [17-end-to-end-examples.md] (all example patterns),
[19-implementation-guide.md] (deliverables and packaging)

**Estimated size:** Medium. Integration work, not new primitives.

---

## Visual overview

```
Phase 1  -- Naming + skeleton ---------------------------------------- DONE
Phase 2  -- Login/logout/status CLI ----------------------------------- DONE
Phase 3  -- @kitaru.flow (incl .deploy()) ----------------------------- DONE
Phase 4  -- @kitaru.checkpoint ---------------------------------------- DONE
Phase 5  -- First working example ------------------------------------- DONE (Milestone)
Phase 6  -- Runtime context ------------------------------------------- DONE
Phase 7  -- kitaru.log() ---------------------------------------------- DONE
Phase 7b -- Global log store ------------------------------------------ DONE
Phase 8  -- kitaru.save() / kitaru.load() ----------------------------- DONE
Phase 9  -- Stack selection ------------------------------------------- DONE
Phase 10 -- Configuration --------------------------------------------- DONE
Phase 11  -- KitaruClient (execution mgmt first) ---------------------- DONE
Phase 11.5-- Secrets surface (wraps ZenML secrets) -------------------- DONE
Phase 12 -- kitaru.llm() (LiteLLM + registry + secrets) --------------- DONE
Phase 13 -- Error handling --------------------------------------------- DONE
Phase 14 -- CLI commands (tiered) ------------------------------------- DONE
Phase 15 -- kitaru.wait() + resume ------------------------------------ DONE
Phase 16 -- Replay + overrides (direct kwargs) ------------------------ PARTIALLY BLOCKED (replay wrappers pending)
Phase 17 -- PydanticAI adapter (incl HITL tools) ---------------------- Medium
Phase 18 -- Stack creation + sandbox ---------------------------------- Large
Phase 19 -- Agent-native integrations (MCP + skill) ------------------- Medium
Phase 20 -- Examples, docs, polish ------------------------------------ Final
```

## Suggested milestones

| Milestone | After phase | What you can show |
|---|---|---|
| "It runs" | 5 | A flow with checkpoints executes and returns a result |
| "It's useful" | 8 | Metadata, artifacts, log store, and structured logging work |
| "It's connected" | 11 | Client can manage and inspect executions programmatically |
| "It's smart" | 12 | Secrets + LLM calls are tracked with cost/token metadata |
| "It's durable" | 16 | Wait, resume, replay with direct kwargs, and retry all work |
| "It's complete" | 20 | Full SDK with adapters, CLI, MCP, examples, and docs |

## What to do when you're blocked

**Updated March 2026:** The ZenML `feature/pause-pipeline-runs` branch now has
working wait/resume support and Kitaru Phase 15 is complete. Phase 16 is
**partially blocked** in Kitaru because replay wrappers are not implemented yet.

### Current blocker status

| Phase | Status | What's blocked |
|---|---|---|
| Phase 15 (`kitaru.wait()`) | **Done** | `kitaru.wait(...)`, `client.executions.input(...)`, and manual `executions resume` wrappers are implemented |
| Phase 16 (replay) | **Partially blocked** | Replay API/CLI wrappers are still stubbed in Kitaru |
| Phase 16 (retry) | **Unblocked in Kitaru** | `client.executions.retry(...)` and CLI retry are implemented |

### What to do now

1. **Phase 15 is complete.** `kitaru.wait(...)`, wait input, and manual resume wrappers are now in Kitaru.
2. **Keep `client.executions.retry(...)` as-is** and validate against live backends as replay work lands.
3. **Replay** should be implemented once the replay machinery can be wrapped safely in Kitaru.
4. Phase 11.5 (secrets) wraps ZenML's existing secret store — no upstream dependency.
5. Phase 12 (`kitaru.llm()`) uses LiteLLM + a local model registry + ZenML secrets
   for remote credentials — depends on Phase 11.5 but has no upstream ZenML branch dependency.

## How to use this plan

- **Pick up the next incomplete phase.** Do it. Commit. Move on.
- **Each phase should be its own PR** (or a small set of commits).
- **Write tests as you go** — each phase should include tests for what it builds.
- **Run `just check` and `just test` before every commit.**
- **When you finish a phase, check it off** and move to the next.
- **If something feels too big**, break it into sub-tasks. That's fine.
- **If you get stuck**, re-read the referenced spec docs. They're detailed.
