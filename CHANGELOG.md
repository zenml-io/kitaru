# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Added a LangGraph/LangChain sandbox command tool via `create_sandbox_command_tool(...)`, plus a sandbox strategy in the LangGraph example, so agents can run shell commands through the active Kitaru stack sandbox. (#420)
- Added count-based execution statistics through `KitaruClient().executions.statistics(...)`, `kitaru executions statistics`, and the `kitaru_executions_statistics` MCP tool, with grouping by status, flow, stack, tag, time bucket, and execution metadata.

### Fixed
- Fixed the LangGraph sandbox demo so its real-model run deterministically executes the documented demo command, and redacted static sandbox tool env values from wrapped provider error messages.
- Fixed a LangGraph adapter crash when running graphs without a LangGraph checkpointer under Kitaru's default durability policy. (#403)

## [0.15.0] - 2026-06-04

### Added
- Added the experimental Gemini Interactions adapter (`kitaru.adapters.gemini`) with an Interactions-first design: one stable Gemini response maps to one Kitaru checkpoint, non-stable background statuses fail instead of being cached as success, raw provider payload capture is opt-in by default, and the public API includes an Antigravity managed-agent preset plus explicit `cache_identity` disambiguation while keeping Google-owned sandbox/tool internals outside Kitaru's replay promise.
- Added OpenAI Agents live streaming via `KitaruRunner.run_stream(...)` and `run_stream_sync(...)`, publishing best-effort `openai_agents.stream.*` events while preserving `OpenAIRunResult` as the durable checkpoint output, plus a provider-key-gated runnable example at `examples/integrations/openai_agents_agent/openai_agents_streaming.py`.
- Added LangGraph graph-call streaming via `KitaruGraphRunner.stream(...)` and `astream(...)`, publishing best-effort `langgraph.stream.*` events while preserving `LangGraphRunResult` as the durable checkpoint output, plus a provider-neutral runnable example at `examples/integrations/langgraph_agent/langgraph_streaming.py`.
- Added Claude Agent SDK live streaming via `KitaruClaudeRunner.run_stream(...)` and `run_stream_sync(...)`, publishing best-effort `claude_agent_sdk.stream.*` events while preserving `ClaudeRunResult` as the durable checkpoint output.
- Added Gemini Interactions streaming surfaces (`run_stream(...)` and `run_stream_sync(...)`) plus exported stream event constants, conservative live-event privacy defaults, streamed poll support when the installed Google SDK exposes it, and example/smoke coverage for the no-network streaming path.
- Updated the Gemini Interactions example so `--stream` prints clipped Gemini text chunks by default for manual testing, with `--hide-text-deltas` available when event-label-only output is preferred; SDK live-event text remains hidden unless `GeminiInteractionCapturePolicy(include_stream_text_deltas=True)` is set.
- Added PydanticAI live stream events for `KitaruAgent` runs: the adapter now emits `pydantic_ai.stream.started`, `.event`, `.completed`, and `.failed` updates for watched PydanticAI streams, plus a provider-key-gated runnable example at `examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py`.
- Added SDK event watching: `KitaruClient().executions.events(...)` now consumes server-backed live execution events with kind/checkpoint/correlation filters, SSE cursor reconnects, and clear feature-unavailable errors for local database mode or disabled server streaming.
- Added checkpoint live-event publishing: `kitaru.progress(...)` and `kitaru.events.publish(...)` can now emit best-effort progress/custom events from inside running checkpoints, and Kitaru automatically publishes checkpoint started/completed/failed lifecycle events when the checkpoint body executes.

### Changed
- Moved adapter docs into a first-class Adapters docs section, with redirects from the old guide URLs to the new canonical adapter pages.

### Fixed
- Fixed PydanticAI adapter compatibility with newer Pydantic AI releases that forward `retries=` from `run_sync()` into `run()`, while preserving Kitaru's legacy `output_retries=` keyword.

## [0.14.0] - 2026-06-02

### Added
- Added PydanticAI `checkpoint_strategy="calls" | "turn"` as the preferred public spelling for adapter checkpoint placement. `"calls"` remains the default and maps to the existing per-model/tool/MCP checkpoint behavior; `"turn"` maps to the existing one-checkpoint-per-agent-run behavior. Existing `granular_checkpoints=True | False` code remains supported. (#374)
- Added a durable chatbot example at `examples/chatbot/` that models an entire conversation as a single PydanticAI agent with one human-in-the-loop tool, using `kitaru.wait()` to release compute between turns so a session can sleep for minutes or days and resume exactly where it left off. (#376)

### Changed
- Standardized adapter docs and examples around the shared `checkpoint_strategy` concept while keeping framework-specific boundary names such as PydanticAI `"turn"`, OpenAI Agents `"runner_call"`, LangGraph `"graph_call"`, and Claude Agent SDK `"invocation"`. (#374)
- Bumped the minimum ZenML dependency, server image, and Helm subchart versions to `0.94.6` so Kitaru tracks the latest upstream ZenML release. (#382)

## [0.13.1] - 2026-05-21

### Added
- **Agent Harness Platform** — a chapter-by-chapter flagship example at `examples/end_to_end/agent_harness_platform/` and a dedicated docs section at [`/docs/agent-harness-platform/`](https://kitaru.ai/docs/agent-harness-platform/). A platform-engineer's starter kit for building internal agent platforms on Kitaru + PydanticAI: six runnable stages take a 30-line durable agent → `DockerSandbox` → skills as markdown → credential proxy with mitmproxy + auth injection → typed-union `exec_service` dispatcher → HITL via `kitaru.wait()`. Includes a per-stage `Profile` gating model, an `agent_harness_platform/` library, mocks + Dockerfiles, and layer-A smoke tests in `tests/test_agent_harness_platform_example.py`. (#288)

### Changed
- `docs/content/docs/getting-started/examples.mdx` reorganized into three categories — Agent Harness Platform tour / Other end-to-end / Feature-focused. The previous goal-keyed table is replaced. (#288)
- `docs/content/docs/guides/news-scout.mdx` removed; the `news_scout` example itself stays runnable in the repo and is now listed under "Other end-to-end examples" on the docs site. The guides section is reserved for Kitaru-feature how-tos. (#288)

### Fixed
- Fixed PydanticAI adapter compatibility with `pydantic-ai-slim>=1.95`, where upstream renamed built-in tools to native tools. The adapter no longer fails at import time on `AgentBuiltinTool` or crashes by forwarding `builtin_tools=None` into PydanticAI's deprecation shim. (#370)

## [0.13.0] - 2026-05-20

### Added
- Added OpenAI Agents adapter context passthrough: `KitaruRunner.run(...)` and `run_sync(...)` now accept a `context=` argument that is forwarded to the OpenAI Agents SDK and included in runner/tool checkpoint cache keys, with an explicit `context_cache_identity=` projection hook for stable production contexts. Context-derived cache identity also covers tool calls resumed from interrupted `RunState` so approved tools after a HITL resume cannot reuse stale same-args/different-context cache entries. (#345)
- Added OpenAI Agents tool-input guardrail observability in `checkpoint_strategy="calls"`: model-requested tool calls a guardrail blocks before the tool body runs are now recorded as `tool_call` events with guardrail metadata, without creating a tool checkpoint or persisting rejected arguments. `OpenAICapturePolicy.save_input=False` redacts guardrail rejection text and unexpected exception details, and `save_interruption_payloads=False` omits raw interruption argument previews. (#345)
- Built wheels now include the `kitaru/py.typed` PEP 561 marker so downstream type checkers pick up Kitaru's public type information. (#343)

### Changed
- Bumped the minimum ZenML dependency, server image, and Helm subchart versions to `0.94.4` so Kitaru tracks the latest upstream ZenML release. (#344)
- `kitaru logout` now resets persisted store state and clears credentials before attempting best-effort local-daemon shutdown, so a failure to stop the daemon no longer leaves the CLI pointed at a broken remote connection. (#343)
- `kitaru secrets list` now uses a stable backend page size before applying CLI pagination, producing deterministic ordering across runs. (#343)

### Fixed
- Fixed adapter-created granular checkpoints being treated as flow-return candidates, so `flow.run(...).wait()` / `.get()` can return the user's final checkpoint result when adapters also produced model/tool checkpoints. (#355)
- Fixed Kitaru-owned request constructors to reject checkpoint output handles with guidance to call `.load()` instead of surfacing generic Pydantic string validation errors. (#353)
- Fixed PydanticAI direct sync tool-body `kp.wait_for_input(...)` calls under ZenML `0.94.4` with explicit `allow_sync_tool_body_waits=True` opt-in, keeping `tool_checkpoint_config_by_name={"tool": False}` as checkpoint-only configuration. (#351)
- Fixed Kitaru flow return compatibility with ZenML `0.94.4` dynamic-pipeline output validation by persisting plain flow returns as internal artifacts while preserving user-facing Python return values and avoiding marker-shaped user dictionaries being mistaken for hidden tuple metadata. (#344)
- Fixed adapter result identity after checkpoint load for OpenAI Agents, Claude Agent SDK, and LangGraph runners: results restored from a synthetic checkpoint are now rebuilt as the canonical local result class, so `isinstance(result, OpenAIRunResult)` (and the Claude/LangGraph equivalents) no longer fails when the loaded payload originally came from an alternate import path. (#354)
- Replaced local-server cleanup's PID-only `SIGKILL` fallback with a "warn and continue" path so a recycled PID cannot cause Kitaru to kill an unrelated process during `kitaru clean global/all`. Inspection failures now surface as `unknown (inspection failed: ...)` instead of being silently treated as "no local server". (#343)
- Restored the caller process environment exactly after `kitaru login` startup attempts, even when local-daemon deployment or connection fails partway through. (#343)
- Removed stale references to the deprecated native memory surface from the docs site, agent-native guides, and comparison pages. (#342)

## [0.12.0] - 2026-05-17

### Added
- Added LangGraph `checkpoint_strategy="calls"` support via `KitaruLangGraphMiddleware`, creating true sync LangChain model/tool call checkpoints while keeping `graph_call` as the default coarse mode. The guide now explicitly documents that callbacks/event streams are trace-only, LangGraph checkpointers remain LangGraph-owned, and async calls mode is metadata-only.
- Added a local LangGraph adapter example (`examples/integrations/langgraph_agent/`) plus a new LangGraph adapter guide (`/adapters/langgraph/`) covering the adapter boundary: Kitaru owns graph-call or middleware-wrapped call checkpoints, LangGraph owns thread/checkpointer semantics, and Deep Agents filesystem/sandbox behavior remains pass-through. Updated the examples indexes and smoke test to include deterministic LangGraph examples with no API keys required.
- Claude Agent SDK adapter (`kitaru.adapters.claude_agent_sdk`) for invocation-level durability: wrap a Claude SDK query in one Kitaru checkpoint, capture the session ID, final result, usage/cost, messages/transcript artifacts when available, and a redacted run manifest. Includes a guide, integration example, and smoke-test coverage while explicitly documenting that Claude-internal Bash, MCP, custom tool, and workspace side effects are not granular replay boundaries.
- Added `kitaru.current_execution_id()` as the public way to read the active Kitaru execution ID inside a running flow or checkpoint.

### Fixed
- LangGraph adapter event logs and run summaries are now saved as real role-first Kitaru context artifacts inside checkpoint scope, with best-effort event persistence by default and hardened config/context redaction for unusual values.
- PydanticAI granular checkpoints now store model messages and tool arguments as structural checkpoint inputs and use the returned checkpoint output as the canonical response/result artifact, avoiding duplicate manual artifacts in new runs.
- OpenAI Agents `checkpoint_strategy="calls"` now stores model inputs and function-tool arguments as structural checkpoint inputs, and adapter-generated artifact names now put the human-readable role first across PydanticAI, OpenAI Agents, and Claude Agent SDK captures.

## [0.11.0] - 2026-05-12

### Fixed
- PydanticAI adapter run surfaces now accept and forward upstream `conversation_id` and `output_retries` kwargs and include them in turn-checkpoint cache keys, while temporarily capping `pydantic-ai-slim` to the supported 1.89–1.92 line.
- Fixed PydanticAI MCP tool calls hanging after a successful request when an explicitly lifecycle-managed MCP server was already open. Kitaru now keeps already-running MCP calls on the active event loop inside explicit flows, and fails fast when auto-flow would otherwise move a pre-opened MCP server across event loops; auto-connected MCP servers still use granular MCP checkpoints by default.
- Added a compatibility shim for the `pydantic_ai.mcp` import path used by current PydanticAI releases when Kitaru is installed with the MCP SDK version still compatible with ZenML server dependencies.

### Removed
- Removed the native memory surface from Kitaru: `kitaru.memory`, `KitaruClient.memories`, the `kitaru memory` CLI group, MCP `kitaru_memory_*` tools, and the corresponding memory docs/examples. Use your own storage for durable application state and pass values into flows explicitly.

### Security
- Bumped transitive dependencies flagged by `pip-audit`: `gitpython` 3.1.47 → 3.1.49 (CVE-2026-44244), `mako` 1.3.11 → 1.3.12 (CVE-2026-44307), `python-multipart` 0.0.26 → 0.0.27 (CVE-2026-42561), and `urllib3` 2.6.3 → 2.7.0. Lockfile-only change; no API surface affected.

## [0.10.0] - 2026-05-08

### Changed
- `kitaru.wait()` and adapter wait paths are flow-scope only. Waits created from checkpoint-contained tool bodies must move to flow scope, or those waiting tools must be opted out of granular tool checkpoints. (#280)
- `KitaruAgent` now defaults to `granular_checkpoints=True`, so model, tool, and MCP calls are persisted as separate adapter checkpoints by default. Pass `granular_checkpoints=False` to keep the previous one-checkpoint-per-agent-run turn mode. (#280)
- PydanticAI adapter checkpoint configs now accept `cache`, and granular model checkpoint cache keys ignore PydanticAI-generated per-run message metadata so identical logical prompts can cache across runs. (#280)
- Streamlined the `openai_research_bot` end-to-end example for readability, and refreshed the OpenAI Agents adapter guide and examples index to match. (#308)

### Fixed
- PydanticAI flow-scope trackers now allocate unique artifact namespaces to avoid cross-run artifact-name collisions. (#280)
- Cached granular PydanticAI model responses now preserve model event/tool-call ordering for parallel tool calls. (#280)
- OpenAI Agents adapter parallel tool-call events now keep assistant-emitted order in event logs and summaries, even when tools start or finish out of order. (#280)
- PydanticAI adapter parallel tool-call events now keep assistant-emitted order in event logs, summaries, and fan-in metadata, even when tools start or finish out of order. (#280)
- PydanticAI adapter observability artifact names now use shorter event-local suffixes inside readable tracker namespaces, avoiding collisions across flow-scope and checkpoint-scope trackers. (#280)

## [0.9.0] - 2026-05-05

### Added
- OpenAI Agents SDK adapter (`kitaru.adapters.openai_agents`) — wrap an `Agent`/`Runner` with `KitaruRunner` to make OpenAI Agents SDK runs durable, replayable, and observable under a Kitaru flow. Supports two tracking strategies via `checkpoint_strategy="runner_call"` (one checkpoint per `Runner.run`, recommended when you want a clean `.wait()` return value) or `checkpoint_strategy="calls"` (per-tool/per-model checkpoints for finer replay units, with per-checkpoint artifacts visible in the Kitaru UI / `KitaruClient`). The guide at `/adapters/openai-agents/` walks through the trade-offs. (#295)
- OpenAI Agents integration example (`examples/integrations/openai_agents_agent/`) and an end-to-end `openai_research_bot` example (planner/writer runner checkpoints, submitted search fan-out, and final report artifacts, with remote secret guidance and Kitaru UI artifacts). Both are exercised by the smoke test. (#295)
- Markdown exports for every docs page at `kitaru.ai/docs/<slug>.md`, plus a substantially expanded `/llms.txt` index — making the docs friendlier for LLMs and agents that consume them programmatically. (#303)

### Changed
- `flow.run(...).wait()` now raises a new dedicated `KitaruAmbiguousFlowResultError` (subclass of `KitaruRuntimeError`) when the flow has multiple terminal checkpoints with no single sink (common with the OpenAI Agents adapter's `checkpoint_strategy="calls"`). The error names the terminal checkpoints, points at the execution's artifacts in the Kitaru UI, and suggests `KitaruClient` retrieval and the `runner_call` strategy as alternatives. Catching this specific subclass lets callers handle the ambiguity case without accidentally swallowing real execution failures.

## [0.8.0] - 2026-05-04

### Added
- OSS-first auth management for service accounts and API keys via `KitaruClient.auth`, `kitaru auth service-accounts`, and `kitaru auth api-keys`. Raw API-key values are only returned on create/rotate so they can be stored immediately; list/show/update responses stay metadata-only. (#230)

### Changed
- Pydantic AI adapter now supports `pydantic-ai-slim>=1.86.0,<2`: per-run `capabilities` and `spec` are forwarded to Pydantic AI and included in turn-checkpoint cache keys to avoid stale cached turns. (#270)
- `examples/` is reorganized into `features/`, `integrations/`, and `end_to_end/` subdirectories. Existing example paths (e.g. `examples/basic_flow/...`) move under one of these categories — update any pinned references. (#242)

### Fixed
- Checkpoint output handles now display Kitaru guidance to call `.load()` instead of leaking raw ZenML artifact metadata when stringified in flow bodies. (#252)
- `kitaru executions replay` now resolves project-local modules correctly when invoked from a project directory, instead of falling back to the CLI bootstrap module via `__main__` and producing a misleading replay. (#218)
- Runtime log retrieval (`KitaruClient.executions.logs(...)`, `kitaru executions logs`) now tolerates server/client version skew on log payload schemas instead of erroring out. (#251)
- Active-stack resolution no longer silently falls back to a deleted or unavailable stack — flow submission, MCP, and `kitaru status` now surface a clear error when the configured active stack is gone. (#263)
- `KitaruAgent` auto-checkpointing of agents that use `@hitl_tool(schema=...)` no longer crashes with `PydanticSerializationError: Unable to serialize unknown type: <class 'type'>` under `pydantic-ai-slim>=1.86`, which now surfaces per-tool metadata through the `AgentRunResult` tree. (#292)

## [0.7.0] - 2026-04-24

### Added
- `kitaru build --image`, `kitaru deploy --image`, and MCP `kitaru_deployments_deploy(image=...)` now accept deploy-time image configuration (base image string or `ImageSettings`-style object), so saved deployment snapshots can carry remote-only package installs and secret-backed environment injection. (#221)

### Changed
- **Breaking:** Replay planning now uses graph reachability from replay roots, so replaying from a branch leaf only re-executes that branch's downstream path. Checkpoint override semantics are aligned accordingly: `checkpoint.<selector>` injects into direct consumers, and replay roots include those consumers. Scripts relying on the previous ordering/index-based frontier may see different execution paths when replaying parallel branches. (#228)
- Bumped the minimum ZenML version to `0.94.3`, picking up upstream artifact-store path validation alongside compatibility fixes to Kitaru's materializers and tests. (#232)
- Clearer error when a stack references an integration whose dependencies are not installed — flow resolution now points users to the exact extra they need to install (e.g. `kitaru[k8s]`, `kitaru[vertex]`) instead of a low-level ZenML import error. (#227)

### Fixed
- `kitaru executions` URL logging now prints the correct dashboard URL for each execution. (#223)

## [0.6.0] - 2026-04-23

### Added
- `kitaru auth token` for printing a short-lived bearer token for the active Kitaru server, suitable for shell command substitution. (#210)
- `kitaru flow deployments curl FLOW` for generating a copy-pasteable curl command that starts a deployment execution through the active Kitaru server without inlining real tokens. (#210)
- CLI commands for building, deploying, invoking, listing, tagging, logging, and deleting snapshot-backed flow deployments. (#210)
- MCP deployment tools for deploying, invoking, listing, inspecting, deleting, tagging, and untagging snapshot-backed flow deployments. (#210)
- Deployment model docs covering auto-versioning, reserved/default tag routing, serverless invocation, active Kitaru server authentication, and producer/consumer examples. (#210)
- Python SDK secret write helpers: `kitaru.create_secret(...)` and `kitaru.delete_secret(...)`. (#206)
- MCP secret creation tool `kitaru_secrets_create` for metadata-only secret creation from MCP clients. (#206)
- `kitaru.adapters.pydantic_ai.wait_for_input(...)` helper for pausing a PydanticAI tool call until a human supplies input, with the wait recorded under the adapter's metadata. (#216)
- `news_scout` example and accompanying guide: an agentic news monitor that demonstrates granular checkpoints, durable shared memory, and replay across executions. (#191)
- `compliance_review` example: a multi-stage Claude Agents SDK workflow illustrating single-turn, multi-domain, memory-backed, and conversational patterns under Kitaru. (#161)

### Changed
- `kitaru secrets set` now creates public secrets by default. Pass `--private` to create a private secret. Updating an existing secret still only updates values and leaves existing visibility unchanged. (#206)
- `kitaru.wait(...)` can now be called from inside `@checkpoint` bodies (previously flow-level only). The enclosing checkpoint suspends for the duration of the wait; on resume, the checkpoint re-runs from the top. (#216)
- Reframed the concept docs around the "platform-builder" primitive: new `harness-runtime-platform` concept page, rewritten `how-it-works` / `flows` / `checkpoints` explainers, and removal of the now-redundant `execution-model` page. (#208)

## [0.5.1] - 2026-04-17

### Added
- `ImageSettings.secret_environment_from` field for attaching ZenML secret references to a flow execution; Kitaru forwards the list through `Pipeline.with_options(secrets=[...])` so secret values never enter `DockerSettings.environment`, image build metadata, logs, or the frozen execution spec (#188)
- `kitaru info --all` now includes active stack/project provenance, showing whether the effective context came from environment variables, repo-local `.kitaru/config.yaml`, or global config. The same structured fields are available through JSON output, exported diagnostics files, and MCP `kitaru_info(all=True)` (#186)
- `KitaruMemoryArtifactUnavailableError` typed exception (subclass of `KitaruBackendError`) for memory entries whose backing artifact cannot be loaded from the current runtime (#189)
- `strict=False` parameter on `kitaru.memory.get(...)`, CLI `kitaru memory get --strict`, and MCP `kitaru_memory_get(strict=...)`. Lenient mode warns and returns `None` (Python) or returns a payload with `value_available: False` and nested `value_unavailable` diagnostics (CLI/MCP); strict mode raises `KitaruMemoryArtifactUnavailableError` (#189)

### Changed
- `kitaru.memory.get(...)` no longer raises `KitaruBackendError` by default when a memory entry's artifact value is unreachable from the current stack (for example, dev→prod stack switches where old artifact URIs point at a local filesystem path). The new default is to warn and return `None` so flows can fall through to their existing missing-key handling. Callers that depended on exception-based signaling should pass `strict=True` (#189)

## [0.5.0] - 2026-04-17

### Breaking Changes
- `kitaru.adapters.pydantic_ai.wrap(...)` is deprecated in favor of `KitaruAgent(...)`. A compatibility shim remains for one release (#156)
- Legacy adapter capture config names were renamed: `"metadata_only"` -> `"metadata"` and `"off"` -> `None` (#156)
- Legacy `tool_capture_config_by_name={"name": {"mode": "metadata_only"}}` now maps to `capture=CapturePolicy(tool_capture_overrides={"name": "metadata"})` (#156)

Migration snippet:

```python
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent

wrapped = KitaruAgent(
    agent,
    capture=CapturePolicy(
        tool_capture="full",
        tool_capture_overrides={"name": "metadata"},
    ),
)
```

### Added
- `kitaru.get_secret()` and the public `Secret` model for exact, Kitaru-native secret reads in Python code without importing ZenML directly (#185)
- `@checkpoint(cache=...)` per-checkpoint cache overrides (`True`/`False`/`None`) with updated configuration docs (#184)
- `kitaru.adapters.pydantic_ai.wrap(...)` compatibility shim with deprecation warning to ease migration to `KitaruAgent(...)` (#156)
- Granular checkpoint mode now installs a run-level tracker at flow scope and persists `pydantic_ai_events` plus `pydantic_ai_run_summaries` even when no turn checkpoint is opened (#156)
- Restored end-to-end PydanticAI adapter integration coverage for turn mode, granular mode, and auto-flow execution (#156)

### Changed
- PydanticAI adapter auto-flow now re-enters the normal run path so turn checkpoints, tracking, and message-history capture apply outside explicit flows (#156)
- PydanticAI granular mode now defaults its per-call checkpoint configs on, rejects invalid config combinations eagerly, keeps HITL interception active when capture is disabled, and raises clear usage errors for unsupported deferred-tool schemas (#156)
- PydanticAI adapter docs, README examples, and migration guidance now match the shipped runtime: `runtime="inline"` only for adapter-managed checkpoints, explicit deprecation path for `wrap(...)`, and corrected capture-policy examples (#156)

### Fixed
- Execution-level cache no longer defaults to `True`, so `@checkpoint(cache=False)` is preserved through ZenML compilation when no flow-level cache is explicitly configured (#184)

## [0.4.1] - 2026-04-16

### Changed
- CLI list commands now default to paginated windows (`--page 1 --size 20`) for executions, memory, stacks, models, and secrets. `kitaru executions list` also shows compact `Started` and `Ended` columns, while JSON output keeps the existing `{command, items, count}` envelope shape. Paging past the end of a non-empty list now reports `no items on page N` across all five commands rather than a misleading "none found". `kitaru executions list --limit N` still works but no longer accepts any explicit `--page`/`--size`, so the two modes don't silently mix (#139)
- Clarified flow-body artifact loading semantics in the concepts and guides docs, including a dedicated section in the artifacts guide and tighter guidance in the `wait`/`input` and LLM-call pages (#143)
- Expanded the MCP setup docs with a venv/PATH caveat (the common failure mode where Claude Code inherits its launcher's PATH rather than a later-activated venv) and added `claude mcp add` with all three scope flags as an alternative to hand-editing `.mcp.json`

### Fixed
- Fixed SDK and CLI reference rendering in the generated docs, including docstring cleanups across `checkpoint`, `flow`, `logging`, `artifacts`, and `client` so the griffe/fumapy pipeline emits correctly formatted reference pages (#141)

## [0.4.0] - 2026-04-12

### Added
- **Durable agent memory** (`kitaru.memory`) — a new core primitive for durable, artifact-backed agent memory with typed scopes (`namespace` for cross-flow sharing, `flow` for per-flow state, `execution` for per-run state). Values persist through restarts, replays, and cross-execution workflows. Inside flows, reads and writes are captured via private non-cacheable synthetic steps so they remain replayable; outside flows, `kitaru.memory.configure(scope=..., scope_type=...)` unlocks the same API for seeding and inspection scripts (#82)
- **Memory compaction** — `kitaru memory compact`, `KitaruClient.memories.compact(...)`, and MCP `kitaru_memory_compact` summarize one or many memory values using an LLM and write the summary back as a new version. Supports single-key or multi-key compaction, current-value or full-history source modes, and records every operation in a per-scope audit log viewable via `kitaru memory compaction-log`
- **Memory purging** — `kitaru memory purge` deletes old versions of a single key while keeping the latest; `kitaru memory purge-scope` reclaims an entire scope (optionally including tombstoned keys) and records audit entries alongside compaction events. The internal compaction log is never itself purged
- Full `kitaru memory` CLI command group: `scopes`, `get`, `set`, `delete`, `history`, `purge`, `purge-scope`, `compact`, `compaction-log`, and `reindex`
- `KitaruClient.memories` typed namespace for `get/list/history/set/delete` plus maintenance operations (`purge`, `compact`, `reindex`) by explicit scope
- Nine MCP memory tools (`kitaru_memory_list/get/set/delete/history/purge/purge_scope/compact/compaction_log`) for agent-facing access from Claude, Cursor, and other MCP clients
- Automatic flow-membership indexing for new execution-scoped memory writes, plus `kitaru memory reindex` / `KitaruClient.memories.reindex(apply=...)` for dry-run-first backfilling of historical memory tags in existing projects
- Shared memory transport helpers (`kitaru._interface_memory`, `kitaru.inspection.serialize_memory_*`) so CLI, MCP, and SDK surfaces share one payload/validation layer
- Dedicated memory docs: concept page (`/concepts/memory`) and full guide (`/guides/memory`) covering typed scopes, in-flow vs outside-flow usage, durability semantics, and maintenance workflows
- Runnable memory example under `examples/features/memory/flow_with_memory.py` with narrated text output

### Changed
- `kitaru.memory.set/get/list/history/delete()` outside flows now require a configured scope via `kitaru.memory.configure(...)` and raise `KitaruStateError` with setup guidance when no scope has been configured. Inside flows, no configuration is needed — the execution scope is inferred automatically
- `memory.*` remains forbidden inside `@checkpoint` — the replay boundary is preserved by routing all memory operations through flow-scope synthetic steps
- Memory writes re-fetch the exact created artifact version by ID before returning typed metadata, so the client surface reports the concrete written version rather than guessing from "latest by name"

### Fixed
- Memory artifact version queries now use the correct `desc:version_number` sort order (was `version_number:desc`)

## [0.3.6] - 2026-04-11

### Added
- Copy-paste prompt examples in MCP server documentation for common workflows (status checks, flow execution, replay, artifact inspection)
- MCP extra mentioned earlier in the installation guide
- Troubleshooting guidance for MCP environment variable configuration

### Changed
- Improved anonymous telemetry metadata for opted-in users (richer flow lifecycle context, version stamping, deployment classification)

## [0.3.5] - 2026-04-11

### Added
- `kitaru analytics` CLI command group with `opt-in`, `opt-out`, and `status` subcommands for managing anonymous usage analytics preferences — persists to config file so the preference is respected by all surfaces including MCP servers

### Fixed
- Analytics events leaking from smoke test runs to Mixpanel (disabled via `ZENML_ANALYTICS_OPT_IN=false` export)
- MCP server ignoring user's analytics opt-out when launched via stdio transport (env vars stripped by MCP SDK; `kitaru analytics opt-out` persists preference to config file as the fix)
- `kitaru analytics` commands no longer eagerly bootstrap the ZenML store (added to `_DEFERRED_BOOTSTRAP_COMMANDS`)

## [0.3.4] - 2026-04-11

### Added
- `kitaru clean` command group with `project`, `global`, and `all` subcommands for resetting Kitaru state (with `--dry-run`, `--force`, `--yes` flags, auto-backup, model registry protection, and local server teardown)
- Enhanced `kitaru info` with new flags (`--all`, `--all-packages`, `--packages`, `--file`) and multi-section output including config provenance, connection source breakdown, system info, ZenML version, and package inventory
- `kitaru info --file` exports diagnostics to JSON or YAML (environment variable secrets are masked)
- Show actionable recovery hint (`kitaru executions retry <id>`) after flow failure in SDK errors and CLI follow-mode output (#120)

## [0.3.3] - 2026-04-08

### Added
- `ImageSettings` now supports `build_context_root`, `image_tag`, `target_repository`, and `user` fields for finer-grained container image configuration
- `ImageSettings.platform` field for specifying the target Docker build platform (e.g. `linux/amd64`)
- Anonymous usage analytics instrumentation across CLI, MCP, and SDK surfaces
- Pre-release smoke test script (`scripts/smoke-test.sh`) for end-to-end sanity checks

### Changed
- Replace runtime dashboard file patching with `ZENML_SERVER_DASHBOARD_FILES_PATH` environment variable, simplifying local server startup (#92)

### Fixed
- Suppress noisy config-change warnings that appeared during flow resume (#97)

## [0.3.2] - 2026-04-06

### Fixed
- Skip eager ZenML store bootstrap for commands that don't need a server connection (`--version`, `--help`, `login`, `logout`, `init`), preventing ~30 second startup delays when the stored config points to an unreachable server (#107)

### Changed
- Add Apple Silicon Docker guidance: `--platform linux/amd64` workaround for M-series Macs, troubleshooting for manifest mismatch errors, and startup timing notes (#106)
- Default Kitaru UI Docker build tag to latest release instead of requiring explicit version (#103)

## [0.3.1] - 2026-04-06

### Fixed
- Fix duplicate terminal handler accumulation after `importlib.reload()` by using marker-based detection instead of `isinstance` checks, preventing duplicated log output in long-running or reload-heavy environments

### Changed
- Bump minimum `pydantic-ai-slim` from `>=0.2.0` to `>=1.75.0` to align with upstream API changes (new method signatures, `tool_plain` decorator, `AgentSpec` support)
- Rewritten examples: realistic research-agent metaphor in basic flow, two-wait pattern (boolean gate + Pydantic schema) in wait/resume, parallel tool submission in coding agent, and consistent “Getting Started” READMEs across all example groups
- CLI command tracking now uses an allowlist of known multi-word commands to avoid leaking positional arguments (URLs, paths) into analytics
- Add PyPI classifiers and keywords for improved package discoverability

## [0.3.0] - 2026-03-24

### Added
- `@checkpoint(runtime="isolated")` parameter for running individual checkpoints in separate containers on remote orchestrators (Kubernetes, Vertex, SageMaker, AzureML); accepts `"inline"`, `"isolated"`, or `StepRuntime` enum values with early validation

### Changed
- Replace LiteLLM dependency with direct OpenAI and Anthropic SDK support
  - `openai` and `anthropic` are now optional extras: `pip install kitaru[openai]`, `pip install kitaru[anthropic]`, or `pip install kitaru[llm]` for both
  - `kitaru.llm()` public API is unchanged; lazy imports raise a clear `KitaruUsageError` with install guidance if the required SDK is not installed
  - Built-in runtime support now covers `openai/*`, `anthropic/*`, `ollama/*`, and `openrouter/*` models; other providers can be used directly inside `@checkpoint`
  - Ollama and OpenRouter use the OpenAI-compatible API (no new dependencies, reuse `kitaru[openai]`)
  - Model alias resolution, credential handling, and artifact/metadata persistence are unchanged
  - `cost_usd` metadata field is now omitted (direct provider SDKs do not include cost data)

### Removed
- `litellm` core dependency (removed due to [PyPI supply chain compromise](https://github.com/BerriAI/litellm/issues/24512) in versions 1.82.7–1.82.8)

## [0.2.1] - 2026-03-23

## [0.2.0] - 2026-03-20

### Added
- `docker/Dockerfile.server-dev` for local server + UI development without a published UI release

### Changed
- Switch ZenML dependency from pinned git commit to PyPI release (`zenml>=0.94.1`)
- Production server Docker image now layers on `zenmldocker/zenml-server` instead of rebuilding ZenML from source
- Kitaru UI is now bundled into the server image, replacing the ZenML dashboard
- Flow-execution image (`docker/Dockerfile.dev`) now installs ZenML from PyPI instead of git

### Removed
- `_FlowDefinition.deploy()` method; `.run(stack="...")` is now the single way to start a flow execution, whether local or remote
- `FlowInvocationResult.invocation` field and the `"invocation"` key in MCP run-tool payloads
- `kitaru run` CLI command and its live terminal renderer; flow execution is now started via Python (`my_flow.run(...)` / `my_flow.deploy(...)`) or MCP tools, while the CLI focuses on execution lifecycle management via `kitaru executions ...`
- `kitaru.terminal` module (run-only Rich Live renderer and helpers)
- Runtime submission observer plumbing (`_submission_observer`, `_notify_submission_observer`) from `kitaru.runtime` and `kitaru.flow`

### Added
- Unified config directory: Kitaru and ZenML now share a single config directory by default; the init hook sets `ZENML_CONFIG_PATH` to Kitaru's app dir so the database, credentials, and local stores live alongside Kitaru's own config; `KITARU_CONFIG_PATH` overrides the location for both; `kitaru status` now reports this unified directory
- `kitaru init` command to initialize a project root by creating a `.kitaru/` directory; this sets the source root for code packaging during remote execution and prevents ambiguous source-root heuristics; the command checks for both `.kitaru/` and legacy `.zen/` markers before initializing
- `kitaru executions input` now auto-detects the single pending wait condition, removing the need for `--wait`; use `--interactive` (`-i`) for guided review with JSON schema display, continue/abort/skip/quit actions, and multi-execution sweep mode; use `--abort` to abort a wait in non-interactive mode
- `KitaruClient.executions.pending_waits(exec_id)` returns all pending wait conditions for an execution
- `KitaruClient.executions.abort_wait(exec_id, wait=...)` aborts a pending wait condition
- MCP local lifecycle tools: `kitaru_start_local_server(port?, timeout?)` and `kitaru_stop_local_server()`
- Native Kitaru terminal logging: ZenML console output is now intercepted and rewritten to Kitaru vocabulary (pipeline→flow, step→checkpoint, run→execution) with colored lifecycle markers; ZenML-specific noise (Dashboard URLs, user/build info, component listings) is suppressed from the terminal while remaining available in stored logs via `kitaru executions logs`
- Shared source-alias module (`kitaru._source_aliases`) centralizing alias prefix constants and normalization helpers previously duplicated across 7+ files

### Changed
- **Breaking:** `kitaru executions input` no longer accepts `--wait`; the CLI auto-detects the single pending wait (use `--interactive` for multi-wait executions). MCP `kitaru_executions_input` still requires explicit `wait` for deterministic tool calls.
- Flows and checkpoints now register with plain names in ZenML (e.g. `my_flow`, `fetch_data`) instead of prefixed internal aliases (`__kitaru_pipeline_source_my_flow`, `__kitaru_checkpoint_source_fetch_data`); the internal source aliases remain for ZenML source loading but are no longer visible in the ZenML UI or API responses
- Moved Claude Code skills (kitaru-scoping, kitaru-authoring) to dedicated repository: [zenml-io/kitaru-skills](https://github.com/zenml-io/kitaru-skills)
- Config and stack helpers now raise Kitaru-specific exception subclasses instead of raw `ValueError` / `RuntimeError`, while preserving compatibility through inheritance
- `kitaru stack list --output json` and MCP `kitaru_stacks_list` now include `is_managed`, derived from the stack's `kitaru.managed` label
- `kitaru stack create --type kubernetes` and MCP `manage_stack(action="create", stack_type="kubernetes", ...)` are now backed by ZenML's one-shot stack provisioning flow: Kitaru validates provider-specific credentials, preflights the connector config, creates the cloud connector plus Kubernetes/orchestrator, artifact-store, and container-registry components transactionally, and returns the richer stack-create metadata (including service connectors and cloud resources) through both surfaces
- `kitaru stack create --type vertex` and MCP `manage_stack(action="create", stack_type="vertex", ...)` now ship the first cloud-managed runner flow beyond Kubernetes: Kitaru provisions a GCP connector plus Vertex orchestrator, GCS artifact store, and GCP container registry components transactionally and returns the richer stack-create metadata through both surfaces
- `kitaru stack create --type sagemaker` and MCP `manage_stack(action="create", stack_type="sagemaker", ...)` now provision an AWS connector plus SageMaker orchestrator, S3 artifact store, and ECR container registry transactionally; `kitaru stack show` / structured stack inspection now classify SageMaker stacks explicitly and surface the runner `execution_role`
- `kitaru stack create --type azureml` and MCP `manage_stack(action="create", stack_type="azureml", ...)` now provision an Azure connector plus AzureML orchestrator, Azure artifact store, and Azure container registry transactionally; `kitaru stack show` / structured stack inspection now classify AzureML stacks explicitly and surface the runner subscription, resource group, workspace, and location
- `kitaru stack create` now accepts `--file/-f` YAML input, letting stack definitions come from a config file while keeping explicit CLI flags authoritative when both are provided
- Stack creation internals now share one CLI/MCP validation layer across local, Kubernetes, Vertex, SageMaker, and AzureML flows, and `kitaru stack show` / structured stack inspection now classify managed-runner stacks explicitly and surface runner-specific metadata (`location` for Vertex, `execution_role` for SageMaker, and subscription/resource-group/workspace details for AzureML)
- `kitaru stack create` and MCP `manage_stack(action="create", ...)` now support advanced component defaults via repeatable `--extra` / structured `extra`, plus the convenience `--async` / `async_mode` flag for remote orchestrators; invalid advanced ZenML options are now rewritten into clear user-facing `KitaruUsageError` messages with suggestions and docs links when available
- Flow submissions now serialize temporary stack rebinding within a Python process, making per-run/decorator/runtime stack overrides safer when multiple executions are submitted concurrently
- Model aliases registered via `kitaru model register` are now automatically transported to submitted and replayed remote executions via `KITARU_MODEL_REGISTRY`; `kitaru.llm()` and `kitaru model list` now read the effective registry visible in the current environment, and frozen execution specs capture that transported snapshot for debugging
- `kitaru stack delete --recursive` now gives Kubernetes-managed stacks full cleanup parity by reporting container-registry deletion alongside the orchestrator and artifact store and by garbage-collecting unshared linked service connectors after a successful delete
- Examples are now grouped into topic-focused subdirectories under `examples/`, each with its own README, and can be run with `uv run examples/<path>.py`; the root README, docs site, and tester guide now point to a unified examples catalog
- Kitaru now treats `KITARU_*` environment variables as the public configuration surface for remote connection/bootstrap, translating the supported connection/debug vars into `ZENML_*` env vars before CLI/SDK startup
- Connection resolution now understands direct `ZENML_*` env vars as a compatibility layer below `KITARU_*`, while env-driven remote connections fail at first use unless an explicit project is set
- `kitaru status` now includes an Environment section showing active `KITARU_*` variables with token/API-key masking
- `kitaru login` now starts and connects to a local daemon server when you omit `SERVER`; remote login remains `kitaru login <server>`
- `kitaru login` CLI flags now distinguish local and remote modes: removed `--url` and `--cloud-api-url` / `--pro-api-url`, added local `--port`, and made `--timeout` shared across local startup and remote connection flows
- Local login now warns — instead of failing — when `KITARU_*` / `ZENML_*` auth environment overrides are active; remote login and `kitaru logout` still refuse to fight those environment variables
- `kitaru logout --output json` now includes `local_server_stopped`, and logout now also tears down any registered local daemon while disconnecting from remote state
- Kitaru now supports `KITARU_CONFIG_PATH` for relocating its config directory and `KITARU_DEFAULT_MODEL` for setting the default `kitaru.llm()` model without touching the alias registry
- The production Docker image now uses `KITARU_DEBUG` / `KITARU_ANALYTICS_OPT_IN` defaults and documents `KITARU_SERVER_URL` / `KITARU_AUTH_TOKEN` / `KITARU_PROJECT` for headless server connection setup
- `kitaru status` and `kitaru log-store show` now surface a mismatch warning when the Kitaru log-store preference differs from the active stack's ZenML stack log store
- Kitaru's global config file now lives in Kitaru's OS-aware app config directory (for example `~/.config/kitaru/config.yaml` on Linux or `~/Library/Application Support/kitaru/config.yaml` on macOS)
- CLI output (`kitaru status`, `kitaru info`) no longer exposes ZenML config paths or local stores path
- Project is no longer inferred from ZenML's active project; `ResolvedConnectionConfig.project` only reflects explicit overrides via `KITARU_PROJECT` env var or `kitaru.configure(project=...)`
- `kitaru info` shows "Project override" row only when an explicit override is set (instead of always showing "Active project")
- `kitaru` and `kitaru-mcp` now fail fast with a clear message on Python versions older than 3.11
- CLI and MCP startup no longer resolve the Kitaru package version eagerly at import time; missing metadata now falls back to `unknown`
- `kitaru login` no longer prints "Active project" in its success output
- `kitaru.configure()` now accepts a `project` parameter for internal/testing use

### Added
- Local stack lifecycle support across SDK, CLI, and MCP: `kitaru.create_stack()`, `kitaru.delete_stack()`, `kitaru stack create/delete`, and MCP `manage_stack`
- New local-stack semantics: `kitaru stack create <name>` auto-activates by default, `--no-activate` leaves the current stack unchanged, and forced active-stack deletion falls back to the default stack
- `kitaru stack show <name-or-id>` for inspecting one stack in Kitaru vocabulary, including translated runner/storage/image-registry component details in both text and JSON output
- Runtime log retrieval with Rich-based checkpoint-by-checkpoint progress display for execution inspection
- Runtime log retrieval lane: `KitaruClient.executions.logs(...)`, `kitaru executions logs` (with `--follow`, `--grouped`, `-v`/`-vv`, and JSONL output), and MCP `get_execution_logs`
- Runtime log retrieval docs updates across logging/log-store guides plus a new getting-started page for execution logs
- Production Docker image (`docker/Dockerfile`): multi-stage server image based on ZenML server architecture with all cloud plugins, published as `zenmldocker/kitaru` during releases
- Docker image build and push integrated into the release workflow (`release.yml`)
- `.dockerignore` to keep Docker build context clean
- Justfile recipes: `just server-image` and `just server-image-push` for local Docker builds
- Phase 16 replay support: replay planning (`src/kitaru/replay.py`), `KitaruClient.executions.replay(...)`, flow-object replay (`my_flow.replay(...)`), `kitaru executions replay`, and fully-enabled MCP replay tool responses
- Replay docs and examples: `/getting-started/replay-and-overrides`, updated execution/error/MCP docs, and `examples/features/replay/replay_with_overrides.py`
- Agent-native MCP server surface: optional `kitaru[mcp]` extra, `kitaru-mcp` console entry point, and Phase 19 MCP tools for execution/artifact/status/stack queries
- Claude Code authoring skill: `.claude-plugin/skills/kitaru-authoring/SKILL.md` (installable via plugin marketplace)
- Phase 19 example workflow: `examples/features/mcp/mcp_query_tools.py`
- MCP-focused tests: import guard coverage (`tests/test_mcp_import_guard.py`) and tool wrapper tests (`tests/mcp/test_server.py`)
- Agent integrations docs pages: `/agent-integrations/mcp-server` and `/agent-integrations/claude-code-skill`
- PydanticAI framework adapter: `kitaru.adapters.pydantic_ai.wrap(agent)` for checkpoint-scoped child-event tracking of model/tool activity
- Adapter capture policy controls: `tool_capture_config` + `tool_capture_config_by_name` with `full`, `metadata_only`, and `off` modes
- Adapter run-summary metadata (`pydantic_ai_run_summaries`) and event-stream-handler metadata (`pydantic_ai_event_stream_handlers`)
- Adapter stream transcript artifacts (`*_stream_transcript`) for streaming replay inspection
- Adapter HITL tool decorator: `kitaru.adapters.pydantic_ai.hitl_tool(...)` with flow-level wait translation
- Optional dependency extra: `pydantic-ai` (`pydantic-ai-slim`)
- Phase 17 runnable example: `examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py`
- Phase 17 integration/unit tests for adapter tracking, runtime scope suspension, HITL behavior, capture config, stream transcripts, and synthetic flow-scope run semantics
- Getting Started docs page for the PydanticAI adapter (`/getting-started/pydantic-ai-adapter`)
- Typed Kitaru exception hierarchy (`KitaruError`, `KitaruContextError`, `KitaruStateError`, `KitaruExecutionError`, `KitaruUserCodeError`, `KitaruDivergenceError`, `KitaruFeatureNotAvailableError`, and related types)
- Failure journaling in `KitaruClient`: structured execution-level failure details (`execution.failure`) and per-checkpoint retry attempt history (`checkpoint.attempts`)
- Phase 14 execution CLI commands: `kitaru executions get/list/retry/cancel`
- Getting Started error-handling docs page (`/getting-started/error-handling`)
- `kitaru.llm()` implementation with LiteLLM backend, context-aware flow/checkpoint behavior, prompt/response artifact capture, and automatic usage/cost/latency metadata logging
- Local model alias registry persisted in Kitaru's user config file, including default alias behavior and model-resolution helpers for `kitaru.llm()`
- Model registry CLI surface: `kitaru model register` and `kitaru model list`
- Phase 12 example workflow: `examples/features/llm/flow_with_llm.py`
- Getting Started LLM docs page (`/getting-started/llm-calls`)
- Secrets CLI surface: `kitaru secrets set/show/list/delete`
- `kitaru secrets set` create-or-update behavior with private-by-default secret creation
- Secret assignment parsing with env-var-style key validation (`--KEY=value`)
- `KitaruClient` execution management API with Kitaru domain models (`Execution`, `ExecutionStatus`, `CheckpointCall`, `ArtifactRef`)
- Execution management operations: `client.executions.get/list/latest/cancel/retry`
- Artifact browsing operations: `client.artifacts.list/get` and `artifact.load()`
- Phase 11 example workflow: `examples/features/execution_management/client_execution_management.py`
- Getting Started execution management docs page (`/getting-started/execution-management`)
- `kitaru.wait(...)` implementation with flow-only guardrails and checkpoint-context blocking
- Wait-input lifecycle APIs: `client.executions.input(...)` and `client.executions.resume(...)`
- Execution CLI wait/resume commands: `kitaru executions input` and `kitaru executions resume`
- Phase 15 wait/resume example workflow: `examples/features/execution_management/wait_and_resume.py`
- Getting Started wait/resume docs page (`/getting-started/wait-and-resume`)
- `kitaru.save()` for explicit named artifact persistence inside checkpoints
- `kitaru.load()` for cross-execution artifact loading inside checkpoints
- Artifact taxonomy validation for explicit `kitaru.save(..., type=...)` values (`prompt`, `response`, `context`, `input`, `output`, `blob`)
- Phase 8 example workflow: `examples/features/basic_flow/flow_with_artifacts.py`
- Global log-store configuration with `kitaru log-store set/show/reset`
- Active stack selection in SDK via `kitaru.list_stacks()`, `kitaru.current_stack()`, and `kitaru.use_stack()`
- Active stack CLI commands: `kitaru stack list/current/use`
- Runtime configuration API: `kitaru.configure(...)`
- Unified config models: `kitaru.KitaruConfig` and `kitaru.ImageSettings`
- Execution config precedence resolution across invocation/decorator/runtime/env/project/global/default layers
- Frozen execution spec persistence on each flow run (`kitaru_execution_spec` metadata)
- Phase 10 example workflow: `examples/features/basic_flow/flow_with_configuration.py`
- Getting Started configuration docs page (`/getting-started/configuration`)
- Persisted Kitaru user config (`config.yaml`) for log-store override state
- Environment override support for runtime log-store resolution

### Changed
- Runtime internals now include `_suspend_checkpoint_scope()` to support adapter-managed flow-level waits during checkpoint-local agent execution
- PydanticAI adapter event metadata now includes timing (`duration_ms`), explicit ordering/lineage fields (`sequence_index`, `turn_index`, `fan_out_from`, `fan_in_from`), and immutable wrapper dispatch semantics across function/MCP/generic toolsets
- Wrapped PydanticAI `run()` / `run_sync()` calls at flow scope now use a synthetic `llm_call` checkpoint boundary so adapter tracking remains available outside explicit checkpoints
- Kitaru global config persistence now uses field-preserving updates, so log-store and model-registry settings no longer clobber each other
- Updated README, CLAUDE guide, AGENTS guide, and docs pages to reflect shipped LLM/model-registry functionality and current implemented primitive status
- Updated the CLI/docs surface so generated command reference pages show real positional usage, `executions logs`/`executions replay` appear everywhere they should, and runtime logs are documented separately from structured metadata
- Agent-facing CLI commands now support a consistent `--output json` / `-o json` contract, with single-item commands emitting `{command, item}`, list commands emitting `{command, items, count}`, and structured JSON errors on stderr
- `kitaru executions logs --output json` now returns a JSON envelope for non-follow mode, while `--follow --output json` emits JSONL event objects (`log`, `waiting`, `terminal`, `interrupted`)
- Added a dedicated secrets + model registration walkthrough and clarified the current secret story: `kitaru.llm()` auto-resolves linked secrets, while non-LLM secret access remains a low-level pattern
- Updated quickstart, docs, and README wording to reflect shipped replay/log/MCP behavior, typed errors, and current Claude Code skill packaging

## [0.1.0] - 2026-03-06

### Added
- Initial project scaffolding with uv, ruff, ty, and CI
- CLI with cyclopts (`kitaru --version`, `kitaru --help`)
- Justfile for common development commands
- Link checking with lychee
- Typo checking with typos
