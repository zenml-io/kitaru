# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- OSS-first auth management for service accounts and API keys via `KitaruClient.auth`, `kitaru auth service-accounts`, and `kitaru auth api-keys`. Raw API-key values are only returned on create/rotate so they can be stored immediately; list/show/update responses stay metadata-only.

### Fixed
- Checkpoint output handles now display Kitaru guidance to call `.load()` instead of leaking raw ZenML artifact metadata when stringified in flow bodies. (#127)

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
