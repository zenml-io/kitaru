# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- Replace LiteLLM dependency with direct OpenAI and Anthropic SDK support
  - `openai` and `anthropic` are now optional extras: `pip install kitaru[openai]`, `pip install kitaru[anthropic]`, or `pip install kitaru[llm]` for both
  - `kitaru.llm()` public API is unchanged; lazy imports raise a clear `KitaruUsageError` with install guidance if the required SDK is not installed
  - Built-in runtime support now covers `openai/*`, `anthropic/*`, `ollama/*`, and `openrouter/*` models; other providers can be used directly inside `@checkpoint`
  - Ollama and OpenRouter use the OpenAI-compatible API (no new dependencies, reuse `kitaru[openai]`)
  - Model alias resolution, credential handling, and artifact/metadata persistence are unchanged
  - `cost_usd` metadata field is now omitted (direct provider SDKs do not include cost data)

### Removed
- `litellm` core dependency (removed due to PyPI supply chain compromise in versions 1.82.7–1.82.8)

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
- Replay docs and examples: `/getting-started/replay-and-overrides`, updated execution/error/MCP docs, and `examples/replay_with_overrides.py`
- Agent-native MCP server surface: optional `kitaru[mcp]` extra, `kitaru-mcp` console entry point, and Phase 19 MCP tools for execution/artifact/status/stack queries
- Claude Code authoring skill: `.claude-plugin/skills/kitaru-authoring/SKILL.md` (installable via plugin marketplace)
- Phase 19 example workflow: `examples/mcp_query_tools.py`
- MCP-focused tests: import guard coverage (`tests/test_mcp_import_guard.py`) and tool wrapper tests (`tests/mcp/test_server.py`)
- Agent integrations docs pages: `/agent-integrations/mcp-server` and `/agent-integrations/claude-code-skill`
- PydanticAI framework adapter: `kitaru.adapters.pydantic_ai.wrap(agent)` for checkpoint-scoped child-event tracking of model/tool activity
- Adapter capture policy controls: `tool_capture_config` + `tool_capture_config_by_name` with `full`, `metadata_only`, and `off` modes
- Adapter run-summary metadata (`pydantic_ai_run_summaries`) and event-stream-handler metadata (`pydantic_ai_event_stream_handlers`)
- Adapter stream transcript artifacts (`*_stream_transcript`) for streaming replay inspection
- Adapter HITL tool decorator: `kitaru.adapters.pydantic_ai.hitl_tool(...)` with flow-level wait translation
- Optional dependency extra: `pydantic-ai` (`pydantic-ai-slim`)
- Phase 17 runnable example: `examples/pydantic_ai_adapter.py`
- Phase 17 integration/unit tests for adapter tracking, runtime scope suspension, HITL behavior, capture config, stream transcripts, and synthetic flow-scope run semantics
- Getting Started docs page for the PydanticAI adapter (`/getting-started/pydantic-ai-adapter`)
- Typed Kitaru exception hierarchy (`KitaruError`, `KitaruContextError`, `KitaruStateError`, `KitaruExecutionError`, `KitaruUserCodeError`, `KitaruDivergenceError`, `KitaruFeatureNotAvailableError`, and related types)
- Failure journaling in `KitaruClient`: structured execution-level failure details (`execution.failure`) and per-checkpoint retry attempt history (`checkpoint.attempts`)
- Phase 14 execution CLI commands: `kitaru executions get/list/retry/cancel`
- Getting Started error-handling docs page (`/getting-started/error-handling`)
- `kitaru.llm()` implementation with LiteLLM backend, context-aware flow/checkpoint behavior, prompt/response artifact capture, and automatic usage/cost/latency metadata logging
- Local model alias registry persisted in Kitaru's user config file, including default alias behavior and model-resolution helpers for `kitaru.llm()`
- Model registry CLI surface: `kitaru model register` and `kitaru model list`
- Phase 12 example workflow: `examples/flow_with_llm.py`
- Getting Started LLM docs page (`/getting-started/llm-calls`)
- Secrets CLI surface: `kitaru secrets set/show/list/delete`
- `kitaru secrets set` create-or-update behavior with private-by-default secret creation
- Secret assignment parsing with env-var-style key validation (`--KEY=value`)
- `KitaruClient` execution management API with Kitaru domain models (`Execution`, `ExecutionStatus`, `CheckpointCall`, `ArtifactRef`)
- Execution management operations: `client.executions.get/list/latest/cancel/retry`
- Artifact browsing operations: `client.artifacts.list/get` and `artifact.load()`
- Phase 11 example workflow: `examples/client_execution_management.py`
- Getting Started execution management docs page (`/getting-started/execution-management`)
- `kitaru.wait(...)` implementation with flow-only guardrails and checkpoint-context blocking
- Wait-input lifecycle APIs: `client.executions.input(...)` and `client.executions.resume(...)`
- Execution CLI wait/resume commands: `kitaru executions input` and `kitaru executions resume`
- Phase 15 wait/resume example workflow: `examples/wait_and_resume.py`
- Getting Started wait/resume docs page (`/getting-started/wait-and-resume`)
- `kitaru.save()` for explicit named artifact persistence inside checkpoints
- `kitaru.load()` for cross-execution artifact loading inside checkpoints
- Artifact taxonomy validation for explicit `kitaru.save(..., type=...)` values (`prompt`, `response`, `context`, `input`, `output`, `blob`)
- Phase 8 example workflow: `examples/flow_with_artifacts.py`
- Global log-store configuration with `kitaru log-store set/show/reset`
- Active stack selection in SDK via `kitaru.list_stacks()`, `kitaru.current_stack()`, and `kitaru.use_stack()`
- Active stack CLI commands: `kitaru stack list/current/use`
- Runtime configuration API: `kitaru.configure(...)`
- Unified config models: `kitaru.KitaruConfig` and `kitaru.ImageSettings`
- Execution config precedence resolution across invocation/decorator/runtime/env/project/global/default layers
- Frozen execution spec persistence on each flow run (`kitaru_execution_spec` metadata)
- Phase 10 example workflow: `examples/flow_with_configuration.py`
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
