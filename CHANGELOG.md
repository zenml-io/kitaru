# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- Kitaru now treats `KITARU_*` environment variables as the public configuration surface for remote connection/bootstrap, translating the supported connection/debug vars into `ZENML_*` env vars before CLI/SDK startup
- Connection resolution now understands direct `ZENML_*` env vars as a compatibility layer below `KITARU_*`, while env-driven remote connections fail at first use unless an explicit project is set
- `kitaru status` now includes an Environment section showing active `KITARU_*` variables with token/API-key masking
- `kitaru login` and `kitaru logout` now refuse both `ZENML_*` and `KITARU_*` auth environment overrides, and report the public `KITARU_*` names when those are driving auth
- Kitaru now supports `KITARU_CONFIG_PATH` for relocating its config directory and `KITARU_DEFAULT_MODEL` for setting the default `kitaru.llm()` model without touching the alias registry
- The production Docker image now uses `KITARU_DEBUG` / `KITARU_ANALYTICS_OPT_IN` defaults and documents `KITARU_SERVER_URL` / `KITARU_AUTH_TOKEN` / `KITARU_PROJECT` for headless server connection setup
- `kitaru status` and `kitaru log-store show` now surface a mismatch warning when the Kitaru log-store preference differs from the active ZenML stack log store
- Kitaru's global config file now lives in Kitaru's OS-aware app config directory (for example `~/.config/kitaru/config.yaml` on Linux or `~/Library/Application Support/kitaru/config.yaml` on macOS)
- CLI output (`kitaru status`, `kitaru info`) no longer exposes ZenML config paths or local stores path
- Project is no longer inferred from ZenML's active project; `ResolvedConnectionConfig.project` only reflects explicit overrides via `KITARU_PROJECT` env var or `kitaru.configure(project=...)`
- `kitaru info` shows "Project override" row only when an explicit override is set (instead of always showing "Active project")
- `kitaru` and `kitaru-mcp` now fail fast with a clear message on Python versions older than 3.11
- CLI and MCP startup no longer resolve the Kitaru package version eagerly at import time; missing metadata now falls back to `unknown`
- `kitaru login` no longer prints "Active project" in its success output
- `kitaru.configure()` now accepts a `project` parameter for internal/testing use

### Added
- Kitaru-branded live terminal output: `kitaru run` now shows a Rich Live checkpoint-by-checkpoint progress display during interactive sessions, replacing ZenML's console output with Kitaru-themed visuals
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
- Phase 14 execution CLI commands: `kitaru run`, `kitaru executions get/list/retry/cancel`
- `kitaru run` JSON argument parsing for flow input kwargs and optional `--stack` deploy mode
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
