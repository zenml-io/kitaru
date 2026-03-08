# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Typed Kitaru exception hierarchy (`KitaruError`, `KitaruContextError`, `KitaruStateError`, `KitaruExecutionError`, `KitaruUserCodeError`, `KitaruDivergenceError`, `KitaruFeatureNotAvailableError`, and related types)
- Failure journaling in `KitaruClient`: structured execution-level failure details (`execution.failure`) and per-checkpoint retry attempt history (`checkpoint.attempts`)
- Phase 14 execution CLI commands: `kitaru run`, `kitaru executions get/list/retry/cancel`
- `kitaru run` JSON argument parsing for flow input kwargs and optional `--stack` deploy mode
- Getting Started error-handling docs page (`/getting-started/error-handling`)
- `kitaru.llm()` implementation with LiteLLM backend, context-aware flow/checkpoint behavior, prompt/response artifact capture, and automatic usage/cost/latency metadata logging
- Local model alias registry persisted in `kitaru.yaml`, including default alias behavior and model-resolution helpers for `kitaru.llm()`
- Model registry CLI surface: `kitaru model register` and `kitaru model list`
- Phase 12 example workflow: `examples/flow_with_llm.py`
- Getting Started LLM docs page (`/getting-started/llm-calls`)
- Secrets CLI surface: `kitaru secrets set/show/list/delete`
- `kitaru secrets set` create-or-update behavior with private-by-default secret creation
- Secret assignment parsing with env-var-style key validation (`--KEY=value`)
- `KitaruClient` execution management API with Kitaru domain models (`Execution`, `ExecutionStatus`, `CheckpointCall`, `ArtifactRef`)
- Execution management operations: `client.executions.get/list/latest/cancel/retry`
- Artifact browsing operations: `client.artifacts.list/get` and `artifact.load()`
- Explicit `NotImplementedError` stub for branch-dependent replay APIs
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
- Persisted Kitaru user config (`kitaru.yaml`) for log-store override state
- Environment override support for runtime log-store resolution

### Changed
- Kitaru global config persistence now uses field-preserving updates, so log-store and model-registry settings no longer clobber each other
- Updated README, CLAUDE guide, AGENTS guide, and docs pages to reflect shipped LLM/model-registry functionality and current implemented primitive status
- Updated execution-management docs to cover shipped wait/input/resume commands and clearly defer replay/log streaming

## [0.1.0] - 2026-03-06

### Added
- Initial project scaffolding with uv, ruff, ty, and CI
- CLI with cyclopts (`kitaru --version`, `kitaru --help`)
- Justfile for common development commands
- Link checking with lychee
- Typo checking with typos
