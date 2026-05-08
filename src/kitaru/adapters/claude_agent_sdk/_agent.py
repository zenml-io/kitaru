"""Public runner wrapper for the Claude Agent SDK adapter."""

import asyncio
import time
from collections.abc import Callable
from typing import Any, cast

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._policy import ClaudeCapturePolicy
from ._runner import (
    ClaudeInvocationPayload,
    claude_agent_sdk_version,
    run_claude_invocation,
)
from ._serialization import redacted_options_manifest, to_cache_identity
from ._tracking import ArtifactKind, EventTracker, tracker_scope
from ._types import ClaudeRunRequest, ClaudeRunResult
from ._utils import (
    CheckpointConfig,
    CheckpointStrategy,
    checkpoint_cache_key,
    elapsed_ms,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
    validate_checkpoint_config,
    validate_checkpoint_strategy,
)


class KitaruClaudeRunner:
    """Wrap one Claude Agent SDK invocation in one Kitaru checkpoint.

    The v0.1 adapter promise is intentionally coarse: one call to
    ``claude_agent_sdk.query(...)`` becomes one durable Kitaru boundary. Claude's
    internal model calls, tool calls, Bash commands, MCP calls, and hooks pass
    through unchanged and are not checkpointed individually.
    """

    def __init__(
        self,
        *,
        name: str,
        options: Any | None = None,
        options_factory: Callable[[ClaudeRunRequest], Any | None] | None = None,
        checkpoint_strategy: CheckpointStrategy = "invocation",
        capture: ClaudeCapturePolicy | None = None,
        checkpoint_config: CheckpointConfig | None = None,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise KitaruUsageError("KitaruClaudeRunner requires a stable `name`.")
        if options is not None and options_factory is not None:
            raise KitaruUsageError(
                "`options` and `options_factory` are mutually exclusive."
            )

        self._name = name
        self._options = options
        self._options_factory = options_factory
        self._checkpoint_strategy = validate_checkpoint_strategy(checkpoint_strategy)
        self._capture = capture or ClaudeCapturePolicy()
        self._checkpoint_config: CheckpointConfig = validate_checkpoint_config(
            checkpoint_config,
            context="checkpoint_config",
        ) or cast(CheckpointConfig, {})

        track(
            AnalyticsEvent.CLAUDE_AGENT_SDK_WRAPPED,
            {
                "checkpoint_strategy": self._checkpoint_strategy,
                "has_options_factory": options_factory is not None,
            },
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def checkpoint_strategy(self) -> CheckpointStrategy:
        return self._checkpoint_strategy

    @property
    def capture(self) -> ClaudeCapturePolicy:
        return self._capture

    async def run(self, request: ClaudeRunRequest) -> ClaudeRunResult:
        """Run one Claude Agent SDK invocation asynchronously."""
        self._require_invocation_scope("KitaruClaudeRunner.run()")
        try:
            result = await self._run_invocation_async(request)
        except Exception:
            self._track_completed("run", status="failed", result=None)
            raise
        self._track_completed("run", status="completed", result=result)
        return result

    def run_sync(self, request: ClaudeRunRequest) -> ClaudeRunResult:
        """Run one Claude Agent SDK invocation synchronously."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise KitaruUsageError(
                "`KitaruClaudeRunner.run_sync()` cannot be called inside an already "
                "running event loop. Use `await KitaruClaudeRunner.run(...)` instead."
            )
        self._require_invocation_scope("KitaruClaudeRunner.run_sync()")
        try:
            result = self._run_invocation_sync(request)
        except Exception:
            self._track_completed("run_sync", status="failed", result=None)
            raise
        self._track_completed("run_sync", status="completed", result=result)
        return result

    async def _run_invocation_async(self, request: ClaudeRunRequest) -> ClaudeRunResult:
        options = self._build_options(request)

        async def _body() -> ClaudeRunResult:
            return await self._run_sdk_async(request, options=options)

        if is_inside_flow() and not is_inside_checkpoint():
            return await run_async_in_checkpoint(
                config=self._invocation_checkpoint_config(),
                step_name=f"{self._name}_claude_invocation",
                body=_body,
                cache_key=self._invocation_cache_key(request, options=options),
            )
        return await _body()

    def _run_invocation_sync(self, request: ClaudeRunRequest) -> ClaudeRunResult:
        options = self._build_options(request)

        def _body() -> ClaudeRunResult:
            return asyncio.run(self._run_sdk_async(request, options=options))

        if is_inside_flow() and not is_inside_checkpoint():
            return run_sync_in_checkpoint(
                config=self._invocation_checkpoint_config(),
                step_name=f"{self._name}_claude_invocation",
                body=_body,
                cache_key=self._invocation_cache_key(request, options=options),
            )
        return _body()

    async def _run_sdk_async(
        self,
        request: ClaudeRunRequest,
        *,
        options: Any | None,
    ) -> ClaudeRunResult:
        with tracker_scope(self._name) as tracker:
            manifest = redacted_options_manifest(
                options,
                request,
                redact=self._capture.redact_options_manifest,
            )
            started_at = time.perf_counter()
            try:
                payload = await run_claude_invocation(
                    request=request,
                    options=options,
                    save_transcript_file=self._capture.save_transcript_file,
                )
            except Exception as error:
                self._record_failed_invocation(
                    tracker,
                    error=error,
                    manifest=manifest,
                    request=request,
                    duration_ms=elapsed_ms(started_at),
                )
                raise
            return self._finalize_run_result(
                payload,
                tracker=tracker,
                manifest=manifest,
                request=request,
            )

    def _finalize_run_result(
        self,
        payload: ClaudeInvocationPayload,
        *,
        tracker: EventTracker,
        manifest: dict[str, Any],
        request: ClaudeRunRequest,
    ) -> ClaudeRunResult:
        artifacts = self._persist_capture_artifacts(
            tracker,
            payload=payload,
            manifest=manifest,
            request=request,
        )
        if self._capture.emit_events:
            tracker.record_invocation(
                status="completed",
                duration_ms=payload.duration_ms,
                session_id=payload.session_id,
                transcript_path=payload.transcript_path,
                warnings=payload.warnings,
                artifacts=artifacts,
                metadata={
                    "sdk_version": claude_agent_sdk_version(),
                    "has_usage": payload.usage is not None,
                    "message_count": len(payload.messages),
                },
            )
        return ClaudeRunResult(
            session_id=payload.session_id,
            final_text=payload.final_text,
            transcript_path=payload.transcript_path,
            usage=payload.usage,
            cost_usd=payload.cost_usd,
            model_usage=payload.model_usage,
            stop_reason=payload.stop_reason,
            subtype=payload.subtype,
            num_turns=payload.num_turns,
            duration_ms=payload.duration_ms,
            duration_api_ms=payload.duration_api_ms,
            messages_artifact_name=artifacts.get("messages"),
            transcript_artifact_name=artifacts.get("transcript"),
            options_manifest_artifact_name=artifacts.get("options_manifest"),
            output_artifact_name=artifacts.get("output"),
            usage_artifact_name=artifacts.get("usage"),
            event_log_artifact_name=(
                tracker.event_log_artifact_name if self._capture.emit_events else None
            ),
            run_summary_artifact_name=(
                tracker.run_summary_artifact_name if self._capture.emit_events else None
            ),
            warnings=payload.warnings,
            metadata={"run_label": tracker.run_label},
        )

    def _persist_capture_artifacts(
        self,
        tracker: EventTracker,
        *,
        payload: ClaudeInvocationPayload,
        manifest: dict[str, Any],
        request: ClaudeRunRequest,
    ) -> dict[str, str]:
        artifacts: dict[str, str] = {}

        def _maybe_register_artifact(
            key: ArtifactKind,
            *,
            enabled: bool,
            payload_value: Any,
            type: str,
        ) -> None:
            if not enabled:
                return
            artifacts[key] = tracker.artifact_name(key)
            if is_inside_checkpoint():
                self._save_artifact(artifacts[key], payload_value, type=type)

        messages_payload: dict[str, Any] = {"messages": payload.messages}
        if self._capture.save_prompt:
            messages_payload["prompt"] = request.prompt

        _maybe_register_artifact(
            "messages",
            enabled=self._capture.save_messages,
            payload_value=messages_payload,
            type="context",
        )
        _maybe_register_artifact(
            "transcript",
            enabled=(
                self._capture.save_transcript_file
                and payload.transcript_payload is not None
            ),
            payload_value=payload.transcript_payload,
            type="context",
        )
        _maybe_register_artifact(
            "options_manifest",
            enabled=self._capture.save_options_manifest,
            payload_value=manifest,
            type="context",
        )
        _maybe_register_artifact(
            "output",
            enabled=self._capture.save_final_output,
            payload_value=payload.final_text,
            type="response",
        )
        _maybe_register_artifact(
            "usage",
            enabled=self._capture.save_usage
            and (payload.usage is not None or payload.model_usage is not None),
            payload_value={
                "usage": payload.usage,
                "model_usage": payload.model_usage,
                "cost_usd": payload.cost_usd,
            },
            type="context",
        )

        return artifacts

    def _record_failed_invocation(
        self,
        tracker: EventTracker,
        *,
        error: BaseException,
        manifest: dict[str, Any],
        request: ClaudeRunRequest,
        duration_ms: float,
    ) -> None:
        artifacts: dict[str, str] = {}
        if self._capture.save_options_manifest:
            artifacts["options_manifest"] = tracker.artifact_name("options_manifest")
            if is_inside_checkpoint():
                self._save_artifact(
                    artifacts["options_manifest"], manifest, type="context"
                )
        if self._capture.emit_events:
            tracker.record_invocation(
                status="failed",
                duration_ms=duration_ms,
                artifacts=artifacts,
                metadata={
                    "sdk_version": claude_agent_sdk_version(),
                    "request_kind": request.kind,
                },
                error=error,
            )

    def _build_options(self, request: ClaudeRunRequest) -> Any | None:
        if self._options_factory is not None:
            return self._options_factory(request)
        if not self._request_needs_sdk_options(request):
            return self._options
        if self._options is not None:
            raise KitaruUsageError(
                "ClaudeRunRequest fields `cwd`, `max_turns`, and "
                "`resume_session_id` are request-scoped SDK options. Use "
                "`options_factory` to merge them with static options."
            )
        return self._options_from_request(request)

    @staticmethod
    def _request_needs_sdk_options(request: ClaudeRunRequest) -> bool:
        return any(
            value is not None
            for value in (request.cwd, request.max_turns, request.resume_session_id)
        )

    @staticmethod
    def _options_from_request(request: ClaudeRunRequest) -> Any:
        try:
            from claude_agent_sdk import ClaudeAgentOptions
        except (ImportError, AttributeError) as exc:
            raise KitaruUsageError(
                "ClaudeRunRequest fields `cwd`, `max_turns`, and "
                "`resume_session_id` require Claude Agent SDK options support. "
                "Pass an `options_factory` if your installed SDK exposes a "
                "different options type."
            ) from exc
        return ClaudeAgentOptions(
            cwd=request.cwd,
            resume=request.resume_session_id,
            max_turns=request.max_turns,
        )

    def _invocation_checkpoint_config(self) -> CheckpointConfig:
        return {
            **self._checkpoint_config,
            "type": self._checkpoint_config.get("type", "agent_call"),
        }

    def _invocation_cache_key(
        self, request: ClaudeRunRequest, *, options: Any | None
    ) -> str:
        return checkpoint_cache_key(
            {
                "adapter": "claude_agent_sdk",
                "checkpoint_strategy": "invocation",
                "claude_agent_sdk_version": claude_agent_sdk_version(),
                "runner_name": self._name,
                "request": request.model_dump(mode="json"),
                "options": to_cache_identity(options),
            }
        )

    def _require_invocation_scope(self, api_name: str) -> None:
        if is_inside_flow() or is_inside_checkpoint():
            return
        raise KitaruUsageError(
            f"{api_name} must be called inside a Kitaru flow or checkpoint. "
            "Claude Agent SDK v0.1 durability is one invocation per checkpoint; "
            "wrap the call in @kitaru.flow, or call it from an existing "
            "@kitaru.checkpoint."
        )

    def _track_completed(
        self,
        surface: str,
        *,
        status: str,
        result: ClaudeRunResult | None,
    ) -> None:
        has_result = result is not None
        track(
            AnalyticsEvent.CLAUDE_AGENT_SDK_RUN_COMPLETED,
            {
                "surface": surface,
                "checkpoint_strategy": self._checkpoint_strategy,
                "status": status,
                "has_usage": has_result and result.usage is not None,
                "has_session_id": has_result and result.session_id is not None,
            },
        )

    @staticmethod
    def _save_artifact(name: str, value: Any, *, type: str) -> None:
        import kitaru

        kitaru.save(name, value, type=type)
