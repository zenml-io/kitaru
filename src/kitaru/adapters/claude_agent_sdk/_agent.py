"""Public runner wrapper for the Claude Agent SDK adapter."""

import asyncio
import copy
import dataclasses
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from kitaru._llm_usage import build_usage_record, log_usage_record
from kitaru.adapters._result_identity import canonicalize_result_model
from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruRuntimeError, KitaruUsageError

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._policy import ClaudeCapturePolicy
from ._runner import (
    ClaudeInvocationPayload,
    claude_agent_sdk_version,
    run_claude_invocation,
    run_claude_invocation_streamed,
)
from ._serialization import redacted_options_manifest, to_cache_identity
from ._streaming import ClaudeStreamPublisher
from ._tracking import (
    ArtifactKind,
    EventPersistenceFailure,
    EventTracker,
    tracker_scope,
)
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

_DIRECT_EXECUTION_WARNING = (
    "Claude Agent SDK ran directly inside an existing Kitaru checkpoint because "
    "allow_direct_execution_inside_checkpoint=True. Replaying the outer "
    "checkpoint can rerun Claude and duplicate Claude-side tool calls, file "
    "edits, or API cost."
)


def _partial_messages_unavailable_warning() -> str:
    return (
        "Claude stream partial messages could not be enabled without mutating "
        "caller-provided options or because the installed Claude Agent SDK does "
        "not expose partial-message options. The invocation will still run, but "
        "live events may be coarse until the SDK yields complete messages."
    )


ArtifactCaptureOperation = Literal["build_artifact_payload", "save_artifact"]
InvocationSurface = Literal["run", "run_stream", "run_sync", "run_stream_sync"]
InvocationCacheSurface = Literal["run", "stream"]


@dataclass(frozen=True)
class _InvocationPlan:
    options: Any | None
    stream_warnings: list[str]
    stream_publisher: ClaudeStreamPublisher | None
    cache_surface: InvocationCacheSurface


@dataclass(frozen=True)
class ArtifactCaptureFailure:
    """Structured failure from best-effort artifact capture."""

    operation: ArtifactCaptureOperation
    kind: ArtifactKind
    artifact_name: str
    exception_type: str
    message: str

    @classmethod
    def from_exception(
        cls,
        *,
        operation: ArtifactCaptureOperation,
        kind: ArtifactKind,
        artifact_name: str,
        error: BaseException,
    ) -> "ArtifactCaptureFailure":
        return cls(
            operation=operation,
            kind=kind,
            artifact_name=artifact_name,
            exception_type=type(error).__name__,
            message=str(error),
        )

    def as_metadata(self) -> dict[str, str]:
        return {
            "operation": self.operation,
            "kind": self.kind,
            "artifact_name": self.artifact_name,
            "exception_type": self.exception_type,
            "message": self.message,
        }

    def warning(self) -> str:
        return (
            "Claude artifact capture failed for "
            f"{self.kind} artifact {self.artifact_name}: "
            f"{self.exception_type}: {self.message}"
        )

    def as_error(self) -> KitaruRuntimeError:
        return KitaruRuntimeError(
            "Claude Agent SDK artifact capture failed for "
            f"{self.kind} artifact {self.artifact_name}: "
            f"{self.exception_type}: {self.message}"
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
        allow_direct_execution_inside_checkpoint: bool = False,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise KitaruUsageError("KitaruClaudeRunner requires a stable `name`.")
        if options is not None and options_factory is not None:
            raise KitaruUsageError(
                "`options` and `options_factory` are mutually exclusive."
            )
        if not isinstance(allow_direct_execution_inside_checkpoint, bool):
            raise KitaruUsageError(
                "`allow_direct_execution_inside_checkpoint` must be a boolean."
            )

        self._name = name
        self._options = options
        self._options_factory = options_factory
        self._checkpoint_strategy = validate_checkpoint_strategy(checkpoint_strategy)
        self._capture = capture or ClaudeCapturePolicy()
        self._allow_direct_execution_inside_checkpoint = (
            allow_direct_execution_inside_checkpoint
        )
        self._checkpoint_config: CheckpointConfig = validate_checkpoint_config(
            checkpoint_config,
            context="checkpoint_config",
        ) or cast(CheckpointConfig, {})

        track(
            AnalyticsEvent.CLAUDE_AGENT_SDK_WRAPPED,
            {
                "checkpoint_strategy": self._checkpoint_strategy,
                "has_options_factory": options_factory is not None,
                "allow_direct_execution_inside_checkpoint": (
                    self._allow_direct_execution_inside_checkpoint
                ),
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
        return await self._run_public_async(
            request,
            surface="run",
            api_name="KitaruClaudeRunner.run()",
            streaming=False,
        )

    async def run_stream(self, request: ClaudeRunRequest) -> ClaudeRunResult:
        """Run one Claude Agent SDK invocation and forward live stream events."""
        return await self._run_public_async(
            request,
            surface="run_stream",
            api_name="KitaruClaudeRunner.run_stream()",
            streaming=True,
        )

    def run_sync(self, request: ClaudeRunRequest) -> ClaudeRunResult:
        """Run one Claude Agent SDK invocation synchronously."""
        self._reject_running_event_loop(sync_method="run_sync", async_method="run")
        return self._run_public_sync(
            request,
            surface="run_sync",
            api_name="KitaruClaudeRunner.run_sync()",
            streaming=False,
        )

    def run_stream_sync(self, request: ClaudeRunRequest) -> ClaudeRunResult:
        """Run one Claude Agent SDK invocation synchronously with live events."""
        self._reject_running_event_loop(
            sync_method="run_stream_sync",
            async_method="run_stream",
        )
        return self._run_public_sync(
            request,
            surface="run_stream_sync",
            api_name="KitaruClaudeRunner.run_stream_sync()",
            streaming=True,
        )

    async def _run_public_async(
        self,
        request: ClaudeRunRequest,
        *,
        surface: InvocationSurface,
        api_name: str,
        streaming: bool,
    ) -> ClaudeRunResult:
        self._require_invocation_scope(api_name)
        try:
            result = await self._run_invocation_async(request, streaming=streaming)
            result = canonicalize_result_model(result, ClaudeRunResult)
        except Exception:
            self._track_completed(surface, status="failed", result=None)
            raise
        self._track_completed(surface, status="completed", result=result)
        return result

    def _run_public_sync(
        self,
        request: ClaudeRunRequest,
        *,
        surface: InvocationSurface,
        api_name: str,
        streaming: bool,
    ) -> ClaudeRunResult:
        self._require_invocation_scope(api_name)
        try:
            result = self._run_invocation_sync(request, streaming=streaming)
            result = canonicalize_result_model(result, ClaudeRunResult)
        except Exception:
            self._track_completed(surface, status="failed", result=None)
            raise
        self._track_completed(surface, status="completed", result=result)
        return result

    @staticmethod
    def _reject_running_event_loop(*, sync_method: str, async_method: str) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise KitaruUsageError(
            f"`KitaruClaudeRunner.{sync_method}()` cannot be called inside an "
            "already running event loop. Use `await "
            f"KitaruClaudeRunner.{async_method}(...)` instead."
        )

    async def _run_invocation_async(
        self, request: ClaudeRunRequest, *, streaming: bool
    ) -> ClaudeRunResult:
        plan = self._invocation_plan(request, streaming=streaming)
        direct_execution_inside_checkpoint = is_inside_checkpoint()

        async def _body() -> ClaudeRunResult:
            return await self._run_sdk_boundary_async(
                request,
                options=plan.options,
                direct_execution_inside_checkpoint=direct_execution_inside_checkpoint,
                stream_publisher=plan.stream_publisher,
                stream_warnings=plan.stream_warnings,
            )

        if is_inside_flow() and not direct_execution_inside_checkpoint:
            return await run_async_in_checkpoint(
                config=self._invocation_checkpoint_config(),
                step_name=f"{self._name}_claude_invocation",
                body=_body,
                cache_key=self._invocation_cache_key(
                    request,
                    options=plan.options,
                    surface=plan.cache_surface,
                ),
            )
        return await _body()

    def _run_invocation_sync(
        self, request: ClaudeRunRequest, *, streaming: bool
    ) -> ClaudeRunResult:
        plan = self._invocation_plan(request, streaming=streaming)
        direct_execution_inside_checkpoint = is_inside_checkpoint()

        def _body() -> ClaudeRunResult:
            return asyncio.run(
                self._run_sdk_boundary_async(
                    request,
                    options=plan.options,
                    direct_execution_inside_checkpoint=direct_execution_inside_checkpoint,
                    stream_publisher=plan.stream_publisher,
                    stream_warnings=plan.stream_warnings,
                )
            )

        if is_inside_flow() and not direct_execution_inside_checkpoint:
            return run_sync_in_checkpoint(
                config=self._invocation_checkpoint_config(),
                step_name=f"{self._name}_claude_invocation",
                body=_body,
                cache_key=self._invocation_cache_key(
                    request,
                    options=plan.options,
                    surface=plan.cache_surface,
                ),
            )
        return _body()

    def _invocation_plan(
        self, request: ClaudeRunRequest, *, streaming: bool
    ) -> _InvocationPlan:
        if not streaming:
            return _InvocationPlan(
                options=self._build_options(request),
                stream_warnings=[],
                stream_publisher=None,
                cache_surface="run",
            )
        options, stream_warnings = self._build_stream_options(request)
        return _InvocationPlan(
            options=options,
            stream_warnings=stream_warnings,
            stream_publisher=ClaudeStreamPublisher(
                runner_name=self._name,
                include_text_deltas=self._capture.include_stream_text_deltas,
            ),
            cache_surface="stream",
        )

    async def _run_sdk_boundary_async(
        self,
        request: ClaudeRunRequest,
        *,
        options: Any | None,
        direct_execution_inside_checkpoint: bool,
        stream_publisher: ClaudeStreamPublisher | None,
        stream_warnings: list[str],
    ) -> ClaudeRunResult:
        with tracker_scope(self._name, persist_on_exit=False) as tracker:
            manifest: dict[str, Any] | None = None
            terminal_stream_event_sent = False

            def get_manifest() -> dict[str, Any]:
                nonlocal manifest
                if manifest is None:
                    manifest = redacted_options_manifest(
                        options,
                        request,
                        redact=self._capture.redact_options_manifest,
                    )
                return manifest

            def publish_stream_completed(result: ClaudeRunResult) -> None:
                nonlocal terminal_stream_event_sent
                if stream_publisher is None or terminal_stream_event_sent:
                    return
                terminal_stream_event_sent = True
                stream_publisher.completed(status=result.status)

            def publish_stream_failed(error: BaseException) -> None:
                nonlocal terminal_stream_event_sent
                if stream_publisher is None or terminal_stream_event_sent:
                    return
                terminal_stream_event_sent = True
                stream_publisher.failed(error)

            started_at = time.perf_counter()
            try:
                try:
                    if stream_publisher is None:
                        payload = await run_claude_invocation(
                            request=request,
                            options=options,
                            save_transcript_file=self._capture.save_transcript_file,
                        )
                    else:
                        stream_publisher.started()
                        payload = await run_claude_invocation_streamed(
                            request=request,
                            options=options,
                            save_transcript_file=self._capture.save_transcript_file,
                            on_event=stream_publisher.event,
                        )
                    if stream_warnings:
                        payload = dataclasses.replace(
                            payload,
                            warnings=[*payload.warnings, *stream_warnings],
                        )
                except Exception as error:
                    self._record_failed_invocation(
                        tracker,
                        error=error,
                        manifest_factory=(
                            get_manifest
                            if self._capture.save_options_manifest
                            else None
                        ),
                        request=request,
                        duration_ms=elapsed_ms(started_at),
                    )
                    tracker.persist(fail_on_error=False)
                    raise

                result = self._finalize_run_result(
                    payload,
                    tracker=tracker,
                    get_manifest=get_manifest,
                    request=request,
                    direct_execution_inside_checkpoint=(
                        direct_execution_inside_checkpoint
                    ),
                )
                persistence_failures = tracker.persist(fail_on_error=False)
                result = self._apply_event_persistence_failures(
                    result, persistence_failures
                )
                if (
                    persistence_failures
                    and self._capture.fail_on_event_persistence_error
                ):
                    raise self._event_persistence_error(persistence_failures)
                publish_stream_completed(result)
                return result
            except BaseException as error:
                publish_stream_failed(error)
                raise

    def _finalize_run_result(
        self,
        payload: ClaudeInvocationPayload,
        *,
        tracker: EventTracker,
        get_manifest: Callable[[], dict[str, Any]],
        request: ClaudeRunRequest,
        direct_execution_inside_checkpoint: bool,
    ) -> ClaudeRunResult:
        artifacts, capture_failures, capture_warnings = self._persist_capture_artifacts(
            tracker,
            payload=payload,
            get_manifest=get_manifest,
            request=request,
        )
        warnings = [*payload.warnings, *capture_warnings]
        if direct_execution_inside_checkpoint:
            warnings.append(_DIRECT_EXECUTION_WARNING)
        metadata: dict[str, Any] = {
            "run_label": tracker.run_label,
            "direct_execution_inside_checkpoint": direct_execution_inside_checkpoint,
        }
        capture_failure_metadata = [
            failure.as_metadata() for failure in capture_failures
        ]
        if capture_failure_metadata:
            metadata["capture_failures"] = capture_failure_metadata
        usage_record = build_usage_record(
            adapter="claude_agent_sdk",
            surface="agent_invocation",
            call_name=self._name,
            event_id=tracker.run_label,
            record_id=tracker.run_label,
            usage=payload.usage,
            actual_cost_usd=payload.cost_usd,
            cost_source="provider_reported" if payload.cost_usd is not None else "none",
            cost_source_label="claude_agent_sdk.total_cost_usd",
            latency_ms=payload.duration_api_ms or payload.duration_ms,
            status="completed",
            billing_effect="incurred",
            cache_status="executed",
            warnings=warnings,
        )
        log_usage_record(usage_record)
        if self._capture.emit_events:
            tracker.record_invocation(
                status="completed",
                duration_ms=payload.duration_ms,
                session_id=payload.session_id,
                transcript_path=payload.transcript_path,
                warnings=warnings,
                artifacts=artifacts,
                metadata={
                    "sdk_version": claude_agent_sdk_version(),
                    "has_usage": payload.usage is not None,
                    "message_count": len(payload.messages),
                    "direct_execution_inside_checkpoint": (
                        direct_execution_inside_checkpoint
                    ),
                    "capture_failures": capture_failure_metadata,
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
            warnings=warnings,
            metadata=metadata,
        )

    def _persist_capture_artifacts(
        self,
        tracker: EventTracker,
        *,
        payload: ClaudeInvocationPayload,
        get_manifest: Callable[[], dict[str, Any]],
        request: ClaudeRunRequest,
    ) -> tuple[dict[str, str], list[ArtifactCaptureFailure], list[str]]:
        artifacts: dict[str, str] = {}
        failures: list[ArtifactCaptureFailure] = []
        warnings: list[str] = []

        def _maybe_register_artifact(
            key: ArtifactKind,
            *,
            enabled: bool,
            payload_factory: Callable[[], Any],
            type: str,
        ) -> None:
            if not enabled:
                return
            artifact_name = tracker.artifact_name(key)
            if not is_inside_checkpoint():
                artifacts[key] = artifact_name
                return
            try:
                payload_value = payload_factory()
            except Exception as error:
                failure = self._capture_failure(
                    operation="build_artifact_payload",
                    kind=key,
                    artifact_name=artifact_name,
                    error=error,
                )
                if self._capture.fail_on_artifact_capture_error:
                    raise self._artifact_capture_error(failure) from error
                failures.append(failure)
                warnings.append(failure.warning())
                return
            try:
                self._save_artifact(artifact_name, payload_value, type=type)
            except Exception as error:
                failure = self._capture_failure(
                    operation="save_artifact",
                    kind=key,
                    artifact_name=artifact_name,
                    error=error,
                )
                if self._capture.fail_on_artifact_capture_error:
                    raise self._artifact_capture_error(failure) from error
                failures.append(failure)
                warnings.append(failure.warning())
                return
            artifacts[key] = artifact_name

        messages_payload: dict[str, Any] = {"messages": payload.messages}
        if self._capture.save_prompt:
            messages_payload["prompt"] = request.prompt

        _maybe_register_artifact(
            "messages",
            enabled=self._capture.save_messages,
            payload_factory=lambda: messages_payload,
            type="context",
        )
        _maybe_register_artifact(
            "transcript",
            enabled=(
                self._capture.save_transcript_file
                and payload.transcript_payload is not None
            ),
            payload_factory=lambda: payload.transcript_payload,
            type="context",
        )
        _maybe_register_artifact(
            "options_manifest",
            enabled=self._capture.save_options_manifest,
            payload_factory=get_manifest,
            type="context",
        )
        _maybe_register_artifact(
            "output",
            enabled=self._capture.save_final_output,
            payload_factory=lambda: payload.final_text,
            type="response",
        )
        _maybe_register_artifact(
            "usage",
            enabled=self._capture.save_usage
            and (payload.usage is not None or payload.model_usage is not None),
            payload_factory=lambda: {
                "usage": payload.usage,
                "model_usage": payload.model_usage,
                "cost_usd": payload.cost_usd,
            },
            type="context",
        )

        return artifacts, failures, warnings

    def _record_failed_invocation(
        self,
        tracker: EventTracker,
        *,
        error: BaseException,
        manifest_factory: Callable[[], dict[str, Any]] | None,
        request: ClaudeRunRequest,
        duration_ms: float,
    ) -> None:
        artifacts: dict[str, str] = {}
        capture_failures: list[ArtifactCaptureFailure] = []
        warnings: list[str] = []
        if self._capture.save_options_manifest and manifest_factory is not None:
            artifact_name = tracker.artifact_name("options_manifest")
            if is_inside_checkpoint():
                try:
                    manifest = manifest_factory()
                except Exception as capture_error:
                    failure = self._capture_failure(
                        operation="build_artifact_payload",
                        kind="options_manifest",
                        artifact_name=artifact_name,
                        error=capture_error,
                    )
                    capture_failures.append(failure)
                    warnings.append(failure.warning())
                else:
                    try:
                        self._save_artifact(artifact_name, manifest, type="context")
                    except Exception as capture_error:
                        failure = self._capture_failure(
                            operation="save_artifact",
                            kind="options_manifest",
                            artifact_name=artifact_name,
                            error=capture_error,
                        )
                        capture_failures.append(failure)
                        warnings.append(failure.warning())
                    else:
                        artifacts["options_manifest"] = artifact_name
            else:
                artifacts["options_manifest"] = artifact_name
        if self._capture.emit_events:
            tracker.record_invocation(
                status="failed",
                duration_ms=duration_ms,
                artifacts=artifacts,
                warnings=warnings,
                metadata={
                    "sdk_version": claude_agent_sdk_version(),
                    "request_kind": request.kind,
                    "capture_failures": [
                        failure.as_metadata() for failure in capture_failures
                    ],
                },
                error=error,
            )

    @staticmethod
    def _capture_failure(
        *,
        operation: ArtifactCaptureOperation,
        kind: ArtifactKind,
        artifact_name: str,
        error: BaseException,
    ) -> ArtifactCaptureFailure:
        return ArtifactCaptureFailure.from_exception(
            operation=operation,
            kind=kind,
            artifact_name=artifact_name,
            error=error,
        )

    @staticmethod
    def _artifact_capture_error(failure: ArtifactCaptureFailure) -> KitaruRuntimeError:
        return failure.as_error()

    @staticmethod
    def _event_persistence_warning(failure: EventPersistenceFailure) -> str:
        target = (
            f" artifact {failure.artifact_name}"
            if failure.artifact_name is not None
            else ""
        )
        return (
            "Claude event/log persistence failed for "
            f"{failure.operation}{target}: "
            f"{failure.exception_type}: {failure.message}"
        )

    @classmethod
    def _event_persistence_error(
        cls, failures: list[EventPersistenceFailure]
    ) -> KitaruRuntimeError:
        failure_summary = "; ".join(
            cls._event_persistence_warning(failure) for failure in failures
        )
        return KitaruRuntimeError(failure_summary)

    @classmethod
    def _apply_event_persistence_failures(
        cls,
        result: ClaudeRunResult,
        failures: list[EventPersistenceFailure],
    ) -> ClaudeRunResult:
        if not failures:
            return result
        failure_metadata = [failure.as_metadata() for failure in failures]
        metadata = {
            **result.metadata,
            "event_persistence_failures": failure_metadata,
        }
        updates: dict[str, Any] = {
            "warnings": [
                *result.warnings,
                *(cls._event_persistence_warning(failure) for failure in failures),
            ],
            "metadata": metadata,
        }
        if any(failure.operation == "save_event_log" for failure in failures):
            updates["event_log_artifact_name"] = None
        if any(failure.operation == "save_run_summary" for failure in failures):
            updates["run_summary_artifact_name"] = None
        return result.model_copy(update=updates)

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

    def _build_stream_options(
        self, request: ClaudeRunRequest
    ) -> tuple[Any | None, list[str]]:
        options = self._build_options(request)
        if options is None:
            return self._stream_options_from_request(request)
        prepared_options, warning = self._copy_options_with_partial_messages(options)
        return prepared_options, [warning] if warning is not None else []

    @staticmethod
    def _stream_options_from_request(
        request: ClaudeRunRequest,
    ) -> tuple[Any | None, list[str]]:
        ClaudeAgentOptions = KitaruClaudeRunner._claude_agent_options_type(
            "Claude streaming requires Claude Agent SDK options support so "
            "Kitaru can request partial stream messages. Pass an "
            "`options_factory` if your installed SDK exposes a different "
            "options type."
        )
        try:
            return (
                ClaudeAgentOptions(
                    **KitaruClaudeRunner._request_options_kwargs(
                        request, include_partial_messages=True
                    )
                ),
                [],
            )
        except TypeError:
            warning = _partial_messages_unavailable_warning()
            if not KitaruClaudeRunner._request_needs_sdk_options(request):
                return None, [warning]
            return (
                ClaudeAgentOptions(
                    **KitaruClaudeRunner._request_options_kwargs(request)
                ),
                [warning],
            )

    @staticmethod
    def _copy_options_with_partial_messages(options: Any) -> tuple[Any, str | None]:
        warning = _partial_messages_unavailable_warning()
        if isinstance(options, Mapping):
            try:
                copied_mapping = dict(options)
                copied_mapping["include_partial_messages"] = True
            except Exception:
                pass
            else:
                return copied_mapping, None

        model_copy = getattr(options, "model_copy", None)
        if callable(model_copy):
            try:
                copied_model = model_copy(update={"include_partial_messages": True})
            except Exception:
                pass
            else:
                if getattr(copied_model, "include_partial_messages", None) is True:
                    return copied_model, None

        if dataclasses.is_dataclass(options) and not isinstance(options, type):
            try:
                copied_dataclass = dataclasses.replace(
                    options, include_partial_messages=True
                )
            except Exception:
                pass
            else:
                if getattr(copied_dataclass, "include_partial_messages", None) is True:
                    return copied_dataclass, None

        try:
            copied = copy.copy(options)
            if copied is not options:
                copied.include_partial_messages = True
                if getattr(copied, "include_partial_messages", None) is True:
                    return copied, None
        except Exception:
            pass

        return options, warning

    @staticmethod
    def _request_needs_sdk_options(request: ClaudeRunRequest) -> bool:
        return any(
            value is not None
            for value in (request.cwd, request.max_turns, request.resume_session_id)
        )

    @staticmethod
    def _request_options_kwargs(
        request: ClaudeRunRequest,
        *,
        include_partial_messages: bool | None = None,
    ) -> dict[str, Any]:
        options_kwargs: dict[str, Any] = {
            "cwd": request.cwd,
            "resume": request.resume_session_id,
            "max_turns": request.max_turns,
        }
        if include_partial_messages is not None:
            options_kwargs["include_partial_messages"] = include_partial_messages
        return options_kwargs

    @staticmethod
    def _claude_agent_options_type(error_message: str) -> Any:
        try:
            from claude_agent_sdk import ClaudeAgentOptions
        except (ImportError, AttributeError) as exc:
            raise KitaruUsageError(error_message) from exc
        return ClaudeAgentOptions

    @staticmethod
    def _options_from_request(request: ClaudeRunRequest) -> Any:
        ClaudeAgentOptions = KitaruClaudeRunner._claude_agent_options_type(
            "ClaudeRunRequest fields `cwd`, `max_turns`, and "
            "`resume_session_id` require Claude Agent SDK options support. "
            "Pass an `options_factory` if your installed SDK exposes a "
            "different options type."
        )
        return ClaudeAgentOptions(**KitaruClaudeRunner._request_options_kwargs(request))

    def _invocation_checkpoint_config(self) -> CheckpointConfig:
        return {
            **self._checkpoint_config,
            "type": self._checkpoint_config.get("type", "agent_call"),
        }

    def _invocation_cache_key(
        self,
        request: ClaudeRunRequest,
        *,
        options: Any | None,
        surface: Literal["run", "stream"] = "run",
    ) -> str:
        payload = {
            "adapter": "claude_agent_sdk",
            "checkpoint_strategy": "invocation",
            "claude_agent_sdk_version": claude_agent_sdk_version(),
            "runner_name": self._name,
            "request": request.model_dump(mode="json"),
            "options": to_cache_identity(options),
        }
        if surface != "run":
            payload["surface"] = surface
        if surface == "stream":
            payload["stream"] = {
                "include_stream_text_deltas": self._capture.include_stream_text_deltas,
            }
        return checkpoint_cache_key(payload)

    def _require_invocation_scope(self, api_name: str) -> None:
        if is_inside_checkpoint():
            if self._allow_direct_execution_inside_checkpoint:
                return
            raise KitaruUsageError(
                f"{api_name} was called from inside an existing Kitaru "
                "checkpoint. By default the Claude Agent SDK adapter refuses "
                "this because it cannot create its own invocation checkpoint "
                "inside another checkpoint. Move the call to the flow body, or "
                "set allow_direct_execution_inside_checkpoint=True to run "
                "Claude directly inside the existing checkpoint and accept "
                "that replaying the outer checkpoint can call Claude again."
            )
        if is_inside_flow():
            return
        raise KitaruUsageError(
            f"{api_name} must be called inside a Kitaru flow body so the "
            "adapter can create one checkpoint around the Claude invocation."
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
