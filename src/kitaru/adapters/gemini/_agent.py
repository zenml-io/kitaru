"""Public runner wrapper for the Gemini Interactions adapter."""

import asyncio
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Literal, cast

from kitaru.adapters._result_identity import canonicalize_result_model
from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruRuntimeError, KitaruUsageError

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._policy import GeminiInteractionCapturePolicy
from ._runner import (
    GeminiInteractionPayload,
    google_genai_version,
    run_gemini_interaction,
    run_gemini_interaction_streamed,
)
from ._serialization import redacted_request_manifest
from ._stream_shapes import _STREAM_RECONSTRUCTION_POLICY
from ._streaming import GeminiStreamPublisher
from ._tracking import (
    ArtifactKind,
    ArtifactNameByKind,
    EventPersistenceFailure,
    EventTracker,
    tracker_scope,
)
from ._types import GeminiInteractionRequest, GeminiInteractionResult
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
    "Gemini Interactions ran directly inside an existing Kitaru checkpoint "
    "because allow_direct_execution_inside_checkpoint=True. Replaying the outer "
    "checkpoint can call Google again and duplicate Gemini-side tool calls, "
    "managed-agent work, sandbox mutations, or API cost."
)

ArtifactCaptureOperation = Literal["build_artifact_payload", "save_artifact"]
InteractionSurface = Literal["run", "run_sync", "run_stream", "run_stream_sync"]


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
            "Gemini artifact capture failed for "
            f"{self.kind} artifact {self.artifact_name}: "
            f"{self.exception_type}: {self.message}"
        )

    def as_error(self) -> KitaruRuntimeError:
        return KitaruRuntimeError(
            "Gemini Interactions artifact capture failed for "
            f"{self.kind} artifact {self.artifact_name}: "
            f"{self.exception_type}: {self.message}. Gemini already succeeded; "
            "retrying may duplicate the interaction."
        )


def _request_descriptor(request: GeminiInteractionRequest) -> dict[str, Any]:
    return {
        "request_kind": request.kind,
        "target_kind": request.target_kind,
        "background": request.background,
        "store": request.store,
        "has_environment": request.environment is not None,
    }


def _result_descriptor(
    result: GeminiInteractionResult | None,
) -> dict[str, Any]:
    if result is None:
        return {
            "response_status": None,
            "has_usage": False,
            "step_count": 0,
            "requires_action": False,
        }
    return {
        "response_status": result.status,
        "has_usage": result.usage is not None,
        "step_count": len(result.steps),
        "requires_action": result.status == "requires_action",
    }


@dataclass(frozen=True)
class _PublicInteractionPlan:
    surface: InteractionSurface

    @property
    def api_name(self) -> str:
        return f"KitaruGeminiInteractionsRunner.{self.surface}()"

    @property
    def streaming(self) -> bool:
        return self.surface in {"run_stream", "run_stream_sync"}


@dataclass(frozen=True)
class _InteractionPlan:
    surface: InteractionSurface
    stream_publisher: GeminiStreamPublisher | None = None
    stream_cache_identity: dict[str, Any] | None = None

    @property
    def streaming(self) -> bool:
        return self.stream_publisher is not None

    @property
    def cache_surface(self) -> str:
        return "stream" if self.streaming else "run"


class KitaruGeminiInteractionsRunner:
    """Wrap one stable Gemini interaction response in one Kitaru checkpoint.

    Gemini manages Interactions state, hosted agents, tools, and managed
    sandboxes. Kitaru records the outer durable workflow boundary: one stable
    Interactions API response, usually ``completed`` and sometimes
    ``requires_action``.
    """

    def __init__(
        self,
        *,
        name: str,
        client: Any | None = None,
        client_factory: Callable[[], Any] | None = None,
        checkpoint_strategy: CheckpointStrategy = "interaction",
        capture: GeminiInteractionCapturePolicy | None = None,
        checkpoint_config: CheckpointConfig | None = None,
        cache_identity: str | None = None,
        allow_direct_execution_inside_checkpoint: bool = False,
        poll_interval_s: float = 2.0,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise KitaruUsageError(
                "KitaruGeminiInteractionsRunner requires a stable `name`."
            )
        if client is not None and client_factory is not None:
            raise KitaruUsageError(
                "`client` and `client_factory` are mutually exclusive."
            )
        if not isinstance(allow_direct_execution_inside_checkpoint, bool):
            raise KitaruUsageError(
                "`allow_direct_execution_inside_checkpoint` must be a boolean."
            )
        if poll_interval_s <= 0:
            raise KitaruUsageError("`poll_interval_s` must be positive.")
        if cache_identity is not None and not isinstance(cache_identity, str):
            raise KitaruUsageError("`cache_identity` must be a stable string.")

        self._name = name
        self._client = client
        self._client_factory = client_factory
        self._checkpoint_strategy = validate_checkpoint_strategy(checkpoint_strategy)
        self._capture = capture or GeminiInteractionCapturePolicy()
        self._cache_identity = cache_identity
        self._allow_direct_execution_inside_checkpoint = (
            allow_direct_execution_inside_checkpoint
        )
        self._poll_interval_s = poll_interval_s
        self._checkpoint_config: CheckpointConfig = validate_checkpoint_config(
            checkpoint_config,
            context="checkpoint_config",
        ) or cast(CheckpointConfig, {})

        track(
            AnalyticsEvent.GEMINI_INTERACTIONS_WRAPPED,
            {
                "checkpoint_strategy": self._checkpoint_strategy,
                "has_client": client is not None,
                "has_client_factory": client_factory is not None,
                "has_cache_identity": cache_identity is not None,
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
    def capture(self) -> GeminiInteractionCapturePolicy:
        return self._capture

    async def run(
        self,
        request: GeminiInteractionRequest,
    ) -> GeminiInteractionResult:
        """Run one Gemini interaction asynchronously."""
        return await self._run_public_async(
            request,
            _PublicInteractionPlan(surface="run"),
        )

    async def run_stream(
        self,
        request: GeminiInteractionRequest,
    ) -> GeminiInteractionResult:
        """Run one Gemini interaction asynchronously with live stream events."""
        return await self._run_public_async(
            request,
            _PublicInteractionPlan(surface="run_stream"),
        )

    def run_sync(
        self,
        request: GeminiInteractionRequest,
    ) -> GeminiInteractionResult:
        """Run one Gemini interaction synchronously."""
        self._reject_running_event_loop(sync_method="run_sync", async_method="run")
        return self._run_public_sync(
            request,
            _PublicInteractionPlan(surface="run_sync"),
        )

    def run_stream_sync(
        self,
        request: GeminiInteractionRequest,
    ) -> GeminiInteractionResult:
        """Run one Gemini interaction synchronously with live stream events."""
        self._reject_running_event_loop(
            sync_method="run_stream_sync",
            async_method="run_stream",
        )
        return self._run_public_sync(
            request,
            _PublicInteractionPlan(surface="run_stream_sync"),
        )

    async def _run_public_async(
        self,
        request: GeminiInteractionRequest,
        plan: _PublicInteractionPlan,
    ) -> GeminiInteractionResult:
        self._require_invocation_scope(plan.api_name)
        try:
            result = await self._run_interaction_async(request, public_plan=plan)
            result = canonicalize_result_model(result, GeminiInteractionResult)
        except Exception:
            self._track_completed(
                plan.surface, status="failed", request=request, result=None
            )
            raise
        self._track_completed(
            plan.surface, status="completed", request=request, result=result
        )
        return result

    def _run_public_sync(
        self,
        request: GeminiInteractionRequest,
        plan: _PublicInteractionPlan,
    ) -> GeminiInteractionResult:
        self._require_invocation_scope(plan.api_name)
        try:
            result = self._run_interaction_sync(request, public_plan=plan)
            result = canonicalize_result_model(result, GeminiInteractionResult)
        except Exception:
            self._track_completed(
                plan.surface, status="failed", request=request, result=None
            )
            raise
        self._track_completed(
            plan.surface, status="completed", request=request, result=result
        )
        return result

    @staticmethod
    def _reject_running_event_loop(*, sync_method: str, async_method: str) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise KitaruUsageError(
            f"`KitaruGeminiInteractionsRunner.{sync_method}()` cannot be called "
            "inside an already running event loop. Use "
            f"`await KitaruGeminiInteractionsRunner.{async_method}(...)` instead."
        )

    async def _run_interaction_async(
        self,
        request: GeminiInteractionRequest,
        *,
        public_plan: _PublicInteractionPlan,
    ) -> GeminiInteractionResult:
        plan = self._interaction_plan(public_plan)
        direct_execution_inside_checkpoint = is_inside_checkpoint()

        async def _body() -> GeminiInteractionResult:
            return await self._run_sdk_async(
                request,
                direct_execution_inside_checkpoint=direct_execution_inside_checkpoint,
                stream_publisher=plan.stream_publisher,
                allow_sync_stream=False,
            )

        if is_inside_flow() and not direct_execution_inside_checkpoint:
            return await run_async_in_checkpoint(
                config=self._interaction_checkpoint_config(),
                step_name=f"{self._name}_gemini_interaction",
                body=_body,
                cache_key=self._interaction_cache_key(request, plan=plan),
            )
        return await _body()

    def _run_interaction_sync(
        self,
        request: GeminiInteractionRequest,
        *,
        public_plan: _PublicInteractionPlan,
    ) -> GeminiInteractionResult:
        plan = self._interaction_plan(public_plan)
        direct_execution_inside_checkpoint = is_inside_checkpoint()

        def _body() -> GeminiInteractionResult:
            return asyncio.run(
                self._run_sdk_async(
                    request,
                    direct_execution_inside_checkpoint=direct_execution_inside_checkpoint,
                    stream_publisher=plan.stream_publisher,
                    allow_sync_stream=True,
                )
            )

        if is_inside_flow() and not direct_execution_inside_checkpoint:
            return run_sync_in_checkpoint(
                config=self._interaction_checkpoint_config(),
                step_name=f"{self._name}_gemini_interaction",
                body=_body,
                cache_key=self._interaction_cache_key(request, plan=plan),
            )
        return _body()

    def _interaction_plan(
        self, public_plan: _PublicInteractionPlan
    ) -> _InteractionPlan:
        if not public_plan.streaming:
            return _InteractionPlan(surface=public_plan.surface)
        stream_cache_identity = {
            "include_stream_text_deltas": self._capture.include_stream_text_deltas,
            "final_result_reconstruction": _STREAM_RECONSTRUCTION_POLICY,
        }
        return _InteractionPlan(
            surface=public_plan.surface,
            stream_publisher=GeminiStreamPublisher(
                runner_name=self._name,
                surface=public_plan.surface,
                include_text_deltas=self._capture.include_stream_text_deltas,
            ),
            stream_cache_identity=stream_cache_identity,
        )

    async def _run_sdk_async(
        self,
        request: GeminiInteractionRequest,
        *,
        direct_execution_inside_checkpoint: bool,
        stream_publisher: GeminiStreamPublisher | None,
        allow_sync_stream: bool,
    ) -> GeminiInteractionResult:
        with tracker_scope(self._name, persist_on_exit=False) as tracker:
            manifest: dict[str, Any] | None = None
            terminal_stream_event_sent = False

            def get_manifest() -> dict[str, Any]:
                nonlocal manifest
                if manifest is None:
                    manifest = redacted_request_manifest(
                        request,
                        client=self._client,
                        redact=self._capture.redact_request_manifest,
                    )
                return manifest

            def publish_stream_completed(result: GeminiInteractionResult) -> None:
                nonlocal terminal_stream_event_sent
                if stream_publisher is None or terminal_stream_event_sent:
                    return
                terminal_stream_event_sent = True
                self._publish_stream_lifecycle(
                    lambda: stream_publisher.completed(
                        status=result.status,
                        interaction_id=result.interaction_id,
                    )
                )

            def publish_stream_failed(error: BaseException) -> None:
                nonlocal terminal_stream_event_sent
                if stream_publisher is None or terminal_stream_event_sent:
                    return
                terminal_stream_event_sent = True
                self._publish_stream_lifecycle(lambda: stream_publisher.failed(error))

            started_at = time.perf_counter()
            try:
                try:
                    payload = await self._execute_sdk_payload(
                        request,
                        stream_publisher=stream_publisher,
                        allow_sync_stream=allow_sync_stream,
                    )
                except Exception as error:
                    self._record_failed_interaction(
                        tracker,
                        error=error,
                        manifest_factory=(
                            get_manifest
                            if self._capture.save_request_manifest
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
                    direct_execution_inside_checkpoint=direct_execution_inside_checkpoint,
                )
                persistence_failures = tracker.persist(fail_on_error=False)
                result = self._apply_event_persistence_failures(
                    result,
                    persistence_failures,
                )
                if (
                    persistence_failures
                    and self._capture.fail_on_event_persistence_error
                ):
                    raise EventPersistenceFailure.as_error(persistence_failures)
                publish_stream_completed(result)
                return result
            except BaseException as error:
                publish_stream_failed(error)
                raise

    async def _execute_sdk_payload(
        self,
        request: GeminiInteractionRequest,
        *,
        stream_publisher: GeminiStreamPublisher | None,
        allow_sync_stream: bool,
    ) -> GeminiInteractionPayload:
        if stream_publisher is None:
            return await run_gemini_interaction(
                request=request,
                client=self._client,
                client_factory=self._client_factory,
                poll_interval_s=self._poll_interval_s,
            )
        self._publish_stream_lifecycle(stream_publisher.started)
        return await run_gemini_interaction_streamed(
            request=request,
            client=self._client,
            client_factory=self._client_factory,
            on_event=stream_publisher.event,
            allow_sync_stream=allow_sync_stream,
            poll_interval_s=self._poll_interval_s,
        )

    @staticmethod
    def _publish_stream_lifecycle(publish: Callable[[], Any]) -> None:
        with suppress(Exception):
            publish()

    def _finalize_run_result(
        self,
        payload: GeminiInteractionPayload,
        *,
        tracker: EventTracker,
        get_manifest: Callable[[], dict[str, Any]],
        request: GeminiInteractionRequest,
        direct_execution_inside_checkpoint: bool,
    ) -> GeminiInteractionResult:
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
        if payload.stream_metadata is not None:
            metadata["stream"] = payload.stream_metadata
        capture_failure_metadata = [
            failure.as_metadata() for failure in capture_failures
        ]
        if capture_failure_metadata:
            metadata["capture_failures"] = capture_failure_metadata
        result = GeminiInteractionResult(
            status=payload.status,
            interaction_id=payload.interaction_id,
            previous_interaction_id=payload.previous_interaction_id,
            output_text=payload.output_text,
            model=payload.model,
            agent=payload.agent,
            environment_id=payload.environment_id,
            steps=payload.steps,
            usage=payload.usage,
            duration_ms=payload.duration_ms,
            poll_count=payload.poll_count,
            sdk_version=payload.sdk_version,
            input_artifact_name=artifacts.get("input"),
            request_manifest_artifact_name=artifacts.get("request_manifest"),
            raw_interaction_artifact_name=artifacts.get("raw_interaction"),
            steps_artifact_name=artifacts.get("steps"),
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
        if self._capture.emit_events:
            tracker.record_interaction(
                status="completed",
                duration_ms=payload.duration_ms,
                interaction_id=payload.interaction_id,
                warnings=warnings,
                artifacts=artifacts,
                metadata={
                    "sdk_version": payload.sdk_version,
                    **_request_descriptor(request),
                    **_result_descriptor(result),
                    "capture_failures": capture_failure_metadata,
                },
            )
        return result

    def _persist_capture_artifacts(
        self,
        tracker: EventTracker,
        *,
        payload: GeminiInteractionPayload,
        get_manifest: Callable[[], dict[str, Any]],
        request: GeminiInteractionRequest,
    ) -> tuple[ArtifactNameByKind, list[ArtifactCaptureFailure], list[str]]:
        artifacts: ArtifactNameByKind = {}
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
                failure = ArtifactCaptureFailure.from_exception(
                    operation="build_artifact_payload",
                    kind=key,
                    artifact_name=artifact_name,
                    error=error,
                )
                if self._capture.fail_on_artifact_capture_error:
                    raise failure.as_error() from error
                failures.append(failure)
                warnings.append(failure.warning())
                return
            try:
                self._save_artifact(artifact_name, payload_value, type=type)
            except Exception as error:
                failure = ArtifactCaptureFailure.from_exception(
                    operation="save_artifact",
                    kind=key,
                    artifact_name=artifact_name,
                    error=error,
                )
                if self._capture.fail_on_artifact_capture_error:
                    raise failure.as_error() from error
                failures.append(failure)
                warnings.append(failure.warning())
                return
            artifacts[key] = artifact_name

        _maybe_register_artifact(
            "input",
            enabled=self._capture.save_input and request.input is not None,
            payload_factory=lambda: request.input,
            type="context",
        )
        _maybe_register_artifact(
            "request_manifest",
            enabled=self._capture.save_request_manifest,
            payload_factory=get_manifest,
            type="context",
        )
        _maybe_register_artifact(
            "raw_interaction",
            enabled=self._capture.save_raw_interaction,
            payload_factory=lambda: payload.raw_interaction,
            type="context",
        )
        _maybe_register_artifact(
            "steps",
            enabled=self._capture.save_steps,
            payload_factory=lambda: payload.raw_steps,
            type="context",
        )
        _maybe_register_artifact(
            "output",
            enabled=self._capture.save_output and payload.output_text is not None,
            payload_factory=lambda: payload.output_text,
            type="response",
        )
        _maybe_register_artifact(
            "usage",
            enabled=self._capture.save_usage and payload.usage is not None,
            payload_factory=lambda: payload.usage,
            type="context",
        )
        return artifacts, failures, warnings

    def _record_failed_interaction(
        self,
        tracker: EventTracker,
        *,
        error: BaseException,
        manifest_factory: Callable[[], dict[str, Any]] | None,
        request: GeminiInteractionRequest,
        duration_ms: float,
    ) -> None:
        artifacts: ArtifactNameByKind = {}
        if self._capture.save_request_manifest and manifest_factory is not None:
            artifact_name = tracker.artifact_name("request_manifest")
            if is_inside_checkpoint():
                with suppress(Exception):
                    self._save_artifact(
                        artifact_name, manifest_factory(), type="context"
                    )
            artifacts["request_manifest"] = artifact_name
        if self._capture.emit_events:
            tracker.record_interaction(
                status="failed",
                duration_ms=duration_ms,
                artifacts=artifacts,
                metadata={
                    "sdk_version": google_genai_version(),
                    **_request_descriptor(request),
                },
                error=error,
            )

    @staticmethod
    def _apply_event_persistence_failures(
        result: GeminiInteractionResult,
        failures: list[EventPersistenceFailure],
    ) -> GeminiInteractionResult:
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
                *(failure.warning() for failure in failures),
            ],
            "metadata": metadata,
        }
        if any(failure.operation == "save_event_log" for failure in failures):
            updates["event_log_artifact_name"] = None
        if any(failure.operation == "save_run_summary" for failure in failures):
            updates["run_summary_artifact_name"] = None
        return result.model_copy(update=updates)

    def _interaction_checkpoint_config(self) -> CheckpointConfig:
        return {
            **self._checkpoint_config,
            "type": self._checkpoint_config.get("type", "agent_call"),
        }

    def _interaction_cache_key(
        self,
        request: GeminiInteractionRequest,
        *,
        plan: _InteractionPlan | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "adapter": "gemini_interactions",
            "checkpoint_strategy": "interaction",
            "google_genai_version": google_genai_version(),
            "runner_name": self._name,
            "request": request.model_dump(mode="json"),
            "cache_identity": self._cache_identity,
        }
        if plan is not None and plan.streaming:
            payload.update(
                {
                    "surface": plan.cache_surface,
                    "streaming": True,
                    "stream_cache_identity": plan.stream_cache_identity,
                }
            )
        return checkpoint_cache_key(payload)

    def _require_invocation_scope(self, api_name: str) -> None:
        if is_inside_checkpoint():
            if self._allow_direct_execution_inside_checkpoint:
                return
            raise KitaruUsageError(
                f"{api_name} was called from inside an existing Kitaru checkpoint. "
                "By default the Gemini Interactions adapter refuses this because "
                "it cannot create its own interaction checkpoint inside another "
                "checkpoint. Move the call to the flow body, or set "
                "allow_direct_execution_inside_checkpoint=True to run Gemini "
                "directly inside the existing checkpoint and accept that replaying "
                "the outer checkpoint can call Google again."
            )
        if is_inside_flow():
            return
        raise KitaruUsageError(
            f"{api_name} must be called inside a Kitaru flow body so the adapter "
            "can create one checkpoint around the Gemini interaction."
        )

    def _track_completed(
        self,
        surface: str,
        *,
        status: str,
        request: GeminiInteractionRequest,
        result: GeminiInteractionResult | None,
    ) -> None:
        track(
            AnalyticsEvent.GEMINI_INTERACTIONS_RUN_COMPLETED,
            {
                "surface": surface,
                "checkpoint_strategy": self._checkpoint_strategy,
                "status": status,
                **_request_descriptor(request),
                **_result_descriptor(result),
            },
        )

    @staticmethod
    def _save_artifact(name: str, value: Any, *, type: str) -> None:
        import kitaru

        kitaru.save(name, value, type=type)
