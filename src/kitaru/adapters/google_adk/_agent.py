"""Google ADK runner wrapper for Kitaru durable execution."""

from __future__ import annotations

import importlib.metadata
import inspect
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, cast

from kitaru._llm_usage import build_usage_record, log_usage_record
from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from . import _kitaru_internal as runtime
from ._constants import ADAPTER_ID, ADAPTER_VERSION, DEFAULT_RUNNER_CHECKPOINT_TYPE
from ._events import dump_adk_events
from ._hitl import extract_handoff_requests, has_handoff_markers
from ._outputs import extract_final_output
from ._policy import (
    ADKCallCheckpointPolicy,
    ADKCapturePolicy,
    resolve_summary_checkpoint_config,
)
from ._serialization import object_metadata, to_json_safe
from ._tracking import EventTracker, tracker_scope
from ._types import ADKRunRequest, ADKRunResult, ADKUsageSummary
from ._usage import usage_from_event
from ._utils import (
    CheckpointConfig,
    CheckpointStrategy,
    checkpoint_cache_key,
    elapsed_ms,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
    validate_checkpoint_config,
    validate_checkpoint_strategy,
    with_default_type,
)


def _google_adk_version() -> str:
    try:
        return importlib.metadata.version("google-adk")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


class KitaruADKRunner:
    """Durable wrapper around a Google ADK runner.

    ``checkpoint_strategy='runner_call'`` opens one checkpoint around a whole
    ADK runner turn. ``checkpoint_strategy='calls'`` runs the ADK runner
    directly while installing Kitaru's tracker context. Only explicit
    ``KitaruADKModel`` and ``KitaruADKTool`` objects can create per-call
    checkpoints in that mode; arbitrary ADK internals are not mutated or
    checkpointed.
    """

    def __init__(
        self,
        runner: Any,
        *,
        name: str | None = None,
        checkpoint_strategy: CheckpointStrategy = "runner_call",
        capture: ADKCapturePolicy | None = None,
        run_checkpoint_config: CheckpointConfig | None = None,
        call_checkpoint_policy: ADKCallCheckpointPolicy | None = None,
        cost_calculator: Callable[[ADKUsageSummary], float | None] | None = None,
    ) -> None:
        self._runner = runner
        self._name = self._resolve_name(runner, explicit=name)
        self._checkpoint_strategy = validate_checkpoint_strategy(checkpoint_strategy)
        self._capture = capture or ADKCapturePolicy()
        self._run_checkpoint_config = validate_checkpoint_config(
            run_checkpoint_config,
            context="run_checkpoint_config",
        )
        self._call_checkpoint_policy = (
            call_checkpoint_policy or ADKCallCheckpointPolicy()
        )
        self._cost_calculator = cost_calculator
        track(
            AnalyticsEvent.GOOGLE_ADK_WRAPPED,
            {"checkpoint_strategy": self._checkpoint_strategy},
        )

    @property
    def name(self) -> str:
        return self._name

    async def run(self, request: ADKRunRequest) -> ADKRunResult:
        """Run one ADK turn asynchronously."""
        request = ADKRunRequest.model_validate(request)
        if self._checkpoint_strategy == "runner_call":
            result = await self._run_runner_call_async(request)
        else:
            result = await self._run_calls_async(request)
        self._track_run_completed(result)
        return result

    def run_sync(self, request: ADKRunRequest) -> ADKRunResult:
        """Run one ADK turn synchronously."""
        request = ADKRunRequest.model_validate(request)
        if self._checkpoint_strategy == "runner_call":
            result = self._run_runner_call_sync(request)
        else:
            result = self._run_calls_sync(request)
        self._track_run_completed(result)
        return result

    @staticmethod
    def _resolve_name(runner: Any, *, explicit: str | None) -> str:
        for value in (
            explicit,
            getattr(runner, "name", None),
            getattr(runner, "app_name", None),
        ):
            if isinstance(value, str) and value.strip():
                return value.strip()
        raise KitaruUsageError(
            "Google ADK runner name could not be resolved. Pass `name=...`, or "
            "use a runner with a non-empty `.name` or `.app_name` attribute."
        )

    def _runner_call_checkpoint_config(self) -> CheckpointConfig:
        config = cast(CheckpointConfig, self._run_checkpoint_config or {})
        return with_default_type(config, DEFAULT_RUNNER_CHECKPOINT_TYPE)

    def _runner_input_envelope(self, request: ADKRunRequest) -> dict[str, Any]:
        return {
            "adapter": ADAPTER_ID,
            "adapter_version": ADAPTER_VERSION,
            "google_adk_version": _google_adk_version(),
            "runner": {"name": self._name, **object_metadata(self._runner)},
            "request": request.model_dump(mode="python"),
            "capture": self._capture.model_dump(mode="python"),
        }

    async def _run_runner_call_async(self, request: ADKRunRequest) -> ADKRunResult:
        async def body() -> ADKRunResult:
            return await self._invoke_runner_async(request, tracker=None)

        envelope = to_json_safe(self._runner_input_envelope(request))
        if runtime.is_inside_flow() and not runtime.is_inside_checkpoint():
            return await run_async_in_checkpoint(
                config=self._runner_call_checkpoint_config(),
                step_name=f"{self._name}_google_adk_runner_call",
                body=body,
                cache_key=checkpoint_cache_key(envelope),
                checkpoint_inputs={"adk_input": envelope},
            )
        return await body()

    def _run_runner_call_sync(self, request: ADKRunRequest) -> ADKRunResult:
        def body() -> ADKRunResult:
            return self._invoke_runner_sync(request, tracker=None)

        envelope = to_json_safe(self._runner_input_envelope(request))
        if runtime.is_inside_flow() and not runtime.is_inside_checkpoint():
            return run_sync_in_checkpoint(
                config=self._runner_call_checkpoint_config(),
                step_name=f"{self._name}_google_adk_runner_call",
                body=body,
                cache_key=checkpoint_cache_key(envelope),
                checkpoint_inputs={"adk_input": envelope},
            )
        return body()

    async def _run_calls_async(self, request: ADKRunRequest) -> ADKRunResult:
        self._validate_explicit_wrapper_calls_mode_policy()
        tracker = EventTracker(self._name)
        with tracker_scope(
            tracker,
            capture=self._capture,
            call_policy=self._call_checkpoint_policy,
        ):
            result = await self._invoke_runner_async(request, tracker=tracker)
        return await self._persist_calls_summary_async(result, tracker=tracker)

    def _run_calls_sync(self, request: ADKRunRequest) -> ADKRunResult:
        self._validate_explicit_wrapper_calls_mode_policy()
        tracker = EventTracker(self._name)
        with tracker_scope(
            tracker,
            capture=self._capture,
            call_policy=self._call_checkpoint_policy,
        ):
            result = self._invoke_runner_sync(request, tracker=tracker)
        return self._persist_calls_summary_sync(result, tracker=tracker)

    def _validate_explicit_wrapper_calls_mode_policy(self) -> None:
        if (
            runtime.is_inside_checkpoint()
            and self._call_checkpoint_policy.nested_checkpoint_policy == "error"
        ):
            raise KitaruUsageError(
                "Google ADK calls mode cannot run from inside an existing Kitaru "
                "checkpoint unless ADKCallCheckpointPolicy("
                "nested_checkpoint_policy='metadata_only') is set."
            )

    async def _invoke_runner_async(
        self,
        request: ADKRunRequest,
        *,
        tracker: EventTracker | None,
    ) -> ADKRunResult:
        run_async = getattr(self._runner, "run_async", None)
        if not callable(run_async):
            raise KitaruUsageError(
                "Google ADK runner does not expose `run_async(...)`; use "
                "run_sync(...)` with a synchronous runner or pass a runner with "
                "an async ADK API."
            )
        started_at = time.perf_counter()
        try:
            raw_result = run_async(
                user_id=request.user_id,
                session_id=request.session_id,
                new_message=request.message,
                **request.run_kwargs,
            )
            collected = await self._collect_async_result(raw_result)
            return self._build_result(collected, tracker=tracker, started_at=started_at)
        except Exception as exc:
            if tracker is not None:
                tracker.mark_failed(exc)
            raise

    def _invoke_runner_sync(
        self,
        request: ADKRunRequest,
        *,
        tracker: EventTracker | None,
    ) -> ADKRunResult:
        run = getattr(self._runner, "run", None)
        if not callable(run):
            if callable(getattr(self._runner, "run_async", None)):
                raise KitaruUsageError(
                    "Google ADK runner only exposes `run_async(...)`; call "
                    "`await KitaruADKRunner.run(...)` instead of `run_sync(...)`."
                )
            raise KitaruUsageError("Google ADK runner does not expose `run(...)`.")
        started_at = time.perf_counter()
        try:
            raw_result = run(
                user_id=request.user_id,
                session_id=request.session_id,
                new_message=request.message,
                **request.run_kwargs,
            )
            if inspect.isawaitable(raw_result):
                raise KitaruUsageError(
                    "Google ADK runner `run(...)` returned an awaitable; call "
                    "`await KitaruADKRunner.run(...)` instead."
                )
            collected = self._collect_sync_result(raw_result)
            return self._build_result(collected, tracker=tracker, started_at=started_at)
        except Exception as exc:
            if tracker is not None:
                tracker.mark_failed(exc)
            raise

    async def _collect_async_result(self, raw_result: Any) -> list[Any]:
        if inspect.isawaitable(raw_result):
            raw_result = await raw_result
        if hasattr(raw_result, "__aiter__"):
            return [event async for event in raw_result]
        return self._collect_sync_result(raw_result)

    def _collect_sync_result(self, raw_result: Any) -> list[Any]:
        if raw_result is None:
            return []
        if isinstance(raw_result, Mapping):
            return [raw_result]
        if isinstance(raw_result, (str, bytes, bytearray)):
            return [raw_result]
        if isinstance(raw_result, Iterator | Iterable):
            return list(raw_result)
        return [raw_result]

    def _build_result(
        self,
        raw_events: list[Any],
        *,
        tracker: EventTracker | None,
        started_at: float,
    ) -> ADKRunResult:
        serialized_events = [
            to_json_safe(event, include_raw=self._capture.capture_mode == "full")
            for event in raw_events
        ]
        usage = self._usage_from_events(raw_events)
        estimated_cost = self._estimate_cost(usage)
        event_dicts = [event for event in serialized_events if isinstance(event, dict)]
        if tracker is not None:
            event_dicts.extend(dump_adk_events(list(tracker.events)))
        handoffs = (
            extract_handoff_requests(event_dicts)
            if has_handoff_markers(event_dicts)
            else []
        )
        result = ADKRunResult(
            status="requires_action" if handoffs else "completed",
            final_output=extract_final_output(
                raw_events,
                include_raw=self._capture.capture_mode == "full",
            ),
            events=event_dicts,
            handoffs=handoffs,
            usage=usage,
            estimated_cost_usd=estimated_cost,
            event_log_artifact_name=None,
            run_summary_artifact_name=None,
            warnings=self._result_warnings(tracker=tracker),
        )
        self._record_usage(result, duration_ms=elapsed_ms(started_at))
        return result

    def _failed_result(
        self,
        error: BaseException,
        *,
        tracker: EventTracker | None,
        started_at: float,
    ) -> ADKRunResult:
        result = ADKRunResult(
            status="failed",
            events=dump_adk_events(list(tracker.events)) if tracker is not None else [],
            event_log_artifact_name=None,
            run_summary_artifact_name=None,
            warnings=[f"Google ADK runner failed: {type(error).__name__}: {error}"],
        )
        self._record_usage(result, duration_ms=elapsed_ms(started_at))
        return result

    def _usage_from_events(self, events: list[Any]) -> ADKUsageSummary | None:
        for event in reversed(events):
            if usage := usage_from_event(event):
                return usage
        return None

    def _estimate_cost(self, usage: ADKUsageSummary | None) -> float | None:
        if usage is None or self._cost_calculator is None:
            return None
        try:
            return self._cost_calculator(usage)
        except Exception:
            return None

    def _result_warnings(self, *, tracker: EventTracker | None) -> list[str]:
        if self._checkpoint_strategy != "calls":
            return []
        if tracker is not None and tracker.events:
            return [
                "Google ADK calls mode used explicit KitaruADKModel / "
                "KitaruADKTool wrapper calls. Only those wrapper calls can "
                "create per-call checkpoints; arbitrary unmodified ADK internals "
                "are not checkpointed."
            ]
        return [
            "Google ADK calls mode observed no KitaruADKModel or KitaruADKTool "
            "calls, so this run has no per-call ADK checkpoints. Arbitrary "
            "unmodified ADK internals are not checkpointed."
        ]

    async def _persist_calls_summary_async(
        self,
        result: ADKRunResult,
        *,
        tracker: EventTracker,
    ) -> ADKRunResult:
        if not self._call_checkpoint_policy.persist_run_artifacts:
            return result
        if runtime.is_inside_flow() and not runtime.is_inside_checkpoint():
            summary = {
                "result": result.model_dump(mode="python"),
                "metadata": tracker.persisted_metadata(),
            }

            async def body() -> dict[str, Any]:
                return summary

            await run_async_in_checkpoint(
                config=resolve_summary_checkpoint_config(self._call_checkpoint_policy),
                step_name=f"{self._name}_google_adk_calls_summary",
                body=body,
                cache_key=checkpoint_cache_key(summary),
            )
        return result

    def _persist_calls_summary_sync(
        self,
        result: ADKRunResult,
        *,
        tracker: EventTracker,
    ) -> ADKRunResult:
        if not self._call_checkpoint_policy.persist_run_artifacts:
            return result
        if runtime.is_inside_flow() and not runtime.is_inside_checkpoint():
            summary = {
                "result": result.model_dump(mode="python"),
                "metadata": tracker.persisted_metadata(),
            }
            run_sync_in_checkpoint(
                config=resolve_summary_checkpoint_config(self._call_checkpoint_policy),
                step_name=f"{self._name}_google_adk_calls_summary",
                body=lambda: summary,
                cache_key=checkpoint_cache_key(summary),
            )
        return result

    def _record_usage(self, result: ADKRunResult, *, duration_ms: float) -> None:
        if result.usage is None or not self._capture.save_usage:
            return
        usage = result.usage
        usage_status = (
            "interrupted" if result.status == "requires_action" else result.status
        )
        usage_record = build_usage_record(
            adapter=ADAPTER_ID,
            surface="adk_turn",
            call_name=self._name,
            status=usage_status,
            model=usage.model_name,
            provider=usage.provider_name,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            total_tokens=usage.total_tokens,
            cached_input_tokens=usage.cached_input_tokens,
            reasoning_tokens=usage.reasoning_tokens,
            raw_usage=usage.raw,
            estimated_cost_usd=result.estimated_cost_usd,
            latency_ms=duration_ms,
            cache_status="unknown",
            billing_effect="unknown",
        )
        log_usage_record(usage_record)

    def _track_run_completed(self, result: ADKRunResult) -> None:
        track(
            AnalyticsEvent.GOOGLE_ADK_RUN_COMPLETED,
            {
                "checkpoint_strategy": self._checkpoint_strategy,
                "status": result.status,
                "event_count": len(result.events),
                "handoff_count": len(result.handoffs),
                "has_usage": result.usage is not None,
            },
        )
