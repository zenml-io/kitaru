"""Google ADK model wrapper for true model-call checkpoints."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable
from typing import Any

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from . import _kitaru_internal as runtime
from ._policy import (
    ADKCallCheckpointPolicy,
    ADKCapturePolicy,
    resolve_model_checkpoint_config,
)
from ._serialization import object_metadata, to_json_safe
from ._tracking import EventTracker, current_tracker
from ._utils import (
    checkpoint_cache_key,
    elapsed_ms,
    run_async_in_checkpoint,
)


class KitaruADKModel:
    """Wrap an ADK model-like object at ``generate_content_async``.

    Public ADK docs expose ``BaseLlm.generate_content_async(...)`` as the model
    execution method. This wrapper puts the complete async response sequence
    inside one Kitaru checkpoint when called from a flow body. If the underlying
    model streams, the wrapper buffers the stream before yielding it back to ADK;
    that is the current replay-safe tradeoff for this spike.
    """

    def __init__(
        self,
        model: Any,
        *,
        name: str | None = None,
        capture: ADKCapturePolicy | None = None,
        call_policy: ADKCallCheckpointPolicy | None = None,
        tracker: EventTracker | None = None,
    ) -> None:
        self._model = model
        self._name = name or str(getattr(model, "model", None) or type(model).__name__)
        self._capture = capture or ADKCapturePolicy()
        self._call_policy = call_policy or ADKCallCheckpointPolicy()
        self._tracker = tracker

    @property
    def model(self) -> Any:
        return getattr(self._model, "model", self._name)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._model, name)

    @classmethod
    def supported_models(cls) -> list[str]:
        """Conservative fallback for ADK model-registry introspection."""
        return []

    async def connect(self, *args: Any, **kwargs: Any) -> Any:
        connect = getattr(self._model, "connect", None)
        if not callable(connect):
            raise AttributeError("Wrapped ADK model does not expose connect(...).")
        return await connect(*args, **kwargs)

    async def generate_content_async(
        self,
        llm_request: Any,
        stream: bool = False,
    ) -> AsyncIterator[Any]:
        """Generate content with the provider call inside a checkpoint if possible."""
        tracker = self._tracker or current_tracker()
        event_id, context = (
            tracker.start_event("model_call")
            if tracker is not None
            else (f"google_adk_model_{self._name}", None)
        )
        started_at = time.perf_counter()

        async def collect() -> list[Any]:
            generate = getattr(self._model, "generate_content_async", None)
            if not callable(generate):
                raise KitaruUsageError(
                    "Wrapped Google ADK model does not expose "
                    "`generate_content_async(...)`."
                )
            responses: list[Any] = []
            async for response in generate(llm_request, stream=stream):
                responses.append(response)
            return responses

        try:
            responses, checkpointed = await self._checkpoint_or_collect(
                llm_request=llm_request,
                stream=stream,
                collect=collect,
            )
        except BaseException as exc:
            if tracker is not None and context is not None:
                tracker.record_event(
                    event_id,
                    context,
                    kind="model_call",
                    status="failed",
                    duration_ms=elapsed_ms(started_at),
                    model_name=str(self.model),
                    error=exc,
                )
            raise

        if tracker is not None and context is not None:
            tracker.record_event(
                event_id,
                context,
                kind="model_call",
                status="completed" if checkpointed else "metadata_only",
                duration_ms=elapsed_ms(started_at),
                model_name=str(self.model),
                metadata={"stream": stream, "response_count": len(responses)},
            )
        track(
            AnalyticsEvent.GOOGLE_ADK_CALL_CHECKPOINTED,
            {"call_kind": "model", "checkpointed": checkpointed},
        )
        for response in responses:
            yield response

    async def _checkpoint_or_collect(
        self,
        *,
        llm_request: Any,
        stream: bool,
        collect: Callable[[], Any],
    ) -> tuple[list[Any], bool]:
        config = resolve_model_checkpoint_config(self._call_policy)
        if config is None or not self._can_checkpoint():
            return await collect(), False
        model_input = to_json_safe(
            {
                "model": self.model,
                "stream": stream,
                "llm_request": llm_request,
                "wrapped_model": object_metadata(self._model),
            },
            include_raw=self._capture.capture_mode == "full",
        )
        return await run_async_in_checkpoint(
            config=config,
            step_name=f"google_adk_model_{self._name}",
            body=collect,
            cache_key=checkpoint_cache_key(model_input),
            checkpoint_inputs={"model_input": model_input},
        ), True

    def _can_checkpoint(self) -> bool:
        if not runtime.is_inside_flow():
            return False
        if not runtime.is_inside_checkpoint():
            return True
        if self._call_policy.nested_checkpoint_policy == "metadata_only":
            return False
        raise KitaruUsageError(
            "KitaruADKModel cannot open a model-call checkpoint while already "
            "inside a Kitaru checkpoint. Set "
            "ADKCallCheckpointPolicy(nested_checkpoint_policy='metadata_only') "
            "to execute the wrapped model directly and record metadata only."
        )
