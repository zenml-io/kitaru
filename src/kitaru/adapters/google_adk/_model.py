"""Google ADK model wrapper for true model-call checkpoints."""

from __future__ import annotations

import importlib
import time
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from . import _kitaru_internal as runtime
from ._policy import (
    ADKCallCheckpointPolicy,
    ADKCapturePolicy,
    resolve_model_checkpoint_config,
)
from ._serialization import object_metadata, to_cache_identity, to_json_safe
from ._tracking import (
    EventTracker,
    current_call_policy,
    current_capture_policy,
    current_tracker,
)
from ._utils import (
    checkpoint_cache_key,
    elapsed_ms,
    run_async_in_checkpoint,
)

_BaseLlm: type[Any] = importlib.import_module("google.adk.models.base_llm").BaseLlm


def _base_llm_init_kwargs(values: dict[str, Any]) -> dict[str, Any]:
    fields = getattr(_BaseLlm, "model_fields", None)
    if isinstance(fields, Mapping):
        return {key: value for key, value in values.items() if key in fields}
    return values


class KitaruADKModel(_BaseLlm):  # type: ignore[misc, valid-type]
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
        public_model = str(
            getattr(model, "model", None) or name or type(model).__name__
        )
        try:
            super().__init__(**_base_llm_init_kwargs({"model": public_model}))
        except TypeError:
            super().__init__()
            object.__setattr__(self, "model", public_model)

        object.__setattr__(self, "_model", model)
        object.__setattr__(self, "_name", name or public_model)
        object.__setattr__(self, "_capture", capture or ADKCapturePolicy())
        object.__setattr__(
            self,
            "_call_policy",
            call_policy or ADKCallCheckpointPolicy(),
        )
        object.__setattr__(self, "_tracker", tracker)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._model, name)

    def supported_models(self) -> list[str]:
        """Return the wrapped model's supported model names when available."""
        supported_models = getattr(self._model, "supported_models", None)
        if not callable(supported_models):
            return []
        return supported_models()

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
        call_policy = current_call_policy() or self._call_policy
        capture = current_capture_policy() or self._capture
        config = resolve_model_checkpoint_config(call_policy)
        if config is None or not self._can_checkpoint(call_policy):
            return await collect(), False
        raw_model_input = {
            "model": self.model,
            "stream": stream,
            "llm_request": llm_request,
            "wrapped_model": object_metadata(self._model),
        }
        model_input = to_json_safe(
            raw_model_input,
            include_raw=capture.capture_mode == "full",
        )
        return await run_async_in_checkpoint(
            config=config,
            step_name=f"google_adk_model_{self._name}",
            body=collect,
            cache_key=checkpoint_cache_key(to_cache_identity(raw_model_input)),
            checkpoint_inputs={"model_input": model_input},
        ), True

    def _can_checkpoint(self, call_policy: ADKCallCheckpointPolicy) -> bool:
        if not runtime.is_inside_flow():
            return False
        if not runtime.is_inside_checkpoint():
            return True
        if call_policy.nested_checkpoint_policy == "metadata_only":
            return False
        raise KitaruUsageError(
            "KitaruADKModel cannot open a model-call checkpoint while already "
            "inside a Kitaru checkpoint. Set "
            "ADKCallCheckpointPolicy(nested_checkpoint_policy='metadata_only') "
            "to execute the wrapped model directly and record metadata only."
        )
