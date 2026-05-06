"""Model/provider checkpoint wrappers for OpenAI Agents SDK calls."""

import time
from collections.abc import AsyncIterator
from typing import Any

from agents.models.interface import Model, ModelProvider

import kitaru

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._policy import OpenAICapturePolicy
from ._serialization import to_cache_identity, to_json_safe
from ._tracking import artifact_name, get_current_tracker
from ._usage import normalize_usage
from ._utils import (
    CheckpointConfig,
    checkpoint_cache_key,
    elapsed_ms,
    run_async_in_checkpoint,
    safe_step_name,
    with_default_type,
)


class KitaruOpenAIModel(Model):
    """OpenAI ``Model`` proxy that checkpoints ``get_response`` calls."""

    def __init__(
        self,
        wrapped: Any,
        *,
        capture: OpenAICapturePolicy,
        agent_name: str,
        checkpoint_config: CheckpointConfig | None = None,
        requested_model_name: str | None = None,
    ) -> None:
        self._wrapped = wrapped
        self._capture = capture
        self._agent_name = agent_name
        self._checkpoint_config = checkpoint_config
        self._requested_model_name = requested_model_name

    @property
    def wrapped(self) -> Any:
        return self._wrapped

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    async def get_response(
        self,
        system_instructions: str | None,
        input: Any,
        model_settings: Any,
        tools: list[Any],
        output_schema: Any,
        handoffs: list[Any],
        tracing: Any,
        *,
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: Any,
    ) -> Any:
        if (
            self._checkpoint_config is not None
            and is_inside_flow()
            and not is_inside_checkpoint()
        ):

            async def _in_checkpoint() -> Any:
                return await self._tracked_get_response(
                    system_instructions,
                    input,
                    model_settings,
                    tools,
                    output_schema,
                    handoffs,
                    tracing,
                    previous_response_id=previous_response_id,
                    conversation_id=conversation_id,
                    prompt=prompt,
                )

            return await run_async_in_checkpoint(
                config=with_default_type(self._checkpoint_config, "llm_call"),
                step_name=safe_step_name(f"{self._agent_name}_openai_model_call"),
                body=_in_checkpoint,
                cache_key=checkpoint_cache_key(
                    {
                        "system_instructions": system_instructions,
                        "model_identity": self._model_cache_identity(),
                        "input": to_cache_identity(input),
                        "model_settings": to_cache_identity(model_settings),
                        "tools": [_tool_cache_identity(tool) for tool in tools],
                        "output_schema": to_cache_identity(output_schema),
                        "handoffs": to_cache_identity(handoffs),
                        "previous_response_id": previous_response_id,
                        "conversation_id": conversation_id,
                        "prompt": to_cache_identity(prompt),
                    }
                ),
            )
        return await self._tracked_get_response(
            system_instructions,
            input,
            model_settings,
            tools,
            output_schema,
            handoffs,
            tracing,
            previous_response_id=previous_response_id,
            conversation_id=conversation_id,
            prompt=prompt,
        )

    async def _tracked_get_response(
        self,
        system_instructions: str | None,
        input: Any,
        model_settings: Any,
        tools: list[Any],
        output_schema: Any,
        handoffs: list[Any],
        tracing: Any,
        *,
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: Any,
    ) -> Any:
        tracker = get_current_tracker()
        should_track = (
            tracker is not None
            and self._capture.emit_child_events
            and is_inside_checkpoint()
        )
        if not should_track:
            return await self._wrapped.get_response(
                system_instructions,
                input,
                model_settings,
                tools,
                output_schema,
                handoffs,
                tracing,
                previous_response_id=previous_response_id,
                conversation_id=conversation_id,
                prompt=prompt,
            )

        assert tracker is not None
        event_id, event_context = tracker.start_llm_event()
        artifacts: dict[str, str] = {}
        if self._capture.save_input:
            input_key = artifact_name(event_id, "input")
            kitaru.save(
                input_key,
                {
                    "system_instructions": system_instructions,
                    "input": to_json_safe(input),
                    "model_settings": to_json_safe(model_settings),
                    "previous_response_id": previous_response_id,
                    "conversation_id": conversation_id,
                    "prompt": to_json_safe(prompt),
                },
                type="input",
            )
            artifacts["input"] = input_key

        started_at = time.perf_counter()
        try:
            response = await self._wrapped.get_response(
                system_instructions,
                input,
                model_settings,
                tools,
                output_schema,
                handoffs,
                tracing,
                previous_response_id=previous_response_id,
                conversation_id=conversation_id,
                prompt=prompt,
            )
        except Exception as error:
            tracker.record_event(
                event_id,
                event_context,
                kind="llm_call",
                status="failed",
                duration_ms=elapsed_ms(started_at),
                artifacts=artifacts,
                error=error,
            )
            raise

        usage = normalize_usage(getattr(response, "usage", None))
        metadata: dict[str, object] = {
            "response_id": getattr(response, "response_id", None),
        }
        if self._capture.save_usage:
            metadata["usage"] = usage.model_dump(mode="json")
        if self._capture.save_response_items:
            metadata["response_item_count"] = len(getattr(response, "output", []) or [])
        if self._capture.save_final_output:
            response_key = artifact_name(event_id, "response")
            kitaru.save(response_key, to_json_safe(response), type="response")
            artifacts["response"] = response_key
        if self._capture.save_usage:
            usage_key = artifact_name(event_id, "usage")
            kitaru.save(usage_key, usage.model_dump(mode="json"), type="context")
            artifacts["usage"] = usage_key

        tracker.record_event(
            event_id,
            event_context,
            kind="llm_call",
            status="completed",
            duration_ms=elapsed_ms(started_at),
            artifacts=artifacts,
            metadata=metadata,
        )
        return response

    def _model_cache_identity(self) -> dict[str, Any]:
        wrapped_type = type(self._wrapped)
        exposed_name = getattr(self._wrapped, "model_name", None)
        return {
            "requested_model_name": self._requested_model_name,
            "model_name": exposed_name if isinstance(exposed_name, str) else None,
            "python_type": f"{wrapped_type.__module__}.{wrapped_type.__qualname__}",
        }

    def stream_response(
        self,
        system_instructions: str | None,
        input: Any,
        model_settings: Any,
        tools: list[Any],
        output_schema: Any,
        handoffs: list[Any],
        tracing: Any,
        *,
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: Any,
    ) -> AsyncIterator[Any]:
        # Streaming checkpoint replay needs a buffered transcript design. Item 2
        # only promises non-streaming model-call checkpoints, so streaming stays
        # a transparent pass-through for now.
        return self._wrapped.stream_response(
            system_instructions,
            input,
            model_settings,
            tools,
            output_schema,
            handoffs,
            tracing,
            previous_response_id=previous_response_id,
            conversation_id=conversation_id,
            prompt=prompt,
        )


class KitaruOpenAIModelProvider(ModelProvider):
    """OpenAI ``ModelProvider`` proxy that returns checkpointed models."""

    def __init__(
        self,
        wrapped: Any,
        *,
        capture: OpenAICapturePolicy,
        agent_name: str,
        checkpoint_config: CheckpointConfig | None = None,
    ) -> None:
        self._wrapped = wrapped
        self._capture = capture
        self._agent_name = agent_name
        self._checkpoint_config = checkpoint_config

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    def get_model(self, model_name: str | None) -> Any:
        model = self._wrapped.get_model(model_name)
        return kitaruify_openai_model(
            model,
            capture=self._capture,
            agent_name=self._agent_name,
            checkpoint_config=self._checkpoint_config,
            requested_model_name=model_name,
        )


def kitaruify_openai_model(
    model: Any,
    *,
    capture: OpenAICapturePolicy,
    agent_name: str,
    checkpoint_config: CheckpointConfig | None,
    requested_model_name: str | None = None,
) -> Any:
    if isinstance(model, KitaruOpenAIModel):
        return model
    return KitaruOpenAIModel(
        model,
        capture=capture,
        agent_name=agent_name,
        checkpoint_config=checkpoint_config,
        requested_model_name=requested_model_name,
    )


def kitaruify_openai_model_provider(
    provider: Any,
    *,
    capture: OpenAICapturePolicy,
    agent_name: str,
    checkpoint_config: CheckpointConfig | None,
) -> Any:
    if isinstance(provider, KitaruOpenAIModelProvider):
        return provider
    return KitaruOpenAIModelProvider(
        provider,
        capture=capture,
        agent_name=agent_name,
        checkpoint_config=checkpoint_config,
    )


def _tool_cache_identity(tool: Any) -> dict[str, Any]:
    return {
        "name": getattr(tool, "name", None),
        "type": type(tool).__name__,
        "description": getattr(tool, "description", None),
        "tool_namespace": getattr(tool, "_tool_namespace", None),
    }
