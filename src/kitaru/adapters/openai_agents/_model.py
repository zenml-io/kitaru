"""Model/provider checkpoint wrappers for OpenAI Agents SDK calls."""

import time
from collections.abc import AsyncIterator, Mapping
from typing import Any

from agents.models.interface import Model, ModelProvider
from agents.tool import FunctionTool

import kitaru

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._policy import OpenAICapturePolicy
from ._serialization import to_cache_identity, to_json_safe
from ._tracking import artifact_name, get_current_tracker
from ._usage import normalize_usage
from ._utils import (
    CheckpointConfig,
    adapter_checkpoint_artifact_refs,
    checkpoint_cache_key,
    elapsed_ms,
    get_adapter_checkpoint_artifact_refs,
    run_async_in_checkpoint,
    safe_step_name,
    with_default_type,
)


def _model_input_envelope(
    *,
    system_instructions: str | None,
    input: Any,
    model_settings: Any,
    previous_response_id: str | None,
    conversation_id: str | None,
    prompt: Any,
) -> dict[str, Any]:
    return {
        "system_instructions": system_instructions,
        "input": to_json_safe(input),
        "model_settings": to_json_safe(model_settings),
        "previous_response_id": previous_response_id,
        "conversation_id": conversation_id,
        "prompt": to_json_safe(prompt),
    }


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
            input_envelope = (
                _model_input_envelope(
                    system_instructions=system_instructions,
                    input=input,
                    model_settings=model_settings,
                    previous_response_id=previous_response_id,
                    conversation_id=conversation_id,
                    prompt=prompt,
                )
                if self._capture.save_input
                else None
            )
            checkpoint_inputs = (
                {"input": input_envelope} if input_envelope is not None else None
            )
            input_artifacts = {"input": "input"} if self._capture.save_input else {}
            output_artifacts = (
                {"response": "output"} if self._capture.save_final_output else {}
            )

            async def _in_checkpoint() -> Any:
                with adapter_checkpoint_artifact_refs(
                    input_artifacts=input_artifacts,
                    output_artifacts=output_artifacts,
                ):
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
                        input_envelope=input_envelope,
                    )

            response = await run_async_in_checkpoint(
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
                checkpoint_inputs=checkpoint_inputs,
            )
            self._reserve_tool_call_order(response, tools)
            return response
        response = await self._tracked_get_response(
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
        self._reserve_tool_call_order(response, tools)
        return response

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
        input_envelope: Mapping[str, Any] | None = None,
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
        adapter_refs = get_adapter_checkpoint_artifact_refs()
        if self._capture.save_input:
            if adapter_refs is not None and "input" in adapter_refs.input_artifacts:
                artifacts["input"] = adapter_refs.input_artifacts["input"]
            else:
                input_envelope = input_envelope or _model_input_envelope(
                    system_instructions=system_instructions,
                    input=input,
                    model_settings=model_settings,
                    previous_response_id=previous_response_id,
                    conversation_id=conversation_id,
                    prompt=prompt,
                )
                input_key = artifact_name(event_id, "input")
                kitaru.save(input_key, input_envelope, type="input")
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
            if adapter_refs is not None and "response" in adapter_refs.output_artifacts:
                artifacts["response"] = adapter_refs.output_artifacts["response"]
            else:
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

    def _reserve_tool_call_order(self, response: Any, tools: list[Any]) -> None:
        tracker = get_current_tracker()
        if tracker is None or not self._capture.emit_child_events:
            return
        tracker.reserve_tool_call_order(_trackable_tool_call_ids(response, tools))

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


def _trackable_tool_call_ids(response: Any, tools: list[Any]) -> list[str]:
    """Return local function-tool call IDs in assistant-emitted order."""
    function_tool_names = {
        tool.name
        for tool in tools
        if isinstance(tool, FunctionTool) and isinstance(tool.name, str)
    }
    if not function_tool_names:
        return []

    tool_call_ids: list[str] = []
    for item in getattr(response, "output", []) or []:
        tool_name = _tool_name_from_response_item(item)
        tool_call_id = _tool_call_id_from_response_item(item)
        if tool_name in function_tool_names and tool_call_id is not None:
            tool_call_ids.append(tool_call_id)
    return tool_call_ids


def _tool_name_from_response_item(item: Any) -> str | None:
    value = _response_item_value(item, "name")
    return value if isinstance(value, str) and value else None


def _tool_call_id_from_response_item(item: Any) -> str | None:
    for key in ("call_id", "tool_call_id"):
        value = _response_item_value(item, key)
        if isinstance(value, str) and value:
            return value
    return None


def _response_item_value(item: Any, key: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(key)
    return getattr(item, key, None)
