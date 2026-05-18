"""Tool checkpoint wrappers for OpenAI Agents SDK local function tools."""

import json
import time
from dataclasses import replace
from typing import Any

from agents.tool import FunctionTool

import kitaru

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._policy import OpenAICapturePolicy
from ._serialization import to_json_safe
from ._tracking import artifact_name, get_current_tracker
from ._utils import (
    CheckpointConfig,
    ToolCheckpointOverrides,
    adapter_checkpoint_artifact_refs,
    checkpoint_cache_key,
    elapsed_ms,
    get_adapter_checkpoint_artifact_refs,
    resolve_tool_checkpoint_config,
    run_async_in_checkpoint,
    safe_step_name,
    with_default_type,
)


def kitaruify_openai_tool(
    tool: Any,
    *,
    capture: OpenAICapturePolicy,
    agent_name: str,
    tool_checkpoint_config: CheckpointConfig | None = None,
    tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
    context_cache_identity: Any = None,
    context_cache_key: str | None = None,
) -> Any:
    """Return a checkpoint-aware copy of supported OpenAI SDK tools."""
    original_tool = getattr(tool, "_kitaru_original_tool", None)
    if isinstance(original_tool, FunctionTool):
        tool = original_tool
    elif getattr(tool, "_kitaru_wrapped", False):
        return tool
    if isinstance(tool, FunctionTool):
        return _wrap_function_tool(
            tool,
            capture=capture,
            agent_name=agent_name,
            tool_checkpoint_config=tool_checkpoint_config,
            tool_checkpoint_config_by_name=tool_checkpoint_config_by_name,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )
    return tool


def kitaruify_openai_tools(
    tools: list[Any],
    *,
    capture: OpenAICapturePolicy,
    agent_name: str,
    tool_checkpoint_config: CheckpointConfig | None = None,
    tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
    context_cache_identity: Any = None,
    context_cache_key: str | None = None,
) -> list[Any]:
    return [
        kitaruify_openai_tool(
            tool,
            capture=capture,
            agent_name=agent_name,
            tool_checkpoint_config=tool_checkpoint_config,
            tool_checkpoint_config_by_name=tool_checkpoint_config_by_name,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )
        for tool in tools
    ]


def _wrap_function_tool(
    tool: FunctionTool,
    *,
    capture: OpenAICapturePolicy,
    agent_name: str,
    tool_checkpoint_config: CheckpointConfig | None,
    tool_checkpoint_config_by_name: ToolCheckpointOverrides | None,
    context_cache_identity: Any = None,
    context_cache_key: str | None = None,
) -> FunctionTool:
    original_callback = tool.on_invoke_tool
    tool_name = tool.name
    resolved_context_cache_key = context_cache_key
    if resolved_context_cache_key is None and context_cache_identity is not None:
        resolved_context_cache_key = checkpoint_cache_key(
            {"context": context_cache_identity}
        )

    async def _wrapped_callback(context: Any, input_json: str) -> Any:
        checkpoint_config = resolve_tool_checkpoint_config(
            tool_name,
            default=tool_checkpoint_config,
            by_name=tool_checkpoint_config_by_name,
        )
        tool_call_id = _tool_call_id(context)
        fallback_sequence = (
            _tool_checkpoint_sequence() if tool_call_id is None else None
        )
        if (
            checkpoint_config is not None
            and is_inside_flow()
            and not is_inside_checkpoint()
        ):
            input_envelope = (
                _tool_input_envelope(
                    tool=tool,
                    tool_call_id=tool_call_id,
                    input_json=input_json,
                )
                if capture.save_input
                else None
            )
            checkpoint_inputs = (
                {"tool_args": input_envelope} if input_envelope is not None else None
            )
            input_artifacts = {"input": "tool_args"} if capture.save_input else {}
            output_artifacts = {"result": "output"} if capture.save_final_output else {}

            async def _in_checkpoint() -> Any:
                with adapter_checkpoint_artifact_refs(
                    input_artifacts=input_artifacts,
                    output_artifacts=output_artifacts,
                ):
                    return await _tracked_tool_call(
                        original_callback,
                        context,
                        input_json,
                        tool=tool,
                        capture=capture,
                        tool_call_id=tool_call_id,
                        input_envelope=input_envelope,
                    )

            cache_payload = {
                "tool_name": tool_name,
                "tool_call_id": tool_call_id,
                "fallback_sequence": fallback_sequence,
                "tool_namespace": getattr(tool, "_tool_namespace", None),
                "input_json": input_json,
            }
            if resolved_context_cache_key is not None:
                cache_payload["context_cache_key"] = resolved_context_cache_key

            return await run_async_in_checkpoint(
                config=with_default_type(checkpoint_config, "tool_call"),
                step_name=safe_step_name(f"{agent_name}_{tool_name}_tool_call"),
                body=_in_checkpoint,
                cache_key=checkpoint_cache_key(cache_payload),
                checkpoint_inputs=checkpoint_inputs,
            )
        return await _tracked_tool_call(
            original_callback,
            context,
            input_json,
            tool=tool,
            capture=capture,
            tool_call_id=tool_call_id,
        )

    wrapped = replace(tool, on_invoke_tool=_wrapped_callback)
    object.__setattr__(wrapped, "_kitaru_wrapped", True)
    object.__setattr__(wrapped, "_kitaru_original_tool", tool)
    return wrapped


async def _tracked_tool_call(
    callback: Any,
    context: Any,
    input_json: str,
    *,
    tool: FunctionTool,
    capture: OpenAICapturePolicy,
    tool_call_id: str | None,
    input_envelope: dict[str, Any] | None = None,
) -> Any:
    tracker = get_current_tracker()
    should_track = (
        tracker is not None and capture.emit_child_events and is_inside_checkpoint()
    )
    if not should_track:
        return await callback(context, input_json)

    assert tracker is not None
    event_id, event_context = tracker.start_tool_event(tool_call_id=tool_call_id)
    artifacts: dict[str, str] = {}
    adapter_refs = get_adapter_checkpoint_artifact_refs()
    if capture.save_input:
        if adapter_refs is not None and "input" in adapter_refs.input_artifacts:
            artifacts["input"] = adapter_refs.input_artifacts["input"]
        else:
            input_envelope = input_envelope or _tool_input_envelope(
                tool=tool,
                tool_call_id=tool_call_id,
                input_json=input_json,
            )
            args_key = artifact_name(event_id, "input")
            kitaru.save(args_key, input_envelope, type="input")
            artifacts["input"] = args_key

    started_at = time.perf_counter()
    try:
        result = await callback(context, input_json)
    except Exception as error:
        tracker.record_event(
            event_id,
            event_context,
            kind="tool_call",
            status="failed",
            duration_ms=elapsed_ms(started_at),
            artifacts=artifacts,
            metadata={"tool_name": tool.name, "tool_call_id": tool_call_id},
            error=error,
        )
        raise

    if capture.save_final_output:
        if adapter_refs is not None and "result" in adapter_refs.output_artifacts:
            artifacts["result"] = adapter_refs.output_artifacts["result"]
        else:
            result_key = artifact_name(event_id, "result")
            kitaru.save(result_key, to_json_safe(result), type="output")
            artifacts["result"] = result_key

    tracker.record_event(
        event_id,
        event_context,
        kind="tool_call",
        status="completed",
        duration_ms=elapsed_ms(started_at),
        artifacts=artifacts,
        metadata={
            "tool_name": tool.name,
            "tool_call_id": tool_call_id,
            "is_agent_tool": bool(getattr(tool, "_is_agent_tool", False)),
            "tool_namespace": getattr(tool, "_tool_namespace", None),
        },
    )
    return result


def _tool_input_envelope(
    *,
    tool: FunctionTool,
    tool_call_id: str | None,
    input_json: str,
) -> dict[str, Any]:
    return {
        "tool_name": tool.name,
        "tool_call_id": tool_call_id,
        "raw_args": input_json,
        "parsed_args": _parse_json(input_json),
    }


def _tool_checkpoint_sequence() -> int | None:
    tracker = get_current_tracker()
    if tracker is None:
        return None
    return tracker.next_tool_checkpoint_sequence()


def _parse_json(input_json: str) -> Any:
    try:
        return json.loads(input_json)
    except json.JSONDecodeError:
        return {"raw": input_json, "serialization_error": "invalid_json"}


def _tool_call_id(context: Any) -> str | None:
    for attr in ("tool_call_id", "call_id"):
        value = getattr(context, attr, None)
        if isinstance(value, str) and value:
            return value
    item = getattr(context, "tool_call", None)
    value = getattr(item, "call_id", None)
    return value if isinstance(value, str) and value else None
