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
    checkpoint_cache_key,
    elapsed_ms,
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
) -> Any:
    """Return a checkpoint-aware copy of supported OpenAI SDK tools."""
    if getattr(tool, "_kitaru_wrapped", False):
        return tool
    if isinstance(tool, FunctionTool):
        return _wrap_function_tool(
            tool,
            capture=capture,
            agent_name=agent_name,
            tool_checkpoint_config=tool_checkpoint_config,
            tool_checkpoint_config_by_name=tool_checkpoint_config_by_name,
        )
    return tool


def kitaruify_openai_tools(
    tools: list[Any],
    *,
    capture: OpenAICapturePolicy,
    agent_name: str,
    tool_checkpoint_config: CheckpointConfig | None = None,
    tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
) -> list[Any]:
    return [
        kitaruify_openai_tool(
            tool,
            capture=capture,
            agent_name=agent_name,
            tool_checkpoint_config=tool_checkpoint_config,
            tool_checkpoint_config_by_name=tool_checkpoint_config_by_name,
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
) -> FunctionTool:
    original_callback = tool.on_invoke_tool
    tool_name = tool.name

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

            async def _in_checkpoint() -> Any:
                return await _tracked_tool_call(
                    original_callback,
                    context,
                    input_json,
                    tool=tool,
                    capture=capture,
                    tool_call_id=tool_call_id,
                )

            return await run_async_in_checkpoint(
                config=with_default_type(checkpoint_config, "tool_call"),
                step_name=safe_step_name(f"{agent_name}_{tool_name}_tool_call"),
                body=_in_checkpoint,
                cache_key=checkpoint_cache_key(
                    {
                        "tool_name": tool_name,
                        "tool_call_id": tool_call_id,
                        "fallback_sequence": fallback_sequence,
                        "tool_namespace": getattr(tool, "_tool_namespace", None),
                        "input_json": input_json,
                    }
                ),
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
    return wrapped


async def _tracked_tool_call(
    callback: Any,
    context: Any,
    input_json: str,
    *,
    tool: FunctionTool,
    capture: OpenAICapturePolicy,
    tool_call_id: str | None,
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
    parsed_args = _parse_json(input_json)
    if capture.save_input:
        args_key = artifact_name(event_id, "input")
        kitaru.save(
            args_key,
            {
                "tool_name": tool.name,
                "tool_call_id": tool_call_id,
                "raw_args": input_json,
                "parsed_args": parsed_args,
            },
            type="input",
        )
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
