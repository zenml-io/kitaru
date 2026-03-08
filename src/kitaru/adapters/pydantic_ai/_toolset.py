"""Toolset interception for Kitaru's PydanticAI adapter."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, TypedDict, cast

from pydantic_ai import (
    AbstractToolset,
    CallDeferred,
    FunctionToolset,
    ToolsetTool,
    WrapperToolset,
)
from pydantic_ai.exceptions import ApprovalRequired
from pydantic_ai.tools import AgentDepsT, RunContext

from kitaru.artifacts import save
from kitaru.errors import KitaruUsageError
from kitaru.logging import log
from kitaru.runtime import _is_inside_checkpoint, _suspend_checkpoint_scope
from kitaru.wait import wait

from ._hitl import hitl_config_from_tool_metadata
from ._tracking import ToolEventContext, get_current_tracker

CaptureMode = Literal["full", "metadata_only", "off"]


class CaptureConfig(TypedDict, total=False):
    """Capture policy for adapter-managed tool call observability."""

    mode: CaptureMode
    enabled: bool
    save_args: bool
    save_result: bool
    include_timings: bool


@dataclass(frozen=True)
class ResolvedCaptureConfig:
    """Normalized capture policy used during tool execution."""

    mode: CaptureMode
    enabled: bool
    save_args: bool
    save_result: bool
    include_timings: bool


def _error_payload(error: Exception) -> dict[str, str]:
    """Build a lightweight error payload for child-event metadata."""
    return {
        "type": error.__class__.__name__,
        "message": str(error),
    }


def _save_with_fallback(name: str, value: Any, *, artifact_type: str) -> str:
    """Save an artifact and fall back to a blob repr if serialization fails."""
    try:
        save(name, value, type=artifact_type)
        return artifact_type
    except Exception:
        fallback_value = {
            "repr": repr(value),
            "python_type": value.__class__.__name__,
        }
        save(name, fallback_value, type="blob")
        return "blob"


def _resolve_capture_config(config: CaptureConfig | None) -> ResolvedCaptureConfig:
    """Normalize a possibly-partial capture config into concrete booleans."""
    raw_config = cast(CaptureConfig, dict(config or {}))
    mode = raw_config.get("mode", "full")
    if mode not in {"full", "metadata_only", "off"}:
        raise ValueError(
            "Unsupported tool capture mode "
            f"{mode!r}. Expected one of: 'full', 'metadata_only', 'off'."
        )

    if mode == "full":
        enabled = True
        save_args = True
        save_result = True
        include_timings = True
    elif mode == "metadata_only":
        enabled = True
        save_args = False
        save_result = False
        include_timings = True
    else:
        enabled = False
        save_args = False
        save_result = False
        include_timings = False

    if "enabled" in raw_config:
        enabled = raw_config["enabled"]
    if "save_args" in raw_config:
        save_args = raw_config["save_args"]
    if "save_result" in raw_config:
        save_result = raw_config["save_result"]
    if "include_timings" in raw_config:
        include_timings = raw_config["include_timings"]

    if not enabled:
        return ResolvedCaptureConfig(
            mode="off",
            enabled=False,
            save_args=False,
            save_result=False,
            include_timings=False,
        )

    normalized_mode: CaptureMode = (
        "full" if save_args or save_result else "metadata_only"
    )
    return ResolvedCaptureConfig(
        mode=normalized_mode,
        enabled=True,
        save_args=save_args,
        save_result=save_result,
        include_timings=include_timings,
    )


class KitaruWrapperToolset(WrapperToolset[AgentDepsT]):
    """Base class for toolsets wrapped with Kitaru tracking behavior."""

    @property
    def id(self) -> str | None:
        return self.wrapped.id

    def visit_and_replace(
        self,
        visitor: Callable[[AbstractToolset[AgentDepsT]], AbstractToolset[AgentDepsT]],
    ) -> AbstractToolset[AgentDepsT]:
        """Prevent visitor-based replacement after Kitaru wrapping."""
        return self


class KitaruToolset(KitaruWrapperToolset[AgentDepsT]):
    """Toolset wrapper that records tool calls as checkpoint child events."""

    def __init__(
        self,
        wrapped: AbstractToolset[AgentDepsT],
        *,
        toolset_kind: str,
        tool_capture_config: CaptureConfig | None = None,
        tool_capture_config_by_name: dict[str, CaptureConfig | None] | None = None,
    ) -> None:
        super().__init__(wrapped)
        self._toolset_kind = toolset_kind
        self._tool_capture_config = cast(CaptureConfig, dict(tool_capture_config or {}))
        self._tool_capture_config_by_name: dict[str, CaptureConfig | None] = {
            key: (None if value is None else cast(CaptureConfig, dict(value)))
            for key, value in (tool_capture_config_by_name or {}).items()
        }

    def _capture_config_for_tool(self, name: str) -> ResolvedCaptureConfig:
        """Resolve effective capture config for a tool call."""
        merged_config = cast(CaptureConfig, dict(self._tool_capture_config))

        if name in self._tool_capture_config_by_name:
            override = self._tool_capture_config_by_name[name]
            if override is None:
                return _resolve_capture_config({"mode": "off"})
            merged_config.update(override)

        return _resolve_capture_config(merged_config)

    def _build_tool_event_payload(
        self,
        *,
        name: str,
        event_context: ToolEventContext,
        status: str,
        hitl: bool,
        capture_config: ResolvedCaptureConfig,
        duration_ms: float | None,
        args_artifact: str | None,
        result_artifact: str | None,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        """Build a metadata payload for one tracked tool event."""
        artifacts: dict[str, str] = {}
        if args_artifact is not None:
            artifacts["args"] = args_artifact
        if result_artifact is not None:
            artifacts["result"] = result_artifact

        payload: dict[str, Any] = {
            "type": "tool_call",
            "tool_name": name,
            "toolset_kind": self._toolset_kind,
            "status": status,
            "hitl": hitl,
            "capture_mode": capture_config.mode,
            "sequence_index": event_context.sequence_index,
            "turn_index": event_context.turn_index,
            "parent_event_ids": event_context.parent_event_ids,
            "fan_out_from": event_context.fan_out_from,
            "artifacts": artifacts,
        }

        if capture_config.include_timings and duration_ms is not None:
            payload["duration_ms"] = duration_ms

        if error is not None:
            payload["error"] = _error_payload(error)

        return payload

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
    ) -> Any:
        tracker = get_current_tracker()
        if not _is_inside_checkpoint() or tracker is None:
            return await super().call_tool(name, tool_args, ctx, tool)

        capture_config = self._capture_config_for_tool(name)
        event_id: str | None = None
        event_context: ToolEventContext | None = None
        args_artifact: str | None = None

        if capture_config.enabled:
            event_id, event_context = tracker.start_tool_event()
            if capture_config.save_args:
                args_artifact = f"{event_id}_args"
                _save_with_fallback(args_artifact, tool_args, artifact_type="input")

        hitl_config = hitl_config_from_tool_metadata(tool.tool_def.metadata)
        started_at = time.perf_counter()

        try:
            if hitl_config is not None:
                wait_metadata = {
                    "adapter": "pydantic_ai",
                    "tool_name": name,
                    "tool_call_id": ctx.tool_call_id,
                    "tool_args": tool_args,
                }
                with _suspend_checkpoint_scope():
                    result = wait(
                        schema=hitl_config.schema,
                        name=hitl_config.name or name,
                        question=hitl_config.question,
                        metadata=wait_metadata,
                    )
            else:
                result = await super().call_tool(name, tool_args, ctx, tool)
        except ApprovalRequired as error:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
            if event_id is not None and event_context is not None:
                tracker.fail_tool_event(event_id)
                log(
                    pydantic_ai_events={
                        event_id: self._build_tool_event_payload(
                            name=name,
                            event_context=event_context,
                            status="failed",
                            hitl=hitl_config is not None,
                            capture_config=capture_config,
                            duration_ms=duration_ms,
                            args_artifact=args_artifact,
                            result_artifact=None,
                            error=error,
                        )
                    }
                )
            raise KitaruUsageError(
                "PydanticAI deferred tool flows are not supported by "
                "kitaru.adapters.pydantic_ai. Use kp.hitl_tool(...) or "
                "an explicit kitaru.wait(...) in your flow."
            ) from error
        except CallDeferred as error:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
            if event_id is not None and event_context is not None:
                tracker.fail_tool_event(event_id)
                log(
                    pydantic_ai_events={
                        event_id: self._build_tool_event_payload(
                            name=name,
                            event_context=event_context,
                            status="failed",
                            hitl=hitl_config is not None,
                            capture_config=capture_config,
                            duration_ms=duration_ms,
                            args_artifact=args_artifact,
                            result_artifact=None,
                            error=error,
                        )
                    }
                )
            raise KitaruUsageError(
                "PydanticAI deferred tool flows are not supported by "
                "kitaru.adapters.pydantic_ai. Use kp.hitl_tool(...) or "
                "an explicit kitaru.wait(...) in your flow."
            ) from error
        except Exception as error:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
            if event_id is not None and event_context is not None:
                tracker.fail_tool_event(event_id)
                log(
                    pydantic_ai_events={
                        event_id: self._build_tool_event_payload(
                            name=name,
                            event_context=event_context,
                            status="failed",
                            hitl=hitl_config is not None,
                            capture_config=capture_config,
                            duration_ms=duration_ms,
                            args_artifact=args_artifact,
                            result_artifact=None,
                            error=error,
                        )
                    }
                )
            raise

        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)

        result_artifact: str | None = None
        if (
            capture_config.enabled
            and capture_config.save_result
            and event_id is not None
        ):
            result_artifact = f"{event_id}_result"
            _save_with_fallback(result_artifact, result, artifact_type="output")

        if event_id is not None and event_context is not None:
            tracker.complete_tool_event(event_id)
            log(
                pydantic_ai_events={
                    event_id: self._build_tool_event_payload(
                        name=name,
                        event_context=event_context,
                        status="completed",
                        hitl=hitl_config is not None,
                        capture_config=capture_config,
                        duration_ms=duration_ms,
                        args_artifact=args_artifact,
                        result_artifact=result_artifact,
                    )
                }
            )

        return result


class KitaruFunctionToolset(KitaruToolset[AgentDepsT]):
    """Kitaru wrapper for PydanticAI function toolsets."""

    def __init__(
        self,
        wrapped: FunctionToolset[AgentDepsT],
        *,
        tool_capture_config: CaptureConfig | None = None,
        tool_capture_config_by_name: dict[str, CaptureConfig | None] | None = None,
    ) -> None:
        super().__init__(
            wrapped,
            toolset_kind="function",
            tool_capture_config=tool_capture_config,
            tool_capture_config_by_name=tool_capture_config_by_name,
        )


class KitaruMCPToolset(KitaruToolset[AgentDepsT]):
    """Kitaru wrapper for PydanticAI MCP toolsets."""

    def __init__(
        self,
        wrapped: AbstractToolset[AgentDepsT],
        *,
        tool_capture_config: CaptureConfig | None = None,
        tool_capture_config_by_name: dict[str, CaptureConfig | None] | None = None,
    ) -> None:
        super().__init__(
            wrapped,
            toolset_kind="mcp",
            tool_capture_config=tool_capture_config,
            tool_capture_config_by_name=tool_capture_config_by_name,
        )


def kitaruify_toolset(
    toolset: AbstractToolset[AgentDepsT],
    *,
    tool_capture_config: CaptureConfig | None = None,
    tool_capture_config_by_name: dict[str, CaptureConfig | None] | None = None,
) -> AbstractToolset[AgentDepsT]:
    """Wrap a toolset with Kitaru tracking behavior and explicit dispatch."""
    if isinstance(toolset, KitaruWrapperToolset):
        return toolset

    if isinstance(toolset, FunctionToolset):
        return cast(
            AbstractToolset[AgentDepsT],
            KitaruFunctionToolset(
                cast(FunctionToolset[Any], toolset),
                tool_capture_config=tool_capture_config,
                tool_capture_config_by_name=tool_capture_config_by_name,
            ),
        )

    mcp_server_cls: type[Any] | None = None
    try:
        from pydantic_ai.mcp import MCPServer
    except ImportError:  # pragma: no cover - depends on optional install extras
        pass
    else:
        mcp_server_cls = MCPServer

    if mcp_server_cls is not None and isinstance(toolset, mcp_server_cls):
        return KitaruMCPToolset(
            toolset,
            tool_capture_config=tool_capture_config,
            tool_capture_config_by_name=tool_capture_config_by_name,
        )

    return KitaruToolset(
        toolset,
        toolset_kind="generic",
        tool_capture_config=tool_capture_config,
        tool_capture_config_by_name=tool_capture_config_by_name,
    )
