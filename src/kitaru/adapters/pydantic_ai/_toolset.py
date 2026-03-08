"""Toolset interception for Kitaru's PydanticAI adapter."""

from __future__ import annotations

from typing import Any

from pydantic_ai import AbstractToolset, CallDeferred, ToolsetTool, WrapperToolset
from pydantic_ai.exceptions import ApprovalRequired
from pydantic_ai.tools import AgentDepsT, RunContext

from kitaru.artifacts import save
from kitaru.errors import KitaruUsageError
from kitaru.logging import log
from kitaru.runtime import _is_inside_checkpoint, _suspend_checkpoint_scope
from kitaru.wait import wait

from ._hitl import hitl_config_from_tool_metadata
from ._tracking import get_current_tracker


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


def kitaruify_toolset(
    toolset: AbstractToolset[AgentDepsT],
) -> AbstractToolset[AgentDepsT]:
    """Wrap a leaf toolset with Kitaru tracking behavior."""
    if isinstance(toolset, KitaruToolset):
        return toolset
    return KitaruToolset(toolset)


class KitaruToolset(WrapperToolset[AgentDepsT]):
    """Toolset wrapper that records tool calls as checkpoint child events."""

    @property
    def id(self) -> str | None:
        return self.wrapped.id

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

        event_id = tracker.next_event_id("tool_call")
        parent_event_ids = tracker.mark_tool_start(event_id)
        args_artifact = f"{event_id}_args"
        result_artifact = f"{event_id}_result"
        _save_with_fallback(args_artifact, tool_args, artifact_type="input")

        hitl_config = hitl_config_from_tool_metadata(tool.tool_def.metadata)

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
        except (ApprovalRequired, CallDeferred) as error:
            log(
                pydantic_ai_events={
                    event_id: {
                        "type": "tool_call",
                        "tool_name": name,
                        "status": "failed",
                        "hitl": hitl_config is not None,
                        "parent_event_ids": parent_event_ids,
                        "artifacts": {"args": args_artifact},
                        "error": _error_payload(error),
                    }
                }
            )
            raise KitaruUsageError(
                "PydanticAI deferred tool flows are not supported by "
                "kitaru.adapters.pydantic_ai. Use kp.hitl_tool(...) or "
                "an explicit kitaru.wait(...) in your flow."
            ) from error
        except Exception as error:
            log(
                pydantic_ai_events={
                    event_id: {
                        "type": "tool_call",
                        "tool_name": name,
                        "status": "failed",
                        "hitl": hitl_config is not None,
                        "parent_event_ids": parent_event_ids,
                        "artifacts": {"args": args_artifact},
                        "error": _error_payload(error),
                    }
                }
            )
            raise

        _save_with_fallback(result_artifact, result, artifact_type="output")
        log(
            pydantic_ai_events={
                event_id: {
                    "type": "tool_call",
                    "tool_name": name,
                    "status": "completed",
                    "hitl": hitl_config is not None,
                    "parent_event_ids": parent_event_ids,
                    "artifacts": {
                        "args": args_artifact,
                        "result": result_artifact,
                    },
                }
            }
        )
        return result
