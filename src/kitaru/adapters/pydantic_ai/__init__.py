"""PydanticAI adapter for Kitaru durable execution."""

from __future__ import annotations

from typing import Any, Literal, TypedDict

from kitaru.errors import KitaruFeatureNotAvailableError

CaptureMode = Literal["full", "metadata_only", "off"]


class CaptureConfig(TypedDict, total=False):
    """Capture policy for adapter-managed tool call observability."""

    mode: CaptureMode
    enabled: bool
    save_args: bool
    save_result: bool
    include_timings: bool


def _require_pydantic_ai() -> None:
    """Ensure optional PydanticAI dependencies are available."""
    try:
        import pydantic_ai  # noqa: F401
    except ImportError as exc:  # pragma: no cover - depends on installation mode
        raise KitaruFeatureNotAvailableError(
            "kitaru.adapters.pydantic_ai requires optional dependency "
            "`pydantic-ai-slim`. Install with `uv sync --extra pydantic-ai`."
        ) from exc


def wrap(
    agent: Any,
    *,
    name: str | None = None,
    tool_capture_config: CaptureConfig | None = None,
    tool_capture_config_by_name: dict[str, CaptureConfig | None] | None = None,
) -> Any:
    """Wrap a PydanticAI agent for checkpoint child-event tracking."""
    _require_pydantic_ai()
    from ._agent import KitaruAgent

    return KitaruAgent(
        agent,
        name=name,
        tool_capture_config=tool_capture_config,
        tool_capture_config_by_name=tool_capture_config_by_name,
    )


def hitl_tool(
    *,
    question: str | None = None,
    name: str | None = None,
    schema: Any = bool,
) -> Any:
    """Mark a PydanticAI tool for flow-level HITL waits when invoked."""
    _require_pydantic_ai()
    from ._hitl import hitl_tool as _hitl_tool

    return _hitl_tool(question=question, name=name, schema=schema)


__all__ = ["CaptureConfig", "CaptureMode", "hitl_tool", "wrap"]
