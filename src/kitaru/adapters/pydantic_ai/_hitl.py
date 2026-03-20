"""Human-in-the-loop tool markers for the PydanticAI adapter."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic_ai import FunctionToolset, Tool
from pydantic_ai.toolsets import AbstractToolset

_HITL_MARKER = "_kitaru_hitl_config"
_HITL_METADATA_KEY = "kitaru_hitl_config"


@dataclass(frozen=True)
class HitlConfig:
    """Configuration for adapter-managed HITL tool calls."""

    question: str | None = None
    name: str | None = None
    schema: Any = None


def _config_from_target(target: object) -> HitlConfig | None:
    """Read a HITL marker from a function or tool object."""
    config = getattr(target, _HITL_MARKER, None)
    if isinstance(config, HitlConfig):
        return config
    return None


def hitl_tool(
    *,
    question: str | None = None,
    name: str | None = None,
    schema: Any = None,
) -> Callable[[Any], Any]:
    """Mark a PydanticAI tool as requiring flow-level human input."""
    config = HitlConfig(question=question, name=name, schema=schema)

    def decorator(target: Any) -> Any:
        setattr(target, _HITL_MARKER, config)
        if isinstance(target, Tool):
            setattr(target.function, _HITL_MARKER, config)
        return target

    return decorator


def attach_hitl_metadata(toolset: AbstractToolset[Any]) -> None:
    """Copy HITL markers from function tools into runtime-visible metadata."""
    if not isinstance(toolset, FunctionToolset):
        return

    for tool in toolset.tools.values():
        config = _config_from_target(tool) or _config_from_target(tool.function)
        if config is None:
            continue

        metadata = dict(tool.metadata or {})
        metadata[_HITL_METADATA_KEY] = config
        tool.metadata = metadata


def hitl_config_from_tool_metadata(
    metadata: dict[str, Any] | None,
) -> HitlConfig | None:
    """Extract adapter HITL config from tool metadata."""
    if metadata is None:
        return None

    config = metadata.get(_HITL_METADATA_KEY)
    if isinstance(config, HitlConfig):
        return config
    return None
