from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic_ai import Tool

_HITL_MARKER = "_kitaru_hitl_config"
_HITL_METADATA_KEY = "kitaru_hitl_config"


@dataclass(frozen=True)
class HitlConfig:
    question: str | None = None
    name: str | None = None
    schema: Any = None


def _config_from_target(target: object) -> HitlConfig | None:
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
    """Mark a tool as requiring a flow-level wait when called under Kitaru."""
    config = HitlConfig(question=question, name=name, schema=schema)

    def decorator(target: Any) -> Any:
        setattr(target, _HITL_MARKER, config)
        if isinstance(target, Tool):
            setattr(target.function, _HITL_MARKER, config)
        return target

    return decorator


def hitl_metadata_for_tool(tool: Tool[Any]) -> dict[str, Any] | None:
    config = _config_from_target(tool) or _config_from_target(tool.function)
    if config is None:
        return None
    return {_HITL_METADATA_KEY: config}


def hitl_config_from_tool_metadata(
    metadata: dict[str, Any] | None,
) -> HitlConfig | None:
    if metadata is None:
        return None
    config = metadata.get(_HITL_METADATA_KEY)
    if isinstance(config, HitlConfig):
        return config
    return None
