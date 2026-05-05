from collections.abc import Callable
from typing import Any

from pydantic import ConfigDict, field_serializer
from pydantic.dataclasses import dataclass
from pydantic_ai import Tool

_HITL_MARKER = '_kitaru_hitl_config'
_HITL_METADATA_KEY = 'kitaru_hitl_config'


@dataclass(frozen=True, config=ConfigDict(arbitrary_types_allowed=True))
class HitlConfig:
    question: str | None = None
    name: str | None = None
    # ``schema`` carries the *runtime* class for human-input validation (e.g.
    # ``bool``). It must remain the live class so ``kitaru.wait(schema=...)``
    # and ``request.schema is bool`` checks downstream still work. Pydantic AI
    # 1.86+ exposes per-tool ``metadata`` through ``AgentRunResult._state``,
    # so the surrounding agent result is now JSON-serialized end-to-end. The
    # serializer below converts unserializable type objects to a stable string
    # name only on the dump path; the in-memory value is unchanged.
    schema: Any = None
    question_arg: str | None = 'question'

    @field_serializer('schema')
    def _serialize_schema(self, value: Any, _info: Any) -> Any:
        if isinstance(value, type):
            return f'{value.__module__}.{value.__qualname__}'
        return value


def _config_from_target(target: object) -> HitlConfig | None:
    config = getattr(target, _HITL_MARKER, None)
    if isinstance(config, HitlConfig):
        return config
    return None


def resolve_hitl_question(
    config: HitlConfig,
    tool_args: dict[str, Any],
) -> str | None:
    """Pick the wait question: LLM-supplied tool arg wins, static decorator loses."""
    if config.question_arg:
        dynamic = tool_args.get(config.question_arg)
        if isinstance(dynamic, str) and dynamic.strip():
            return dynamic
    return config.question


def hitl_tool(
    *,
    question: str | None = None,
    name: str | None = None,
    schema: Any = None,
    question_arg: str | None = 'question',
) -> Callable[[Any], Any]:
    """Mark a tool as requiring a flow-level wait when called under Kitaru."""
    config = HitlConfig(
        question=question, name=name, schema=schema, question_arg=question_arg
    )

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


def hitl_config_from_tool_metadata(metadata: dict[str, Any] | None) -> HitlConfig | None:
    if metadata is None:
        return None
    config = metadata.get(_HITL_METADATA_KEY)
    if isinstance(config, HitlConfig):
        return config
    return None
