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


def _litellm_to_pydantic_ai_model(model_id: str) -> str:
    """Convert a LiteLLM model identifier to PydanticAI format.

    LiteLLM uses ``provider/model`` (slash), PydanticAI uses
    ``provider:model`` (colon).  Bare model names (no slash) are
    returned unchanged.
    """
    if "/" in model_id:
        return model_id.replace("/", ":", 1)
    return model_id


def resolve_model(model: str | None = None) -> tuple[str, dict[str, str]]:
    """Resolve a Kitaru model alias into a PydanticAI-compatible model ID.

    Thin wrapper around :func:`kitaru.llm.resolve_model` that converts
    the returned LiteLLM model identifier (``provider/model``) to the
    PydanticAI format (``provider:model``).

    Args:
        model: A Kitaru model alias, a concrete LiteLLM model identifier,
            or ``None`` to use the default registered model.

    Returns:
        A ``(model_id, env_overlay)`` tuple where *model_id* is ready to
        pass to ``pydantic_ai.Agent()``.

    Example::

        from kitaru.adapters.pydantic_ai import resolve_model

        model_id, env = resolve_model("fast")
        os.environ.update(env)
        agent = Agent(model_id, ...)
    """
    from kitaru.llm import resolve_model as _resolve_model

    litellm_model, env_overlay = _resolve_model(model)
    return _litellm_to_pydantic_ai_model(litellm_model), env_overlay


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


__all__ = ["CaptureConfig", "CaptureMode", "hitl_tool", "resolve_model", "wrap"]
