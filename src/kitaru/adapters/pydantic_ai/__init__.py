"""PydanticAI adapter for Kitaru durable execution. See README.md for the upstream delta."""

from __future__ import annotations

import warnings
from typing import Any, Literal, TypedDict

from kitaru.errors import KitaruFeatureNotAvailableError

from ._mcp_compat import ensure_pydantic_ai_mcp_import_compat

try:
    import pydantic_ai  # noqa: F401
except ImportError as exc:  # pragma: no cover - import-time guard only
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.pydantic_ai requires optional dependency "
        "`pydantic-ai-slim`. Install with `uv sync --extra pydantic-ai`."
    ) from exc

from ._agent import KitaruAgent
from ._function_toolset import KitaruFunctionToolset
from ._hitl import hitl_tool
from ._mcp_server import KitaruMCPServer, kitaruify_mcp_server
from ._model import KitaruModel
from ._policy import CaptureMode, CapturePolicy
from ._streaming import (
    PYDANTIC_AI_STREAM_COMPLETED,
    PYDANTIC_AI_STREAM_EVENT,
    PYDANTIC_AI_STREAM_EVENT_KINDS,
    PYDANTIC_AI_STREAM_FAILED,
    PYDANTIC_AI_STREAM_STARTED,
    PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS,
)
from ._toolset import KitaruToolset, kitaruify_toolset
from ._utils import (
    CheckpointConfig,
    CheckpointRuntime,
    CheckpointStrategy,
    validate_checkpoint_strategy,
)
from ._wait_for_input import wait_for_input

ensure_pydantic_ai_mcp_import_compat()

LegacyCaptureMode = Literal["full", "metadata_only", "off"]


class CaptureConfig(TypedDict, total=False):
    """Legacy compatibility type for `wrap(..., tool_capture_config=...)`."""

    mode: LegacyCaptureMode
    enabled: bool
    save_args: bool
    save_result: bool
    include_timings: bool


def _capture_mode_from_legacy(config: CaptureConfig | None) -> CaptureMode | None:
    if config is None:
        return None
    if config.get("enabled") is False:
        return None
    mode = config.get("mode")
    if mode == "off":
        return None
    if mode == "metadata_only":
        return "metadata"
    if mode == "full":
        return "full"
    save_args = config.get("save_args", True)
    save_result = config.get("save_result", True)
    return "full" if save_args or save_result else "metadata"


def wrap(
    agent: Any,
    *,
    name: str | None = None,
    tool_capture_config: CaptureConfig | None = None,
    tool_capture_config_by_name: dict[str, CaptureConfig | None] | None = None,
    **kwargs: Any,
) -> KitaruAgent[Any, Any]:
    """Deprecated shim for older adapter entrypoints."""
    warnings.warn(
        "`kitaru.adapters.pydantic_ai.wrap()` is deprecated; "
        "construct `KitaruAgent(...)` directly.",
        DeprecationWarning,
        stacklevel=2,
    )
    capture = kwargs.pop("capture", None)
    if capture is not None and (
        tool_capture_config is not None or tool_capture_config_by_name is not None
    ):
        raise TypeError(
            "Pass either `capture=` or legacy tool capture configs, not both."
        )
    if capture is None and (
        tool_capture_config is not None or tool_capture_config_by_name is not None
    ):
        capture = CapturePolicy(
            tool_capture=_capture_mode_from_legacy(tool_capture_config)
            if tool_capture_config is not None
            else "full",
            tool_capture_overrides={
                tool_name: _capture_mode_from_legacy(config)
                for tool_name, config in (tool_capture_config_by_name or {}).items()
            },
        )
    return KitaruAgent(agent, name=name, capture=capture, **kwargs)


__all__ = [
    "PYDANTIC_AI_STREAM_COMPLETED",
    "PYDANTIC_AI_STREAM_EVENT",
    "PYDANTIC_AI_STREAM_EVENT_KINDS",
    "PYDANTIC_AI_STREAM_FAILED",
    "PYDANTIC_AI_STREAM_STARTED",
    "PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS",
    "CaptureConfig",
    "CaptureMode",
    "CapturePolicy",
    "CheckpointConfig",
    "CheckpointRuntime",
    "CheckpointStrategy",
    "KitaruAgent",
    "KitaruFunctionToolset",
    "KitaruMCPServer",
    "KitaruModel",
    "KitaruToolset",
    "hitl_tool",
    "kitaruify_mcp_server",
    "kitaruify_toolset",
    "validate_checkpoint_strategy",
    "wait_for_input",
    "wrap",
]
