"""PydanticAI adapter for Kitaru durable execution. See README.md for the upstream delta."""

from __future__ import annotations

from kitaru.errors import KitaruFeatureNotAvailableError

try:
    import pydantic_ai  # noqa: F401
except ImportError as exc:  # pragma: no cover - import-time guard only
    raise KitaruFeatureNotAvailableError(
        'kitaru.adapters.pydantic_ai requires optional dependency '
        '`pydantic-ai-slim`. Install with `uv sync --extra pydantic-ai`.'
    ) from exc

from ._agent import KitaruAgent
from ._function_toolset import KitaruFunctionToolset
from ._hitl import hitl_tool
from ._mcp_server import KitaruMCPServer
from ._model import KitaruModel
from ._policy import CaptureMode, CapturePolicy
from ._run_context import KitaruRunContext
from ._toolset import KitaruToolset, kitaruify_toolset
from ._utils import CheckpointConfig, CheckpointRuntime

__all__ = [
    'CaptureMode',
    'CapturePolicy',
    'CheckpointConfig',
    'CheckpointRuntime',
    'KitaruAgent',
    'KitaruFunctionToolset',
    'KitaruMCPServer',
    'KitaruModel',
    'KitaruRunContext',
    'KitaruToolset',
    'hitl_tool',
    'kitaruify_toolset',
]
