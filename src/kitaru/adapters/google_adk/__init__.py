"""Google ADK adapter for Kitaru durable execution."""

import importlib

from kitaru.errors import KitaruFeatureNotAvailableError

try:
    importlib.import_module("google.adk")
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard only
    if exc.name not in {"google", "google.adk"}:
        raise
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.google_adk requires optional dependency `google-adk`. "
        "Install with `uv sync --extra google-adk` or "
        "`pip install 'kitaru[google-adk]'`."
    ) from exc

from ._agent import KitaruADKRunner
from ._events import ADKAdapterEvent, ADKEventError, ADKRunEvent
from ._hitl import (
    build_tool_confirmation_message,
    build_tool_confirmation_request,
    wait_for_tool_confirmation,
)
from ._model import KitaruADKModel
from ._plugin import KitaruADKPlugin
from ._policy import ADKCallCheckpointPolicy, ADKCapturePolicy
from ._tool import KitaruADKTool, wrap_tool
from ._types import (
    ADKHandoffRequest,
    ADKRunRequest,
    ADKRunResult,
    ADKUsageSummary,
    final_output_preview,
)
from ._utils import (
    CheckpointConfig,
    CheckpointRuntime,
    CheckpointStrategy,
    ToolCheckpointOverride,
    validate_checkpoint_strategy,
)

__all__ = [
    "ADKAdapterEvent",
    "ADKCallCheckpointPolicy",
    "ADKCapturePolicy",
    "ADKEventError",
    "ADKHandoffRequest",
    "ADKRunEvent",
    "ADKRunRequest",
    "ADKRunResult",
    "ADKUsageSummary",
    "CheckpointConfig",
    "CheckpointRuntime",
    "CheckpointStrategy",
    "KitaruADKModel",
    "KitaruADKPlugin",
    "KitaruADKRunner",
    "KitaruADKTool",
    "ToolCheckpointOverride",
    "build_tool_confirmation_message",
    "build_tool_confirmation_request",
    "final_output_preview",
    "validate_checkpoint_strategy",
    "wait_for_tool_confirmation",
    "wrap_tool",
]
