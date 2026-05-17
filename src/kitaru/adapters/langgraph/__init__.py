"""LangGraph adapter for Kitaru durable outer graph calls."""

from kitaru.errors import KitaruFeatureNotAvailableError

try:
    import langgraph  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard only
    if exc.name != "langgraph":
        raise
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.langgraph requires optional dependency `langgraph`. "
        "Install with `uv sync --extra langgraph` or "
        "`pip install 'kitaru[langgraph]'`."
    ) from exc

from ._agent import KitaruGraphRunner
from ._events import LangGraphAdapterEvent, LangGraphEventError, LangGraphRunEvent
from ._hitl import build_resume_request, wait_for_interrupt
from ._policy import LangGraphCapturePolicy, LangGraphDurabilityPolicy
from ._types import (
    LangGraphInterruptSummary,
    LangGraphPendingState,
    LangGraphResumeRequest,
    LangGraphRunRequest,
    LangGraphRunResult,
    LangGraphStateSummary,
    LangGraphUsageSummary,
)
from ._utils import (
    CheckpointConfig,
    CheckpointRuntime,
    GraphCheckpointStrategy,
    merge_config,
    validate_checkpoint_strategy,
)

__all__ = [
    "CheckpointConfig",
    "CheckpointRuntime",
    "GraphCheckpointStrategy",
    "KitaruGraphRunner",
    "LangGraphAdapterEvent",
    "LangGraphCapturePolicy",
    "LangGraphDurabilityPolicy",
    "LangGraphEventError",
    "LangGraphInterruptSummary",
    "LangGraphPendingState",
    "LangGraphResumeRequest",
    "LangGraphRunEvent",
    "LangGraphRunRequest",
    "LangGraphRunResult",
    "LangGraphStateSummary",
    "LangGraphUsageSummary",
    "build_resume_request",
    "merge_config",
    "validate_checkpoint_strategy",
    "wait_for_interrupt",
]
