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
from ._policy import (
    LangGraphCallCheckpointPolicy,
    LangGraphCapturePolicy,
    LangGraphDurabilityPolicy,
    LangGraphStreamPolicy,
)
from ._sandbox_tool import (
    DEFAULT_SANDBOX_COMMAND_TOOL_MAX_CHARS,
    DEFAULT_SANDBOX_COMMAND_TOOL_NAME,
    SandboxCommandToolArgs,
    create_sandbox_command_tool,
)
from ._streaming import (
    LANGGRAPH_STREAM_CHECKPOINTS,
    LANGGRAPH_STREAM_COMPLETED,
    LANGGRAPH_STREAM_CUSTOM,
    LANGGRAPH_STREAM_DEBUG,
    LANGGRAPH_STREAM_EVENT_KINDS,
    LANGGRAPH_STREAM_FAILED,
    LANGGRAPH_STREAM_MESSAGES,
    LANGGRAPH_STREAM_STARTED,
    LANGGRAPH_STREAM_TASKS,
    LANGGRAPH_STREAM_TERMINAL_EVENT_KINDS,
    LANGGRAPH_STREAM_UPDATES,
    LANGGRAPH_STREAM_VALUES,
)
from ._types import (
    LangGraphInterruptSummary,
    LangGraphPendingState,
    LangGraphResumeRequest,
    LangGraphRunRequest,
    LangGraphRunResult,
    LangGraphStateSummary,
    LangGraphStreamMode,
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
    "DEFAULT_SANDBOX_COMMAND_TOOL_MAX_CHARS",
    "DEFAULT_SANDBOX_COMMAND_TOOL_NAME",
    "LANGGRAPH_STREAM_CHECKPOINTS",
    "LANGGRAPH_STREAM_COMPLETED",
    "LANGGRAPH_STREAM_CUSTOM",
    "LANGGRAPH_STREAM_DEBUG",
    "LANGGRAPH_STREAM_EVENT_KINDS",
    "LANGGRAPH_STREAM_FAILED",
    "LANGGRAPH_STREAM_MESSAGES",
    "LANGGRAPH_STREAM_STARTED",
    "LANGGRAPH_STREAM_TASKS",
    "LANGGRAPH_STREAM_TERMINAL_EVENT_KINDS",
    "LANGGRAPH_STREAM_UPDATES",
    "LANGGRAPH_STREAM_VALUES",
    "CheckpointConfig",
    "CheckpointRuntime",
    "GraphCheckpointStrategy",
    "KitaruGraphRunner",
    "LangGraphAdapterEvent",
    "LangGraphCallCheckpointPolicy",
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
    "LangGraphStreamMode",
    "LangGraphStreamPolicy",
    "LangGraphUsageSummary",
    "SandboxCommandToolArgs",
    "build_resume_request",
    "create_sandbox_command_tool",
    "merge_config",
    "validate_checkpoint_strategy",
    "wait_for_interrupt",
]
