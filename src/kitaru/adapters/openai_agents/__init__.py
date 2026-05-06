"""OpenAI Agents SDK adapter for Kitaru durable execution."""

from kitaru.errors import KitaruFeatureNotAvailableError

try:
    import agents  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard only
    if exc.name != "agents":
        raise
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.openai_agents requires optional dependency "
        "`openai-agents`. Install with `uv sync --extra openai-agents` "
        "or `pip install 'kitaru[openai-agents]'`."
    ) from exc

from ._agent import KitaruRunner
from ._events import OpenAIAdapterEvent, OpenAIEventError, OpenAIRunEvent
from ._hitl import build_resume_request, wait_for_approval
from ._policy import OpenAICapturePolicy
from ._types import (
    OpenAIApprovalDecision,
    OpenAIInterruptionSummary,
    OpenAIRunRequest,
    OpenAIRunResult,
    OpenAIRunStateEnvelope,
    OpenAIUsageSummary,
)
from ._utils import (
    CheckpointConfig,
    CheckpointRuntime,
    ToolCheckpointOverride,
    validate_checkpoint_strategy,
)

__all__ = [
    "CheckpointConfig",
    "CheckpointRuntime",
    "KitaruRunner",
    "OpenAIAdapterEvent",
    "OpenAIApprovalDecision",
    "OpenAICapturePolicy",
    "OpenAIEventError",
    "OpenAIInterruptionSummary",
    "OpenAIRunEvent",
    "OpenAIRunRequest",
    "OpenAIRunResult",
    "OpenAIRunStateEnvelope",
    "OpenAIUsageSummary",
    "ToolCheckpointOverride",
    "build_resume_request",
    "validate_checkpoint_strategy",
    "wait_for_approval",
]
