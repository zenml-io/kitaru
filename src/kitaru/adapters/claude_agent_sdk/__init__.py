"""Claude Agent SDK adapter for Kitaru durable execution."""

from kitaru.errors import KitaruFeatureNotAvailableError

try:
    import claude_agent_sdk  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard only
    if exc.name != "claude_agent_sdk":
        raise
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.claude_agent_sdk requires optional dependency "
        "`claude-agent-sdk`. Install with `uv sync --extra claude-agent-sdk` "
        "or `pip install 'kitaru[claude-agent-sdk]'`."
    ) from exc

from ._agent import KitaruClaudeRunner
from ._events import ClaudeAdapterEvent, ClaudeEventError, ClaudeRunEvent
from ._policy import ClaudeCapturePolicy
from ._types import ClaudeRunRequest, ClaudeRunResult
from ._utils import (
    CheckpointConfig,
    CheckpointRuntime,
    validate_checkpoint_strategy,
)

__all__ = [
    "CheckpointConfig",
    "CheckpointRuntime",
    "ClaudeAdapterEvent",
    "ClaudeCapturePolicy",
    "ClaudeEventError",
    "ClaudeRunEvent",
    "ClaudeRunRequest",
    "ClaudeRunResult",
    "KitaruClaudeRunner",
    "validate_checkpoint_strategy",
]
