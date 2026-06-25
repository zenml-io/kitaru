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
from ._sandbox_tool import (
    DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS,
    KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME,
    KITARU_SANDBOX_COMMAND_TOOL_NAME,
    KITARU_SANDBOX_MCP_SERVER_NAME,
    allowed_tool_name,
    create_kitaru_sandbox_mcp_server,
)
from ._streaming import (
    CLAUDE_STREAM_COMPLETED,
    CLAUDE_STREAM_EVENT,
    CLAUDE_STREAM_EVENT_KINDS,
    CLAUDE_STREAM_FAILED,
    CLAUDE_STREAM_STARTED,
    CLAUDE_STREAM_TERMINAL_EVENT_KINDS,
)
from ._types import (
    ClaudeRunRequest,
    ClaudeRunResult,
    ClaudeUsageSummary,
)
from ._utils import (
    CheckpointConfig,
    CheckpointRuntime,
    validate_checkpoint_strategy,
)

__all__ = [
    "CLAUDE_STREAM_COMPLETED",
    "CLAUDE_STREAM_EVENT",
    "CLAUDE_STREAM_EVENT_KINDS",
    "CLAUDE_STREAM_FAILED",
    "CLAUDE_STREAM_STARTED",
    "CLAUDE_STREAM_TERMINAL_EVENT_KINDS",
    "DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS",
    "KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME",
    "KITARU_SANDBOX_COMMAND_TOOL_NAME",
    "KITARU_SANDBOX_MCP_SERVER_NAME",
    "CheckpointConfig",
    "CheckpointRuntime",
    "ClaudeAdapterEvent",
    "ClaudeCapturePolicy",
    "ClaudeEventError",
    "ClaudeRunEvent",
    "ClaudeRunRequest",
    "ClaudeRunResult",
    "ClaudeUsageSummary",
    "KitaruClaudeRunner",
    "allowed_tool_name",
    "create_kitaru_sandbox_mcp_server",
    "validate_checkpoint_strategy",
]
