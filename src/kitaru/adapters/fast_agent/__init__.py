"""fast-agent adapter for Kitaru calls-mode wrapping."""

from kitaru.errors import KitaruFeatureNotAvailableError

try:
    import fast_agent  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard only
    if exc.name != "fast_agent":
        raise
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.fast_agent requires optional dependency "
        "`fast-agent-mcp`. Install it before importing this adapter."
    ) from exc

from ._agent import KitaruFastAgent
from ._utils import CheckpointConfig
from ._wrapping import (
    FastAgentCall,
    FastAgentCallRecorder,
    KitaruFastAgentCallRecorder,
    kitaru_call_recorder,
    passthrough_call_recorder,
    wrap_fast_agent_agent,
    wrap_fast_agent_app,
)

__all__ = [
    "CheckpointConfig",
    "FastAgentCall",
    "FastAgentCallRecorder",
    "KitaruFastAgent",
    "KitaruFastAgentCallRecorder",
    "kitaru_call_recorder",
    "passthrough_call_recorder",
    "wrap_fast_agent_agent",
    "wrap_fast_agent_app",
]
