"""Model setup for the coding agent."""

import os

from kitaru.config import register_model_alias, resolve_model_selection
from kitaru.llm import _resolve_credential_overlay

# ---------------------------------------------------------------------------
# Model registration + credential resolution (runs once at import time)
# ---------------------------------------------------------------------------

register_model_alias(
    "coding-agent",
    model="anthropic/claude-sonnet-4-20250514",
    secret="anthropic-creds",
)

_selection = resolve_model_selection(
    os.environ.get("CODING_AGENT_MODEL") or "coding-agent"
)
MODEL: str = _selection.resolved_model
_ENV_OVERLAY, _ = _resolve_credential_overlay(_selection)
os.environ.update(_ENV_OVERLAY)

MAX_TOOL_ROUNDS: int = int(os.environ.get("CODING_AGENT_MAX_TOOL_ROUNDS", "30"))
