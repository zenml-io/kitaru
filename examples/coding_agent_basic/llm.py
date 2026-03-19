"""Model setup for the coding agent.

Call ``setup()`` before using ``MODEL`` or ``credential_env()``.
Nothing runs at import time.
"""

import contextlib
import os
from collections.abc import Generator

from kitaru.config import register_model_alias, resolve_model_selection
from kitaru.llm import _resolve_credential_overlay  # noqa: PLC2701

MAX_TOOL_ROUNDS: int = int(os.environ.get("CODING_AGENT_MAX_TOOL_ROUNDS", "30"))

MODEL: str = ""
_env_overlay: dict[str, str] = {}
_setup_done = False


def setup() -> None:
    """Register model alias and resolve credentials. Call once before use."""
    global MODEL, _env_overlay, _setup_done
    if _setup_done:
        return

    register_model_alias(
        "coding-agent",
        model="anthropic/claude-sonnet-4-20250514",
        secret="anthropic-creds",
    )

    selection = resolve_model_selection(
        os.environ.get("CODING_AGENT_MODEL") or "coding-agent"
    )
    MODEL = selection.resolved_model
    _env_overlay, _ = _resolve_credential_overlay(selection)
    _setup_done = True


@contextlib.contextmanager
def credential_env() -> Generator[None, None, None]:
    """Temporarily inject provider credentials for the duration of a block."""
    setup()
    saved: dict[str, str | None] = {}
    try:
        for key, value in _env_overlay.items():
            saved[key] = os.environ.get(key)
            os.environ[key] = value
        yield
    finally:
        for key, prev in saved.items():
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev
