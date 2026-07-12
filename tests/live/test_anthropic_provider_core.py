# ruff: noqa: E402
"""Low-cost live Anthropic/Claude Agent SDK provider checks.

These tests are excluded from default pytest runs by the ``live_llm`` marker.
They are intended for trusted manual/weekly runs with provider credentials.
"""

import pytest

pytestmark = [pytest.mark.live_llm, pytest.mark.live_anthropic]

pytest.importorskip("claude_agent_sdk")

from kitaru.adapters.claude_agent_sdk import (
    ClaudeCapturePolicy,
    ClaudeRunRequest,
    KitaruClaudeRunner,
)
from kitaru.runtime import _checkpoint_scope

_PROMPT = (
    "Explain one Kitaru checkpoint in one short sentence. "
    "Do not use tools, Bash, or files."
)
_CAPTURE_NOTHING = ClaudeCapturePolicy(
    emit_events=False,
    save_prompt=False,
    save_messages=False,
    save_transcript_file=False,
    save_options_manifest=False,
    save_final_output=False,
    save_usage=False,
)


def _runner() -> KitaruClaudeRunner:
    return KitaruClaudeRunner(
        name="kitaru-live-claude-core",
        capture=_CAPTURE_NOTHING,
        allow_direct_execution_inside_checkpoint=True,
    )


def test_claude_agent_sdk_adapter_basic_run_completes(tmp_path) -> None:
    """The Claude Agent SDK adapter can run one bounded provider call."""
    request = ClaudeRunRequest.start(_PROMPT, cwd=str(tmp_path), max_turns=1)

    with _checkpoint_scope(
        name="live_claude_provider_core", checkpoint_type="agent_call"
    ):
        result = _runner().run_sync(request)

    assert result.status == "completed"
    assert result.final_text is not None
    assert result.final_text.strip()


def test_claude_agent_sdk_adapter_streaming_run_completes(tmp_path) -> None:
    """The Claude Agent SDK streaming adapter can run one bounded provider call."""
    request = ClaudeRunRequest.start(_PROMPT, cwd=str(tmp_path), max_turns=1)

    with _checkpoint_scope(
        name="live_claude_provider_core_stream",
        checkpoint_type="agent_call",
    ):
        result = _runner().run_stream_sync(request)

    assert result.status == "completed"
    assert result.final_text is not None
    assert result.final_text.strip()
