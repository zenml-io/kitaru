# ruff: noqa: E402
"""Provider-extended live behavior checks for the Claude Agent SDK adapter."""

import pytest

pytestmark = [
    pytest.mark.live_llm,
    pytest.mark.live_anthropic,
    pytest.mark.provider_extended,
]

pytest.importorskip("claude_agent_sdk")

from kitaru.adapters.claude_agent_sdk import (
    ClaudeCapturePolicy,
    ClaudeRunRequest,
    KitaruClaudeRunner,
)
from kitaru.runtime import _checkpoint_scope

_PROMPT = (
    "Answer in one short sentence: Claude Agent SDK live adapter behavior check. "
    "Do not use tools, Bash, or files."
)
_CAPTURE_USEFUL_ARTIFACTS = ClaudeCapturePolicy(
    emit_events=True,
    save_prompt=True,
    save_messages=True,
    save_transcript_file=False,
    save_options_manifest=True,
    save_final_output=True,
    save_usage=True,
)


def _runner(name: str) -> KitaruClaudeRunner:
    return KitaruClaudeRunner(
        name=name,
        capture=_CAPTURE_USEFUL_ARTIFACTS,
        allow_direct_execution_inside_checkpoint=True,
    )


def test_claude_agent_sdk_live_streaming_records_event_artifacts(tmp_path) -> None:
    """The streaming path completes and records persisted lifecycle evidence."""
    request = ClaudeRunRequest.start(_PROMPT, cwd=str(tmp_path), max_turns=1)

    with _checkpoint_scope(
        name="live_claude_adapter_behavior_stream",
        checkpoint_type="agent_call",
    ):
        result = _runner("kitaru-live-claude-stream").run_stream_sync(request)

    assert result.status == "completed"
    assert result.final_text is not None
    assert result.final_text.strip()
    assert result.event_log_artifact_name is not None
    assert result.run_summary_artifact_name is not None


def test_claude_agent_sdk_live_capture_records_result_artifacts(tmp_path) -> None:
    """The non-streaming path records useful result metadata/artifact names."""
    request = ClaudeRunRequest.start(_PROMPT, cwd=str(tmp_path), max_turns=1)

    with _checkpoint_scope(
        name="live_claude_adapter_behavior_capture",
        checkpoint_type="agent_call",
    ):
        result = _runner("kitaru-live-claude-capture").run_sync(request)

    assert result.status == "completed"
    assert result.final_text is not None
    assert result.final_text.strip()
    assert result.messages_artifact_name is not None
    assert result.options_manifest_artifact_name is not None
    assert result.output_artifact_name is not None
    assert result.event_log_artifact_name is not None
    assert result.run_summary_artifact_name is not None
