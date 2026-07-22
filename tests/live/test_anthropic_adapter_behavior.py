# ruff: noqa: E402
"""Provider-extended live behavior checks for the Claude Agent SDK adapter."""

from typing import Any, cast
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.live_llm,
    pytest.mark.live_anthropic,
    pytest.mark.provider_extended,
]

pytest.importorskip("claude_agent_sdk")

from claude_agent_sdk import ClaudeAgentOptions

from kitaru import flow
from kitaru.adapters.claude_agent_sdk import (
    ClaudeCapturePolicy,
    ClaudeRunRequest,
    KitaruClaudeRunner,
)

_PROMPT = (
    "Answer in one short sentence: Claude Agent SDK live adapter behavior check. "
    "Do not use tools, Bash, or files."
)
# Pin the model and ignore local Claude Code settings: without this the
# bundled Claude Code CLI falls back to the developer's personal default-model
# setting (~/.claude), which the ANTHROPIC_API_KEY may not be able to access.
_MODEL = "claude-haiku-4-5-20251001"
_CAPTURE_USEFUL_ARTIFACTS = ClaudeCapturePolicy(
    emit_events=True,
    save_prompt=True,
    save_messages=True,
    save_transcript_file=False,
    save_options_manifest=True,
    save_final_output=True,
    save_usage=True,
)


def _pinned_model_options(request: ClaudeRunRequest) -> ClaudeAgentOptions:
    return ClaudeAgentOptions(
        model=_MODEL,
        cwd=request.cwd,
        max_turns=request.max_turns,
        setting_sources=[],
    )


def _runner(name: str) -> KitaruClaudeRunner:
    return KitaruClaudeRunner(
        name=name,
        options_factory=_pinned_model_options,
        capture=_CAPTURE_USEFUL_ARTIFACTS,
        checkpoint_config={"cache": False},
    )


@flow
def claude_live_stream_flow(nonce: str, cwd: str) -> dict[str, Any]:
    """Run one streaming Claude Agent SDK invocation as a real checkpoint."""
    request = ClaudeRunRequest.start(f"{_PROMPT} Nonce: {nonce}.", cwd=cwd, max_turns=1)
    result = _runner("kitaru-live-claude-stream").run_stream_sync(request)
    return result.model_dump(mode="json")


@flow
def claude_live_capture_flow(nonce: str, cwd: str) -> dict[str, Any]:
    """Run one non-streaming Claude Agent SDK invocation as a real checkpoint."""
    request = ClaudeRunRequest.start(f"{_PROMPT} Nonce: {nonce}.", cwd=cwd, max_turns=1)
    result = _runner("kitaru-live-claude-capture").run_sync(request)
    return result.model_dump(mode="json")


def test_claude_agent_sdk_live_streaming_records_event_artifacts(
    primed_zenml, tmp_path
) -> None:
    """The streaming path completes and records persisted lifecycle evidence."""
    handle = claude_live_stream_flow.run(uuid4().hex, str(tmp_path))
    result = cast(dict[str, Any], handle.wait())

    assert result["status"] == "completed"
    assert result["final_text"] is not None
    assert result["final_text"].strip()
    assert result["event_log_artifact_name"] is not None
    assert result["run_summary_artifact_name"] is not None


def test_claude_agent_sdk_live_capture_records_result_artifacts(
    primed_zenml, tmp_path
) -> None:
    """The non-streaming path records useful result metadata/artifact names."""
    handle = claude_live_capture_flow.run(uuid4().hex, str(tmp_path))
    result = cast(dict[str, Any], handle.wait())

    assert result["status"] == "completed"
    assert result["final_text"] is not None
    assert result["final_text"].strip()
    assert result["messages_artifact_name"] is not None
    assert result["options_manifest_artifact_name"] is not None
    assert result["output_artifact_name"] is not None
    assert result["event_log_artifact_name"] is not None
    assert result["run_summary_artifact_name"] is not None
