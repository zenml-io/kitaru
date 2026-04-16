"""Focused tests for the compliance review Claude wrapper."""

from __future__ import annotations

import asyncio
import importlib
from pathlib import Path

import pytest

from tests.compliance_review_fakes import (
    clear_compliance_review_modules,
    configure_fake_claude_home,
    fake_claude_response,
    install_fake_claude_agent_sdk,
)


@pytest.fixture
def claude_agent_module(monkeypatch, tmp_path):
    """Import the example Claude wrapper with the fake SDK installed."""
    configure_fake_claude_home(monkeypatch, tmp_path)
    install_fake_claude_agent_sdk(monkeypatch)
    clear_compliance_review_modules()
    return importlib.import_module("examples.compliance_review.claude_agent")


def test_run_agent_turn_surfaces_result_error_before_transport_wrapper(
    monkeypatch,
    claude_agent_module,
) -> None:
    """A Claude result error should not be hidden by a later process exit."""

    async def fake_query(*, prompt, options):
        del prompt
        assert options.stderr is not None
        yield claude_agent_module.ResultMessage(
            subtype="error_max_plan",
            duration_ms=1,
            duration_api_ms=1,
            is_error=True,
            num_turns=1,
            session_id="limit-hit-session",
            result=(
                "API Error: 400 workspace API usage limits reached; "
                "regain access on 2026-05-01 at 00:00 UTC."
            ),
            stop_reason="stop_sequence",
        )
        raise Exception(
            "Command failed with exit code 1 (exit code: 1)\n"
            "Error output: Check stderr output for details"
        )

    monkeypatch.setattr(claude_agent_module, "query", fake_query)

    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(claude_agent_module.run_agent_turn("test prompt"))

    message = str(exc_info.value)
    assert "workspace API usage limits reached" in message
    assert "Claude Agent SDK turn failed:" in message


def test_run_agent_turn_includes_stderr_for_transport_failures(
    monkeypatch,
    claude_agent_module,
) -> None:
    """Collected Claude CLI stderr should be attached to transport errors."""

    async def fake_query(*, prompt, options):
        del prompt
        assert options.stderr is not None
        options.stderr("Authentication failed for bundled Claude CLI")
        raise Exception("Command failed with exit code 1")
        yield  # pragma: no cover

    monkeypatch.setattr(claude_agent_module, "query", fake_query)

    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(claude_agent_module.run_agent_turn("test prompt"))

    message = str(exc_info.value)
    assert "Claude Agent SDK transport failed:" in message
    assert "Claude CLI stderr:" in message
    assert "Authentication failed for bundled Claude CLI" in message


def test_claude_agent_result_materializer_restores_transcript(
    tmp_path,
    claude_agent_module,
) -> None:
    """The example materializer should bundle and restore Claude JSONL state."""
    materializers = importlib.import_module("examples.compliance_review.materializers")
    response = fake_claude_response(
        prompt="Check whether resume state survives.",
        cwd=tmp_path,
        session_id="materializer-test-session",
        result="Durable transcript result.",
    )
    result = claude_agent_module.to_claude_agent_result(response)
    transcript_path = Path(result.transcript_path)
    original_transcript = transcript_path.read_text()

    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    materializer = materializers.ClaudeAgentResultMaterializer(uri=str(artifact_dir))

    materializer.save(result)
    transcript_path.unlink()
    assert not transcript_path.exists()

    loaded = materializer.load(claude_agent_module.ClaudeAgentResult)

    assert loaded == result
    assert transcript_path.exists()
    assert transcript_path.read_text() == original_transcript
