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


def test_resolve_claude_transcript_path_matches_documented_layout(
    monkeypatch,
    tmp_path,
    claude_agent_module,
) -> None:
    """Pin the production encoder to the documented Claude session layout.

    The fakes in `compliance_review_fakes.py` mirror the production encoder by
    construction, so round-trip tests that only use the fake cannot catch
    drift. Assert the output against hardcoded expectations — if production
    changes the encoding character class or the ``.claude/projects/`` layout,
    this test breaks independently of the fake.
    """
    fake_home = tmp_path / "pinned_home"
    fake_home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: fake_home)

    # The basename mixes every character class the encoder should collapse:
    # letters, digits, dot, underscore, space, punctuation. Every non-
    # alphanumeric character must become a single '-'.
    cwd = tmp_path / "app_v1.2 test!"
    cwd.mkdir()

    path = claude_agent_module.resolve_claude_transcript_path(
        "session-abc",
        cwd=cwd,
    )

    assert path.startswith(str(fake_home) + "/.claude/projects/")
    # Path separators and special chars in the basename all collapse to '-'.
    assert path.endswith("-app-v1-2-test-/session-abc.jsonl")


def test_resolve_claude_transcript_path_rejects_non_ascii_cwd(
    tmp_path,
    claude_agent_module,
) -> None:
    """Non-ASCII cwds must raise instead of silently producing a dead path."""
    cwd = tmp_path / "proyecto-españa"
    cwd.mkdir()

    with pytest.raises(ValueError, match="non-ASCII"):
        claude_agent_module.resolve_claude_transcript_path("session-x", cwd=cwd)


def test_materializer_recomputes_transcript_path_on_different_home(
    monkeypatch,
    tmp_path,
    claude_agent_module,
) -> None:
    """Restore should land at the current host's path, not the saved one.

    On remote stacks, save and load may run on different pods with different
    ``Path.home()`` values. The materializer must recompute the destination so
    Claude's ``resume=<session_id>`` lookup finds the restored JSONL on the
    new host.
    """
    materializers = importlib.import_module("examples.compliance_review.materializers")

    pod_a_home = tmp_path / "pod_a_home"
    pod_a_home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: pod_a_home)

    cwd = tmp_path / "shared-project"
    cwd.mkdir()

    response = fake_claude_response(
        prompt="Turn 1 under pod A's home directory.",
        cwd=cwd,
        session_id="cross-pod-session",
        result="Pod A result.",
    )
    result = claude_agent_module.to_claude_agent_result(response)
    saved_transcript = Path(result.transcript_path).read_text()
    saved_transcript_path = result.transcript_path
    assert str(pod_a_home) in saved_transcript_path

    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    materializer = materializers.ClaudeAgentResultMaterializer(uri=str(artifact_dir))
    materializer.save(result)

    pod_b_home = tmp_path / "pod_b_home"
    pod_b_home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: pod_b_home)

    loaded = materializer.load(claude_agent_module.ClaudeAgentResult)

    expected_path = claude_agent_module.resolve_claude_transcript_path(
        "cross-pod-session",
        cwd=result.cwd,
    )
    assert loaded.transcript_path == expected_path
    assert str(pod_b_home) in expected_path
    assert str(pod_a_home) not in expected_path
    assert Path(expected_path).read_text() == saved_transcript
