"""Focused tests for the compliance review Claude wrapper."""

from __future__ import annotations

import asyncio
import importlib

import pytest

from tests.compliance_review_fakes import (
    clear_compliance_review_modules,
    install_fake_claude_agent_sdk,
)


@pytest.fixture
def claude_agent_module(monkeypatch):
    """Import the example Claude wrapper with the fake SDK installed."""
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
