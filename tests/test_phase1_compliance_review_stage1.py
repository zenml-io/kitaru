"""Guarded tests for the compliance review Stage 1 flow boundary."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from tests.compliance_review_fakes import (
    clear_compliance_review_modules,
    fake_claude_response,
    install_fake_claude_agent_sdk,
)


@pytest.fixture
def stage1_module(monkeypatch):
    """Import Stage 1 with a tiny fake Claude SDK module.

    The normal test suite should not need the real `claude-agent-sdk` package
    or a live Anthropic call just to verify the Kitaru boundary. These tests
    monkeypatch `run_agent_turn()` before it can call the SDK.
    """
    install_fake_claude_agent_sdk(monkeypatch)
    clear_compliance_review_modules(
        "examples.compliance_review.stage_1_single_turn",
    )

    return importlib.import_module("examples.compliance_review.stage_1_single_turn")


def _fake_claude_response(*, prompt: str, cwd: Path) -> dict[str, Any]:
    """Build a Claude-shaped response without making a model call."""
    return fake_claude_response(
        prompt=prompt,
        cwd=cwd,
        session_id="stage-1-test-session",
    )


def test_stage1_checkpoint_wraps_one_stubbed_claude_turn(
    monkeypatch,
    stage1_module,
) -> None:
    """The checkpoint body should call one Claude turn and return the boundary."""
    calls: list[dict[str, Any]] = []
    log_calls: list[dict[str, Any]] = []

    async def fake_run_agent_turn(
        prompt: str,
        *,
        allowed_tools: list[str],
        cwd: Path,
    ) -> dict[str, Any]:
        calls.append({"prompt": prompt, "allowed_tools": allowed_tools, "cwd": cwd})
        return _fake_claude_response(prompt=prompt, cwd=cwd)

    monkeypatch.setattr(stage1_module, "run_agent_turn", fake_run_agent_turn)
    monkeypatch.setattr(
        stage1_module.kitaru,
        "log",
        lambda **kwargs: log_calls.append(kwargs),
    )

    # The public object is a Kitaru checkpoint. Calling __wrapped__ keeps this
    # test pure and avoids booting ZenML while still exercising the checkpoint
    # body that the flow will execute.
    result = stage1_module.check_it_security_policy.__wrapped__("custom stage 1 prompt")

    assert hasattr(stage1_module.check_it_security_policy, "submit")
    assert isinstance(result, stage1_module.ClaudeAgentResult)
    assert result.session_id == "stage-1-test-session"
    assert result.result == "Stubbed finding for: custom stage 1 prompt"
    assert result.num_turns == 1

    assert calls == [
        {
            "prompt": "custom stage 1 prompt",
            "allowed_tools": stage1_module.DEFAULT_ALLOWED_TOOLS,
            "cwd": stage1_module.EXAMPLE_DIR,
        }
    ]
    assert log_calls == [
        {
            "stage": "stage_1_single_turn",
            "domain": "it_security",
            "document": "it_security_policy",
            "standard": "soc2_controls",
            "checkpoint_boundary": "one_claude_turn",
        }
    ]


def test_stage1_run_workflow_uses_flow_run_and_wait(
    monkeypatch,
    stage1_module,
) -> None:
    """run_workflow() should go through the Kitaru flow boundary."""
    expected = stage1_module.ClaudeAgentResult(
        session_id="stage-1-test-session",
        cwd=str(stage1_module.EXAMPLE_DIR),
        transcript_path="/tmp/stage-1-test-session.jsonl",
        result="Stubbed flow result",
        num_turns=1,
    )
    fake_handle = Mock()
    fake_handle.wait = Mock(return_value=expected)
    fake_flow = Mock()
    fake_flow.run = Mock(return_value=fake_handle)
    monkeypatch.setattr(stage1_module, "it_policy_check", fake_flow)

    result = stage1_module.run_workflow("custom flow prompt")

    assert result == expected
    fake_flow.run.assert_called_once_with("custom flow prompt")
    fake_handle.wait.assert_called_once_with()


def test_stage1_flow_runs_one_checkpoint_with_stubbed_claude(
    monkeypatch,
    primed_zenml,
    stage1_module,
) -> None:
    """The decorated Kitaru flow should run one checkpoint boundary."""
    from zenml.client import Client

    calls: list[dict[str, Any]] = []

    async def fake_run_agent_turn(
        prompt: str,
        *,
        allowed_tools: list[str],
        cwd: Path,
    ) -> dict[str, Any]:
        calls.append({"prompt": prompt, "allowed_tools": allowed_tools, "cwd": cwd})
        return _fake_claude_response(prompt=prompt, cwd=cwd)

    monkeypatch.setattr(stage1_module, "run_agent_turn", fake_run_agent_turn)

    handle = stage1_module.it_policy_check.run("flow integration prompt")
    result = handle.wait()

    assert isinstance(result, stage1_module.ClaudeAgentResult)
    assert result.result == "Stubbed finding for: flow integration prompt"
    assert calls == [
        {
            "prompt": "flow integration prompt",
            "allowed_tools": stage1_module.DEFAULT_ALLOWED_TOOLS,
            "cwd": stage1_module.EXAMPLE_DIR,
        }
    ]

    run = Client().get_pipeline_run(handle.exec_id, allow_name_prefix_match=False)
    hydrated_run = run.get_hydrated_version()
    assert len(hydrated_run.steps) == 1

    step = next(iter(hydrated_run.steps.values()))
    assert step.run_metadata["checkpoint_boundary"] == "one_claude_turn"
    assert step.run_metadata["document"] == "it_security_policy"
