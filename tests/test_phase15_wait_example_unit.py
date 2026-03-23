"""Unit tests for the wait/resume example."""

from __future__ import annotations

from unittest.mock import Mock

from examples.execution_management import wait_and_resume


def test_run_workflow_calls_flow_run(monkeypatch) -> None:
    """run_workflow should call .run() and return its result."""
    fake_flow = Mock()
    fake_flow.run = Mock(return_value="handle")
    monkeypatch.setattr(wait_and_resume, "wait_for_approval_flow", fake_flow)

    result = wait_and_resume.run_workflow(topic="v1.0")

    fake_flow.run.assert_called_once_with("v1.0")
    assert result == "handle"


def test_main_calls_run_workflow(monkeypatch) -> None:
    """main() should call run_workflow."""
    mock_run = Mock(return_value=None)
    monkeypatch.setattr(wait_and_resume, "run_workflow", mock_run)

    wait_and_resume.main()

    mock_run.assert_called_once()
