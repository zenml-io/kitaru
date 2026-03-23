"""Unit tests for the wait/resume example."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from examples.execution_management import wait_and_resume


def test_run_workflow_calls_flow_and_returns_result(monkeypatch) -> None:
    """run_workflow should call .run().wait() and return its final result."""
    fake_handle = SimpleNamespace(wait=Mock(return_value="done"))
    fake_flow = Mock()
    fake_flow.run = Mock(return_value=fake_handle)
    monkeypatch.setattr(wait_and_resume, "wait_for_approval_flow", fake_flow)

    result = wait_and_resume.run_workflow(topic="v1.0")

    fake_flow.run.assert_called_once_with("v1.0")
    fake_handle.wait.assert_called_once()
    assert result == "done"


def test_main_prints_result(monkeypatch, capsys) -> None:
    """main() should run the workflow and print the result."""
    monkeypatch.setattr(
        wait_and_resume,
        "run_workflow",
        Mock(return_value="PUBLISHED: test"),
    )

    wait_and_resume.main()

    wait_and_resume.run_workflow.assert_called_once()  # type: ignore[union-attr]
    output = capsys.readouterr().out
    assert "Result: PUBLISHED: test" in output
