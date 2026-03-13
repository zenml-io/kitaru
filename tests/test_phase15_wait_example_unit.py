"""Unit tests for the Phase 15 wait/resume example."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from examples.execution_management import wait_and_resume


def test_watch_prints_manual_unblock_commands(monkeypatch, capsys) -> None:
    """Watcher should emit CLI commands once the pending wait becomes visible."""
    stop_event = wait_and_resume.threading.Event()

    wait_lookup = Mock(side_effect=[None, ("kr-15", "wait-uuid")])
    monkeypatch.setattr(wait_and_resume, "_find_pending_wait_for_topic", wait_lookup)
    monkeypatch.setattr(wait_and_resume.time, "sleep", lambda _: None)

    wait_and_resume._watch_and_print_unblock_commands(
        client=Mock(),
        topic="kitaru",
        stop_event=stop_event,
    )

    output = capsys.readouterr().out
    assert "kitaru executions input kr-15 --wait wait-uuid --value true" in output
    assert "kitaru executions resume kr-15" in output


def test_run_workflow_calls_flow_and_returns_result(monkeypatch) -> None:
    """run_workflow should call .run().wait() and return its final result."""
    fake_client = Mock()
    monkeypatch.setattr(
        wait_and_resume,
        "KitaruClient",
        Mock(return_value=fake_client),
    )
    monkeypatch.setattr(
        wait_and_resume,
        "_watch_and_print_unblock_commands",
        lambda **_: None,
    )

    fake_handle = SimpleNamespace(wait=Mock(return_value="done"))
    fake_flow = Mock()
    fake_flow.run = Mock(return_value=fake_handle)
    monkeypatch.setattr(wait_and_resume, "wait_for_approval_flow", fake_flow)

    result = wait_and_resume.run_workflow(topic="kitaru")

    fake_flow.run.assert_called_once_with("kitaru")
    fake_handle.wait.assert_called_once()
    assert result == "done"


def test_main_parses_topic_and_prints_result(monkeypatch, capsys) -> None:
    """main() should pass --topic through to run_workflow and print result."""
    monkeypatch.setattr(
        wait_and_resume,
        "run_workflow",
        Mock(return_value="PUBLISHED: test"),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["wait_and_resume", "--topic", "my-topic"],
    )

    wait_and_resume.main()

    wait_and_resume.run_workflow.assert_called_once_with(topic="my-topic")  # type: ignore[union-attr]
    output = capsys.readouterr().out
    assert "Result: PUBLISHED: test" in output
