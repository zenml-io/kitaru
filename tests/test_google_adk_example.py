"""Deterministic coverage for the Google ADK adapter example."""

import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from examples.integrations.google_adk_agent import google_adk_adapter as example
from examples.integrations.google_adk_agent import (
    google_adk_workflow as workflow_example,
)


def _install_no_hosted_provider_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    real_connect = socket.socket.connect

    def guarded_connect(sock: socket.socket, address: Any) -> Any:
        host = address[0] if isinstance(address, tuple) and address else address
        if isinstance(host, bytes):
            host = host.decode()
        if host not in {"127.0.0.1", "::1", "localhost"}:
            raise AssertionError(
                "Google ADK example local mode must not open network "
                f"connections: {address!r}"
            )
        return real_connect(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)


def test_google_adk_example_help_does_not_require_google_adk(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        example.main(["--help"])

    assert exc_info.value.code == 0
    assert "experimental ADK adapter" in capsys.readouterr().out


def test_google_adk_workflow_help_does_not_require_google_adk(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        workflow_example.main(["--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "persisted Kitaru workflow" in output
    assert "--mode" in output


def test_google_adk_workflow_help_runs_when_executed_by_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "examples/integrations/google_adk_agent/google_adk_workflow.py"

    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "persisted Kitaru workflow" in result.stdout
    assert "--mode" in result.stdout


@pytest.mark.parametrize("local_only_flag", ["--deny", "--interactive-wait"])
def test_google_adk_workflow_live_mode_rejects_local_confirmation_flags(
    local_only_flag: str,
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        workflow_example.main(["--mode", "live", local_only_flag])

    assert "only apply to --mode local" in str(exc_info.value)


def _clear_google_adk_auth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_name in (
        example.GEMINI_API_KEY_ENV,
        example.GOOGLE_API_KEY_ENV,
        example.VERTEXAI_ENV,
        example.CLOUD_PROJECT_ENV,
        example.CLOUD_LOCATION_ENV,
    ):
        monkeypatch.delenv(env_name, raising=False)


def test_google_adk_live_credentials_alias_gemini_key_to_google_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_google_adk_auth_env(monkeypatch)
    monkeypatch.setenv(example.GEMINI_API_KEY_ENV, "gemini-key")

    example.prepare_live_google_credentials()

    assert os.environ[example.GOOGLE_API_KEY_ENV] == "gemini-key"


def test_google_adk_live_credentials_accept_google_key_without_gemini_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_google_adk_auth_env(monkeypatch)
    monkeypatch.setenv(example.GOOGLE_API_KEY_ENV, "google-key")

    example.prepare_live_google_credentials()

    assert example.GEMINI_API_KEY_ENV not in os.environ
    assert os.environ[example.GOOGLE_API_KEY_ENV] == "google-key"


@pytest.mark.parametrize("truthy_value", ["1", "true", "TRUE", "yes", "on"])
def test_google_adk_live_credentials_accept_vertex_without_api_keys(
    monkeypatch: pytest.MonkeyPatch,
    truthy_value: str,
) -> None:
    _clear_google_adk_auth_env(monkeypatch)
    monkeypatch.setenv(example.VERTEXAI_ENV, truthy_value)
    monkeypatch.setenv(example.CLOUD_PROJECT_ENV, "demo-project")
    monkeypatch.setenv(example.CLOUD_LOCATION_ENV, "europe-north1")

    example.prepare_live_google_credentials()


@pytest.mark.parametrize(
    "missing_env_names",
    [
        (example.CLOUD_PROJECT_ENV,),
        (example.CLOUD_LOCATION_ENV,),
        (example.CLOUD_PROJECT_ENV, example.CLOUD_LOCATION_ENV),
    ],
)
def test_google_adk_live_credentials_vertex_names_missing_settings(
    monkeypatch: pytest.MonkeyPatch,
    missing_env_names: tuple[str, ...],
) -> None:
    _clear_google_adk_auth_env(monkeypatch)
    monkeypatch.setenv(example.VERTEXAI_ENV, "true")
    monkeypatch.setenv(example.CLOUD_PROJECT_ENV, "demo-project")
    monkeypatch.setenv(example.CLOUD_LOCATION_ENV, "europe-north1")
    for env_name in missing_env_names:
        monkeypatch.delenv(env_name, raising=False)

    with pytest.raises(SystemExit) as exc_info:
        example.prepare_live_google_credentials()

    message = str(exc_info.value)
    for env_name in missing_env_names:
        assert env_name in message
    assert "no API key" in message


def test_google_adk_live_credentials_vertex_mode_takes_precedence_over_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_google_adk_auth_env(monkeypatch)
    monkeypatch.setenv(example.VERTEXAI_ENV, "true")
    monkeypatch.setenv(example.GOOGLE_API_KEY_ENV, "google-key")

    with pytest.raises(SystemExit) as exc_info:
        example.prepare_live_google_credentials()

    message = str(exc_info.value)
    assert example.CLOUD_PROJECT_ENV in message
    assert example.CLOUD_LOCATION_ENV in message


def test_google_adk_live_credentials_false_vertex_value_needs_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_google_adk_auth_env(monkeypatch)
    monkeypatch.setenv(example.VERTEXAI_ENV, "false")
    monkeypatch.setenv(example.CLOUD_PROJECT_ENV, "demo-project")
    monkeypatch.setenv(example.CLOUD_LOCATION_ENV, "europe-north1")

    with pytest.raises(SystemExit) as exc_info:
        example.prepare_live_google_credentials()

    assert "Missing Google/Gemini credentials" in str(exc_info.value)


def test_google_adk_live_credentials_missing_message_lists_both_auth_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_google_adk_auth_env(monkeypatch)

    with pytest.raises(SystemExit) as exc_info:
        example.prepare_live_google_credentials()

    message = str(exc_info.value)
    assert "API key" in message
    assert example.GEMINI_API_KEY_ENV in message
    assert example.GOOGLE_API_KEY_ENV in message
    assert "Vertex AI" in message
    assert example.VERTEXAI_ENV in message
    assert example.CLOUD_PROJECT_ENV in message
    assert example.CLOUD_LOCATION_ENV in message


def test_google_adk_example_local_mode_runs_without_provider_credentials(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pytest.importorskip("google.adk")
    _install_no_hosted_provider_guard(monkeypatch)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    example.main(
        [
            "--mode",
            "local",
            "--query",
            "cats",
            "--session-id",
            f"example-local-{uuid4().hex}",
        ]
    )

    output = capsys.readouterr().out
    assert "Checkpoint strategy: runner_call" in output
    assert "Status: completed" in output
    assert "Final output preview: final local answer: local-cat-fact for cats" in output
    assert "Handoff count: 0" in output


def test_google_adk_workflow_runs_persisted_local_path_without_provider_credentials(
    monkeypatch: pytest.MonkeyPatch,
    primed_zenml: None,
) -> None:
    _ = primed_zenml
    pytest.importorskip("google.adk")
    _install_no_hosted_provider_guard(monkeypatch)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    result = workflow_example.run_workflow(
        query="cats",
        approval_decision=True,
        session_id=f"workflow-local-{uuid4().hex}",
    )

    assert result["mode"] == "local"
    assert result["status"] == "completed"
    assert result["checkpoint_strategy"] == "calls"
    assert result["human_decision_happened"] is True
    assert result["approval_decision"] is True
    assert result["approval_source"] == "injected_decision"
    assert result["final_answer"] == (
        "final workflow answer: workflow-tool-calculation=3049 for cats"
    )
    assert result["physical_tool_executions"] == [
        "multiply_numbers:97x31=3007",
        "add_offset:3007+42=3049",
    ]
    assert result["status_history"] == ["requires_action", "completed"]
    assert "model_call" in result["first_turn"]["tracked_event_kinds"]
    assert "model_call" in result["final_turn"]["tracked_event_kinds"]
    assert "tool_call" in result["final_turn"]["tracked_event_kinds"]

    second_result = workflow_example.run_workflow(
        query="dogs",
        approval_decision=True,
        session_id=f"workflow-local-{uuid4().hex}",
    )

    assert second_result["final_answer"] == (
        "final workflow answer: workflow-tool-calculation=3049 for dogs"
    )
    assert second_result["physical_tool_executions"] == [
        "multiply_numbers:97x31=3007",
        "add_offset:3007+42=3049",
    ]


def test_google_adk_workflow_denial_path_does_not_run_tool(
    monkeypatch: pytest.MonkeyPatch,
    primed_zenml: None,
) -> None:
    _ = primed_zenml
    pytest.importorskip("google.adk")
    _install_no_hosted_provider_guard(monkeypatch)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    result = workflow_example.run_workflow(
        query="cats",
        approval_decision=False,
        session_id=f"workflow-deny-{uuid4().hex}",
    )

    assert result["human_decision_happened"] is True
    assert result["approval_decision"] is False
    assert result["approval_source"] == "injected_decision"
    assert result["physical_tool_executions"] == []
