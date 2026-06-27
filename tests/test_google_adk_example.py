"""Deterministic coverage for the Google ADK adapter example."""

import socket
from typing import Any
from uuid import uuid4

import pytest
from examples.integrations.google_adk_agent import google_adk_adapter as example


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
