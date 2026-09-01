#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""CLI bootstrap, invocation, and lazy-extra behavior."""

import builtins
import io
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

import kitaru.cli
from kitaru.cli import app as app_module
from kitaru.cli.output import (
    CLIError,
    CommandResult,
    OutputMode,
    emit_event,
    get_output_context,
)
from kitaru.cli.skill_discovery import INSTALL_COMMAND, SKILLS_URL
from kitaru.client.exceptions import APIError, InvalidServerResponseError


def test_main_emits_deep_error_as_structured_stderr(monkeypatch, capsys) -> None:
    """Deep error details do not escape the CLI's error boundary."""
    details: dict[str, Any] = {}
    cursor = details
    for _ in range(3000):
        cursor["nested"] = {}
        cursor = cursor["nested"]
    cursor["password"] = "unmarked-secret"

    def fail_version() -> str:
        raise CLIError("invalid_arguments", "Invalid input", details=details)

    monkeypatch.setattr(app_module.diagnostics, "package_version", fail_version)
    assert app_module.main(["version", "--output", "json"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"]["kind"] == "invalid_arguments"
    assert "Traceback" not in captured.err
    assert "unmarked-secret" not in captured.err


def test_bare_command_group_prints_help_without_bootstrap(monkeypatch, capsys) -> None:
    """A bare command group prints its help without local setup."""

    def fail_read_config():
        raise AssertionError("help read local config")

    monkeypatch.setattr(app_module, "read_config", fail_read_config)

    assert app_module.main(["worker"]) == 0
    captured = capsys.readouterr()
    assert "Usage: kitaru worker COMMAND [OPTIONS]" in captured.out
    assert INSTALL_COMMAND not in captured.out
    assert SKILLS_URL not in captured.out
    assert captured.err == ""


@pytest.mark.parametrize(
    "argv",
    [
        ["--help"],
        ["-h"],
        ["--machine", "--help"],
        ["--help", "--output", "text"],
        ["--output=text", "-h"],
    ],
)
def test_root_help_adds_skill_onboarding(argv, monkeypatch, capsys) -> None:
    """Explicit root help includes the same skill guidance as bare Kitaru."""
    monkeypatch.setattr(
        app_module,
        "get_kitaru_skill_status",
        lambda: {
            "installed": False,
            "skill_count": 0,
            "skills": [],
            "installations": [],
            "locations_checked": [],
        },
    )

    assert app_module.main(argv) == 0

    captured = capsys.readouterr()
    assert "Usage: kitaru COMMAND [OPTIONS]" in captured.out
    assert f"skills: {SKILLS_URL}" in captured.out
    assert f"Next: {INSTALL_COMMAND}" in captured.out
    assert captured.err == ""


def test_root_help_honors_json_output(monkeypatch, capsys) -> None:
    """Explicit JSON root help emits one structured onboarding document."""
    monkeypatch.setattr(
        app_module,
        "get_kitaru_skill_status",
        lambda: {
            "installed": False,
            "skill_count": 0,
            "skills": [],
            "installations": [],
            "locations_checked": [],
        },
    )

    assert app_module.main(["--help", "--output", "json"]) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["command"] == "kitaru"
    assert payload["item"]["skills"]["installed"] is False
    assert payload["next_actions"] == [INSTALL_COMMAND]
    assert captured.err == ""


@pytest.mark.parametrize("output", ["jsonl", "yaml"])
def test_root_help_validates_explicit_output(output, capsys) -> None:
    """Unsupported root-help output modes remain argument errors."""
    assert app_module.main(["--help", "--output", output]) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"]["kind"] == "invalid_arguments"


def test_bare_root_emits_structured_skill_onboarding_for_machines(
    monkeypatch, capsys
) -> None:
    """A non-terminal root invocation returns one stable machine document."""
    monkeypatch.setattr(
        app_module,
        "get_kitaru_skill_status",
        lambda: {
            "installed": False,
            "skill_count": 0,
            "skills": [],
            "installations": [],
            "locations_checked": ["/tmp/project/.agents/skills"],
        },
    )

    assert app_module.main([]) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload == {
        "schema_version": "1",
        "command": "kitaru",
        "ok": True,
        "warnings": [],
        "links": {"skills": SKILLS_URL},
        "next_actions": [INSTALL_COMMAND],
        "item": {
            "skills": {
                "installed": False,
                "skill_count": 0,
                "skills": [],
                "installations": [],
                "locations_checked": ["/tmp/project/.agents/skills"],
            }
        },
    }
    assert captured.err == ""


def test_bare_root_omits_install_action_when_skills_are_detected(
    monkeypatch, capsys
) -> None:
    """Detected skills remain visible without an inapplicable install action."""
    monkeypatch.setattr(
        app_module,
        "get_kitaru_skill_status",
        lambda: {
            "installed": True,
            "skill_count": 1,
            "skills": ["kitaru-investigation"],
            "installations": [],
            "locations_checked": [],
        },
    )

    assert app_module.main([]) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["item"]["skills"]["installed"] is True
    assert payload["links"] == {"skills": SKILLS_URL}
    assert payload["next_actions"] == []
    assert captured.err == ""


def test_bare_root_text_on_a_non_terminal_renders_plain_footer(
    monkeypatch, capsys
) -> None:
    """Explicit text on a pipe keeps root onboarding readable for scripts."""
    monkeypatch.setattr(
        app_module,
        "get_kitaru_skill_status",
        lambda: {"installed": False, "skills": []},
    )

    assert app_module.main(["--output", "text"]) == 0

    captured = capsys.readouterr()
    assert f"skills: {SKILLS_URL}" in captured.out
    assert f"Next: {INSTALL_COMMAND}" in captured.out
    assert captured.err == ""


class _TTYStringIO(io.StringIO):
    """In-memory stream that presents itself as an interactive terminal."""

    def isatty(self) -> bool:
        """Report terminal output so auto mode selects human text."""
        return True


def test_bare_root_reports_detected_skills_without_install_action(
    monkeypatch, capsys
) -> None:
    """Interactive output does not suggest reinstalling detected skills."""
    stdout = _TTYStringIO()
    monkeypatch.setattr(app_module.sys, "stdout", stdout)
    monkeypatch.setattr(
        app_module,
        "get_kitaru_skill_status",
        lambda: {
            "installed": True,
            "skill_count": 2,
            "skills": ["kitaru-investigation", "kitaru-replay-lab"],
            "installations": [],
            "locations_checked": [],
        },
    )

    assert app_module.main([]) == 0

    rendered = stdout.getvalue()
    assert "Usage: kitaru COMMAND [OPTIONS]" in rendered
    assert "Kitaru agent skills detected" in rendered
    assert "kitaru-investigation" in rendered
    assert INSTALL_COMMAND not in rendered
    assert SKILLS_URL in rendered
    assert capsys.readouterr().err == ""


def test_bare_root_machine_mode_on_a_terminal_renders_plain_footer(
    monkeypatch, capsys
) -> None:
    """Machine mode uses stable plain root footer labels even on a terminal."""
    stdout = _TTYStringIO()
    monkeypatch.setattr(app_module.sys, "stdout", stdout)
    monkeypatch.setattr(
        app_module,
        "get_kitaru_skill_status",
        lambda: {"installed": False, "skills": []},
    )

    assert app_module.main(["--machine"]) == 0

    rendered = stdout.getvalue()
    assert f"skills: {SKILLS_URL}" in rendered
    assert f"Next: {INSTALL_COMMAND}" in rendered
    assert capsys.readouterr().err == ""


def test_help_version_schema_and_scaffold_skip_bootstrap(
    monkeypatch, capsys, tmp_path: Path
) -> None:
    """Offline bootstrap commands do not construct the local config store."""

    def fail_read_config():
        raise AssertionError("offline command read local config")

    monkeypatch.setattr(app_module, "read_config", fail_read_config)
    monkeypatch.setattr(
        app_module,
        "get_kitaru_skill_status",
        lambda: {
            "installed": True,
            "skill_count": 1,
            "skills": ["kitaru-investigation"],
            "installations": [],
            "locations_checked": [],
        },
    )

    assert app_module.main(["--help"]) == 0
    help_output = capsys.readouterr()
    assert "Connect to, inspect, and configure Kitaru" in help_output.out
    assert INSTALL_COMMAND not in help_output.out
    assert SKILLS_URL in help_output.out

    assert app_module.main(["version"]) == 0
    version_output = capsys.readouterr()
    payload = json.loads(version_output.out)
    assert payload["command"] == "version"
    assert payload["item"]["version"]
    assert version_output.err == ""

    assert app_module.main(["schema", "config"]) == 0
    schema_output = capsys.readouterr()
    schema_payload = json.loads(schema_output.out)
    assert schema_payload["command"] == "schema"
    assert {item["command"] for item in schema_payload["items"]} == {
        "config.get",
        "config.list",
        "config.path",
        "config.set",
    }

    scaffold_path = tmp_path / "importer.py"
    assert (
        app_module.main(
            [
                "importer",
                "scaffold",
                "demo",
                "--path",
                str(scaffold_path),
            ]
        )
        == 0
    )
    scaffold_output = capsys.readouterr()
    assert json.loads(scaffold_output.out)["command"] == "importer.scaffold"
    assert scaffold_path.is_file()


def test_non_finite_request_timeout_is_rejected(capsys) -> None:
    """Global request timeouts must remain finite positive bounds."""
    for value in ("nan", "inf"):
        assert app_module.main(["version", "--request-timeout", value]) == 2
        captured = capsys.readouterr()
        assert captured.out == ""
        payload = json.loads(captured.err)
        assert payload["error"]["kind"] == "invalid_arguments"


def test_parse_error_is_structured_on_stderr(capsys) -> None:
    """Argument failures never corrupt structured stdout."""
    assert app_module.main(["not-a-command"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["ok"] is False
    assert payload["error"]["kind"] == "invalid_arguments"


def test_bare_root_rejects_jsonl_with_a_structured_error(capsys) -> None:
    """Bare root JSONL reports an invalid-arguments error without stdout data."""
    assert app_module.main(["--output", "jsonl"]) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["ok"] is False
    assert payload["error"]["kind"] == "invalid_arguments"


def test_nested_asset_parse_error_reports_full_command(capsys) -> None:
    """Early argument failures retain the complete nested command path."""
    assert app_module.main(["importer", "version", "get"]) == 2
    captured = capsys.readouterr()
    assert json.loads(captured.err)["command"] == "importer.version.get"


def test_output_context_resets_between_invocations(capsys) -> None:
    """An explicit text invocation does not leak into the next call."""
    assert app_module.main(["version", "--output", "text"]) == 0
    first = capsys.readouterr()
    assert first.out.startswith("version: ")

    assert app_module.main(["version"]) == 0
    second = capsys.readouterr()
    assert json.loads(second.out)["command"] == "version"


@pytest.mark.parametrize("output", ["text", "json", "jsonl"])
async def test_active_interrupt_uses_resolved_output_context(
    output: OutputMode, monkeypatch, capsys
) -> None:
    """Interrupts after bootstrap use one stable error without debug output."""

    def interrupt() -> CommandResult:
        raise KeyboardInterrupt

    spec = replace(
        app_module._FUNCTION_SPECS[app_module.version],
        streams=output == "jsonl",
    )
    monkeypatch.setitem(app_module._FUNCTION_SPECS, interrupt, spec)
    monkeypatch.setattr(
        type(app_module.app),
        "parse_args",
        lambda self, tokens: (
            interrupt,
            SimpleNamespace(args=(), kwargs={}),
            None,
        ),
    )

    assert (
        await app_module._launch("version", output=output, debug=True, traceback=True)
        == 130
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    if output == "text":
        assert captured.err == "Error: Interrupted.\n"
    else:
        assert json.loads(captured.err) == {
            "schema_version": "1",
            "command": "version",
            "ok": False,
            "error": {
                "kind": "interrupted",
                "message": "Interrupted.",
                "retryable": False,
            },
        }
    with pytest.raises(RuntimeError, match="No CLI output context"):
        get_output_context()
    assert app_module._INVOCATION.get() is None


@pytest.mark.parametrize("output", ["text", "json", "jsonl"])
def test_early_interrupt_uses_requested_output(
    output: str, monkeypatch, capsys
) -> None:
    """Interrupts before bootstrap retain the requested serialization mode."""

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(app_module, "app", SimpleNamespace(meta=interrupt))

    assert (
        app_module.main(["version", "--output", output, "--debug", "--traceback"])
        == 130
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    if output == "text":
        assert captured.err == "Error: Interrupted.\n"
    else:
        payload = json.loads(captured.err)
        assert payload["command"] == "version"
        assert payload["error"] == {
            "kind": "interrupted",
            "message": "Interrupted.",
            "retryable": False,
        }
        assert "debug" not in payload


class _BrokenFlushWriter(io.StringIO):
    """Structured writer whose consumer closes at flush time."""

    def flush(self) -> None:
        """Simulate a closed downstream consumer."""
        raise BrokenPipeError


def test_result_and_error_broken_pipes_exit_zero(monkeypatch, capsys) -> None:
    """Closed structured success and error streams do not emit a second error."""
    stdout = _BrokenFlushWriter()
    with monkeypatch.context() as scoped:
        scoped.setattr(app_module.sys, "stdout", stdout)
        assert app_module.main(["version", "--output", "json"]) == 0
    assert capsys.readouterr().err == ""

    stderr = _BrokenFlushWriter()
    with monkeypatch.context() as scoped:
        scoped.setattr(app_module.sys, "stderr", stderr)
        assert app_module.main(["not-a-command", "--output", "json"]) == 0
    assert capsys.readouterr().out == ""


async def test_event_broken_pipe_exits_zero(monkeypatch, capsys) -> None:
    """A closed JSONL event stream is handled by the active invocation."""

    def stream() -> CommandResult:
        emit_event("starting")
        return CommandResult(item={"status": "stopped"})

    spec = replace(app_module._FUNCTION_SPECS[app_module.version], streams=True)
    monkeypatch.setitem(app_module._FUNCTION_SPECS, stream, spec)
    monkeypatch.setattr(
        type(app_module.app),
        "parse_args",
        lambda self, tokens: (
            stream,
            SimpleNamespace(args=(), kwargs={}),
            None,
        ),
    )
    stdout = _BrokenFlushWriter()
    with monkeypatch.context() as scoped:
        scoped.setattr(app_module.sys, "stdout", stdout)
        assert await app_module._launch("version", output="jsonl") == 0
    assert capsys.readouterr().err == ""


def test_commands_advertise_interrupted_error() -> None:
    """Executable leaves include the shared invocation-level interrupt outcome."""
    assert "interrupted" in app_module._FUNCTION_SPECS[app_module.version].error_kinds


def test_http_413_maps_to_invalid_arguments_with_server_detail() -> None:
    """The shared boundary preserves the authoritative server blob rejection."""
    error = app_module._convert_error(APIError(413, "payload exceeds configured cap"))

    assert error.kind == "invalid_arguments"
    assert error.message == "payload exceeds configured cap"
    assert error.details == {"status_code": 413}


def test_html_response_maps_to_invalid_configuration() -> None:
    """The shared boundary names the endpoint that answered with an HTML page."""
    error = app_module._convert_error(
        InvalidServerResponseError(
            "The server answered GET /api/v1/info with an HTML page "
            "instead of an API response"
        )
    )

    assert error.kind == "invalid_configuration"
    assert "GET /api/v1/info" in error.message
    assert error.hint is not None


def test_transport_error_falls_back_to_secret_safe_request_origin() -> None:
    """A transport error without invocation state omits paths and query values."""
    request = httpx.Request(
        "GET",
        "https://user:password@api.example.com/api/v1/workers?credential=secret#token",
    )

    error = app_module._convert_error(
        httpx.ConnectError("connection refused", request=request)
    )

    assert error.message == (
        "The server at https://api.example.com is unavailable: connection refused"
    )
    assert error.details == {"server_url": "https://api.example.com"}
    assert "credential" not in error.message
    assert "secret" not in error.message
    assert "user" not in error.message
    assert "password" not in error.message
    assert "token" not in error.message


def test_transport_error_reports_different_failed_request_origin() -> None:
    """A request to another service is not attributed to the selected server."""
    request = httpx.Request("GET", "https://control.example.com/v1/token")

    error = app_module._convert_error(
        httpx.ConnectError("connection refused", request=request),
        server_url="https://api.example.com/kitaru",
    )

    assert error.message == (
        "The server at https://control.example.com is unavailable: connection refused"
    )
    assert error.details == {"server_url": "https://control.example.com"}


def test_transport_error_without_request_uses_selected_server() -> None:
    """Resolved invocation state identifies errors without request metadata."""
    error = app_module._convert_error(
        httpx.ConnectError("connection refused"),
        server_url="https://api.example.com/kitaru",
    )

    assert error.message == (
        "The server at https://api.example.com/kitaru is unavailable: "
        "connection refused"
    )
    assert error.details == {"server_url": "https://api.example.com/kitaru"}


def test_login_network_error_preserves_selected_server_path(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A login transport failure identifies the full selected server URL."""
    server = "https://api.example.com/kitaru"

    async def fail_login(**options: Any) -> CommandResult:
        request = httpx.Request("GET", f"{options['server']}/api/v1/info")
        raise httpx.ConnectError("connection refused", request=request)

    monkeypatch.setattr(app_module.auth_commands, "login", fail_login)

    assert app_module.main(["login", server, "--output", "json"]) == 6

    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert payload["error"]["message"] == (
        f"The server at {server} is unavailable: connection refused"
    )
    assert payload["error"]["details"] == {"server_url": server}


def test_login_timeout_identifies_selected_server(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A login timeout identifies the full selected server URL."""
    server = "https://api.example.com/kitaru"

    async def fail_login(**options: Any) -> CommandResult:
        request = httpx.Request("GET", f"{options['server']}/api/v1/info")
        raise httpx.ReadTimeout("read timed out", request=request)

    monkeypatch.setattr(app_module.auth_commands, "login", fail_login)

    assert app_module.main(["login", server, "--output", "json"]) == 6

    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert payload["error"]["message"] == f"The request to {server} timed out."
    assert payload["error"]["details"] == {"server_url": server}


def test_lazy_entry_point_reports_missing_cli_extra(monkeypatch, capsys) -> None:
    """The package entry point turns an optional import failure into a hint."""
    real_import = builtins.__import__

    def missing_cli_extra(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "kitaru.cli.app":
            raise ModuleNotFoundError("No module named 'cyclopts'", name="cyclopts")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", missing_cli_extra)
    assert kitaru.cli.main(["--help"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "kitaru[cli]" in captured.err
