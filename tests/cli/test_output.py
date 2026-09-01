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
"""Output modes, stable envelopes, and secret masking."""

import io
import json
from typing import Any, Literal

import pytest
from rich.console import Console
from rich.text import Text

from kitaru.cli.output import (
    ERROR_EXIT_CODES,
    CLIError,
    CommandResult,
    OutputContext,
    emit_error,
    emit_event,
    emit_result,
    redact_data,
    reset_output_context,
    resolve_output_mode,
    set_output_context,
)


def _render_text(
    command: str,
    result: CommandResult,
    *,
    rich: bool,
    columns: int = 120,
    strip_ansi: bool = True,
) -> tuple[str, str]:
    """Render one text result with a deterministic terminal width."""
    stdout = io.StringIO()
    stderr = io.StringIO()
    token = set_output_context(
        OutputContext(
            command=command,
            mode="text",
            debug=False,
            traceback=False,
            stdout=stdout,
            stderr=stderr,
            rich=rich,
            terminal_width=columns,
        )
    )
    try:
        emit_result(result)
    finally:
        reset_output_context(token)
    if strip_ansi:
        return Text.from_ansi(stdout.getvalue()).plain, Text.from_ansi(
            stderr.getvalue()
        ).plain
    return stdout.getvalue(), stderr.getvalue()


@pytest.mark.parametrize("rich", [False, True])
def test_text_redacts_fields_and_metadata_before_rendering(rich: bool) -> None:
    """Selecting a display field must not discard its secret-key context."""
    result = CommandResult(
        item={"password": "unmarked-secret", "tokens": {"input_tokens": 4}},
        warnings=["Bearer warning-secret"],
        links={"inspect": "https://example.test/KITKEY_link-secret"},
        next_actions=["Use ZENPROKEY_action-secret"],
    )
    stdout, stderr = _render_text("example.get", result, rich=rich)
    assert "unmarked-secret" not in stdout
    assert "link-secret" not in stdout
    assert "action-secret" not in stdout
    assert "warning-secret" not in stderr
    assert "input_tokens" in stdout
    assert result.item["password"] == "unmarked-secret"
    assert result.warnings == ["Bearer warning-secret"]


@pytest.mark.parametrize("mode", ["json", "jsonl", "text"])
def test_deep_result_and_error_remain_serializable(
    mode: Literal["json", "jsonl", "text"],
) -> None:
    """Deep values must not break success or error emission."""
    value: Any = {"password": "deep-secret"}
    for _ in range(3000):
        value = {"nested": value}
    stdout, stderr = io.StringIO(), io.StringIO()
    context = OutputContext(
        command="example.get",
        mode=mode,
        debug=False,
        traceback=False,
        stdout=stdout,
        stderr=stderr,
        rich=False,
    )
    token = set_output_context(context)
    try:
        assert emit_result(CommandResult(item=value)) == 0
        assert (
            emit_error(CLIError("invalid_arguments", "bad input", details=value)) == 2
        )
    finally:
        reset_output_context(token)
    assert "deep-secret" not in stdout.getvalue() + stderr.getvalue()
    if mode != "text":
        assert json.loads(stdout.getvalue())["ok"] is True
        assert json.loads(stderr.getvalue())["ok"] is False


def test_auto_mode_and_finite_jsonl_contract() -> None:
    """Finite commands use TTY text, piped JSON, and reject JSONL."""
    assert resolve_output_mode("auto", is_tty=True) == "text"
    assert resolve_output_mode("auto", is_tty=False) == "json"
    with pytest.raises(CLIError, match="streaming"):
        resolve_output_mode("jsonl", is_tty=False)
    assert resolve_output_mode("auto", is_tty=False, streaming=True) == "jsonl"
    assert resolve_output_mode("json", is_tty=False, streaming=True) == "json"


def test_json_success_and_error_use_separate_streams() -> None:
    """Success data stays on stdout and errors stay on stderr."""
    stdout = io.StringIO()
    stderr = io.StringIO()
    token = set_output_context(
        OutputContext(
            command="context.get",
            mode="json",
            debug=False,
            traceback=False,
            stdout=stdout,
            stderr=stderr,
            rich=False,
        )
    )
    try:
        assert emit_result(CommandResult(item={"name": "Prod"})) == 0
        success = json.loads(stdout.getvalue())
        assert success == {
            "schema_version": "1",
            "command": "context.get",
            "ok": True,
            "warnings": [],
            "links": {},
            "next_actions": [],
            "item": {"name": "Prod"},
        }
        assert stderr.getvalue() == ""

        stdout.seek(0)
        stdout.truncate(0)
        assert emit_error(CLIError("not_found", "missing")) == 4
        assert stdout.getvalue() == ""
        failure = json.loads(stderr.getvalue())
        assert failure["error"] == {
            "kind": "not_found",
            "message": "missing",
            "retryable": False,
        }
    finally:
        reset_output_context(token)


def test_jsonl_lifecycle_events_are_append_only() -> None:
    """Streaming commands emit typed events followed by one terminal result."""
    stdout = io.StringIO()
    token = set_output_context(
        OutputContext(
            command="worker.start",
            mode="jsonl",
            debug=False,
            traceback=False,
            stdout=stdout,
            stderr=io.StringIO(),
            rich=False,
        )
    )
    try:
        emit_event("starting", {"name": "local"})
        assert (
            emit_result(
                CommandResult(
                    item={"name": "local", "status": "stopped"}, event="stopped"
                )
            )
            == 0
        )
    finally:
        reset_output_context(token)

    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["event"] for event in events] == ["starting", "stopped"]
    assert all(event["command"] == "worker.start" for event in events)


def test_text_lifecycle_event_redacts_structured_data() -> None:
    """Text events retain safe fields without exposing credential context."""
    stdout = io.StringIO()
    token = set_output_context(
        OutputContext(
            command="worker.start",
            mode="text",
            debug=False,
            traceback=False,
            stdout=stdout,
            stderr=io.StringIO(),
            rich=False,
        )
    )
    try:
        emit_event(
            "starting",
            {"name": "local", "password": "unmarked-secret", "note": "Bearer marked"},
        )
    finally:
        reset_output_context(token)

    output = stdout.getvalue()
    assert "starting" in output and "local" in output
    assert "unmarked-secret" not in output and "marked" not in output


def test_interrupted_error_has_stable_structured_shape() -> None:
    """Interrupt errors use the shared stable kind and exit code."""
    stderr = io.StringIO()
    token = set_output_context(
        OutputContext(
            command="worker.start",
            mode="jsonl",
            debug=True,
            traceback=True,
            stdout=io.StringIO(),
            stderr=stderr,
            rich=False,
        )
    )
    try:
        assert emit_error(CLIError("interrupted", "Interrupted.")) == 130
    finally:
        reset_output_context(token)

    assert ERROR_EXIT_CODES["interrupted"] == 130
    assert json.loads(stderr.getvalue()) == {
        "schema_version": "1",
        "command": "worker.start",
        "ok": False,
        "error": {
            "kind": "interrupted",
            "message": "Interrupted.",
            "retryable": False,
        },
    }


class _FlushTrackingWriter(io.StringIO):
    """String writer that records explicit flushes."""

    def __init__(self) -> None:
        super().__init__()
        self.flush_count = 0

    def flush(self) -> None:
        """Record and perform one flush."""
        self.flush_count += 1
        super().flush()


def test_structured_results_events_and_errors_flush() -> None:
    """Every JSON and JSONL document is explicitly flushed."""
    stdout = _FlushTrackingWriter()
    stderr = _FlushTrackingWriter()
    token = set_output_context(
        OutputContext(
            command="worker.start",
            mode="jsonl",
            debug=False,
            traceback=False,
            stdout=stdout,
            stderr=stderr,
            rich=False,
        )
    )
    try:
        emit_event("starting")
        emit_result(CommandResult(item={"status": "stopped"}, event="stopped"))
        emit_error(CLIError("interrupted", "Interrupted."))
    finally:
        reset_output_context(token)

    assert stdout.flush_count == 2
    assert stderr.flush_count == 1


def test_redaction_masks_secret_fields_and_recognizable_values() -> None:
    """Nested credentials are masked while safe credential metadata survives."""
    redacted = redact_data(
        {
            "api_key": "KITKEY_topsecret",
            "credential_stored": True,
            "credential_status": {"kind": "api_key", "renewable": True},
            "annotation": {
                "client_secret": "client-value",
                "private_key": "private-value",
                "secret_env": "env-value",
            },
            "message": "Authorization: Bearer token-value",
        }
    )
    assert redacted["api_key"] == "***"
    assert redacted["credential_stored"] is True
    assert redacted["credential_status"] == {
        "kind": "api_key",
        "renewable": True,
    }
    assert redacted["annotation"] == {
        "client_secret": "***",
        "private_key": "***",
        "secret_env": "***",
    }
    assert "token-value" not in redacted["message"]


@pytest.mark.parametrize("rich", [False, True])
def test_annotation_text_escapes_terminal_control_characters(rich: bool) -> None:
    """Stored annotation text cannot emit terminal control sequences."""
    payload = "move\x1b[999Dlink\x1b]8;;https://evil.example\x07text"
    stdout, _ = _render_text(
        "annotation.get",
        CommandResult(item={"id": "annotation-id", "value": payload}),
        rich=rich,
        strip_ansi=False,
    )
    assert "\x1b[999D" not in stdout
    assert "\x1b]8;;https://evil.example" not in stdout
    assert "\\u001b[999D" in stdout
    assert "\\u001b]8;;https://evil.example\\u0007text" in stdout


def test_annotation_json_redacts_nested_secret_fields() -> None:
    """Structured annotation output masks recognized nested credentials."""
    stdout = io.StringIO()
    token = set_output_context(
        OutputContext(
            command="annotation.get",
            mode="json",
            debug=False,
            traceback=False,
            stdout=stdout,
            stderr=io.StringIO(),
            rich=False,
        )
    )
    try:
        emit_result(
            CommandResult(
                item={
                    "value": {
                        "client_secret": "client-value",
                        "private_key": "private-value",
                        "secret_env": "env-value",
                    }
                }
            )
        )
    finally:
        reset_output_context(token)
    assert json.loads(stdout.getvalue())["item"]["value"] == {
        "client_secret": "***",
        "private_key": "***",
        "secret_env": "***",
    }


@pytest.mark.parametrize(
    ("columns", "shown", "hidden"),
    [
        (
            80,
            ("Status", "Name", "ID"),
            ("Origin", "Imported from", "LLM calls", "Tool calls", "Cost"),
        ),
        (
            120,
            ("Status", "Name", "ID", "Origin"),
            ("Imported from", "LLM calls", "Tool calls", "Cost"),
        ),
        (
            200,
            (
                "Status",
                "Name",
                "ID",
                "Origin",
                "Imported from",
                "LLM calls",
                "Tool calls",
                "Cost",
            ),
            (),
        ),
    ],
)
def test_human_session_list_selects_columns_for_terminal_width(
    columns: int, shown: tuple[str, ...], hidden: tuple[str, ...]
) -> None:
    """Human tables remain useful at common widths and never shorten IDs."""
    session_id = "019f0000-1111-7222-8333-444444444444"
    stdout, _ = _render_text(
        "session.list",
        CommandResult(
            items=[
                {
                    "id": session_id,
                    "status": "completed",
                    "name": "Example",
                    "origin": "imported",
                    "imported_from": "langfuse",
                    "llm_call_count": 2,
                    "tool_call_count": 3,
                    "cost": "0.012",
                    "created": "2026-08-03T12:00:00Z",
                    "inputs": {"large": "payload that must not become a column"},
                }
            ],
            page={"limit": 20, "next_cursor": None, "truncated": False},
        ),
        rich=True,
        columns=columns,
    )

    assert session_id in stdout.replace("\n", "")
    for heading in shown:
        assert heading in stdout
    for heading in hidden:
        assert heading not in stdout
    assert "large" not in stdout


def test_human_values_preserve_literal_rich_markup_characters() -> None:
    """Server values display literally instead of being interpreted as Rich markup."""
    stdout, _ = _render_text(
        "session.list",
        CommandResult(
            items=[
                {
                    "id": "019f0000-1111-7222-8333-444444444444",
                    "status": "completed",
                    "name": "[bold red]literal[/bold red]",
                }
            ],
            page={"limit": 20, "next_cursor": None, "truncated": False},
        ),
        rich=True,
        columns=100,
    )

    assert "[bold red]literal[/bold red]" in stdout


def test_human_list_renders_empty_state_pagination_and_next_actions() -> None:
    """Human lists explain empty pages and expose exact continuation data."""
    stdout, _ = _render_text(
        "worker.list",
        CommandResult(
            items=[],
            page={"limit": 20, "next_cursor": "cursor-token", "truncated": True},
            links={"dashboard": "https://example.test/workers"},
            next_actions=["kitaru worker start"],
        ),
        rich=True,
    )

    assert "No workers found." in stdout
    assert "Next cursor: cursor-token" in stdout
    assert "Showing 0 items." in stdout
    assert "dashboard: https://example.test/workers" in stdout
    assert "Next: kitaru worker start" in stdout


def test_human_detail_uses_sections_and_pretty_prints_nested_values() -> None:
    """Human detail views separate summary fields from nested payloads."""
    stdout, _ = _render_text(
        "session.get",
        CommandResult(
            item={
                "id": "019f0000-1111-7222-8333-444444444444",
                "status": "completed",
                "name": "Example",
                "origin": "native",
                "inputs": {"prompt": "Hello"},
                "outputs": {"answer": "Hi"},
                "metadata": {"team": "cli"},
                "llm_call_count": 1,
                "tool_call_count": 0,
            }
        ),
        rich=True,
    )

    assert "Summary" in stdout
    assert "Usage" in stdout
    assert "Payload" in stdout
    assert '"prompt": "Hello"' in stdout
    assert '{"prompt": "Hello"}' not in stdout


def test_machine_text_ignores_human_view_metadata() -> None:
    """Plain machine text retains the existing complete line representation."""
    item = {
        "id": "019f0000-1111-7222-8333-444444444444",
        "status": "completed",
        "inputs": {"prompt": "Hello"},
    }
    stdout, _ = _render_text("session.list", CommandResult(items=[item]), rich=False)

    assert stdout == json.dumps(item, sort_keys=True) + "\n"


def test_machine_text_renders_links_and_next_actions_after_the_item() -> None:
    """Plain text retains the rich footer's stable link and action labels."""
    stdout, _ = _render_text(
        "session.get",
        CommandResult(
            item={"id": "019f0000-1111-7222-8333-444444444444"},
            links={"skills": "https://kitaru.ai/skills"},
            next_actions=["kitaru session list"],
        ),
        rich=False,
    )

    assert stdout.splitlines() == [
        "id: 019f0000-1111-7222-8333-444444444444",
        "skills: https://kitaru.ai/skills",
        "Next: kitaru session list",
    ]


def test_root_rich_text_escapes_disk_derived_skill_names() -> None:
    """Detected skill names cannot inject terminal control sequences."""
    stdout, _ = _render_text(
        "kitaru",
        CommandResult(
            item={
                "skills": {
                    "installed": True,
                    "skills": ["kitaru-safe\x1b[999Dname"],
                }
            }
        ),
        rich=True,
        strip_ansi=False,
    )

    assert "\x1b[999D" not in stdout
    assert "kitaru-safe\\u001b[999Dname" in stdout


def test_human_error_uses_a_clear_headline_and_recovery_hint() -> None:
    """Interactive errors separate the failure kind, cause, and next step."""
    stdout = io.StringIO()
    stderr = io.StringIO()
    token = set_output_context(
        OutputContext(
            command="session.list",
            mode="text",
            debug=False,
            traceback=False,
            stdout=stdout,
            stderr=stderr,
            rich=True,
            terminal_width=100,
        )
    )
    try:
        emit_error(
            CLIError(
                "invalid_arguments",
                "--sort must use a supported field.",
                hint="Run `kitaru session list --help` for valid values.",
            )
        )
    finally:
        reset_output_context(token)

    rendered = Text.from_ansi(stderr.getvalue()).plain
    assert "Invalid arguments" in rendered
    assert "--sort must use a supported field." in rendered
    assert "Run `kitaru session list --help` for valid values." in rendered
    assert stdout.getvalue() == ""


def test_human_doctor_renders_checks_as_an_operational_table() -> None:
    """Doctor output is readable without decoding a nested JSON array."""
    stdout, _ = _render_text(
        "doctor",
        CommandResult(
            item={
                "healthy": False,
                "checks": [
                    {
                        "name": "server_info",
                        "status": "fail",
                        "required": True,
                        "detail": "Connection refused",
                    },
                    {
                        "name": "uv",
                        "status": "pass",
                        "required": False,
                        "detail": "/opt/homebrew/bin/uv",
                    },
                    {
                        "name": "compatibility",
                        "status": "warn",
                        "required": False,
                        "detail": "Version mismatch",
                    },
                    {
                        "name": "authentication",
                        "status": "skip",
                        "required": True,
                        "detail": "Server unavailable",
                    },
                ],
            },
            exit_code=6,
        ),
        rich=True,
        strip_ansi=False,
    )

    rendered = Text.from_ansi(stdout)
    assert "needs attention" in rendered.plain
    assert "Check" in rendered.plain
    assert "Status" in rendered.plain
    assert "server_info" in rendered.plain
    assert "Connection refused" in rendered.plain
    assert '"checks"' not in rendered.plain

    console = Console()
    expected_colors = {"pass": 2, "fail": 1, "warn": 3}
    for status, color_number in expected_colors.items():
        style = rendered.get_style_at_offset(console, rendered.plain.index(status))
        assert style.color is not None
        assert style.color.number == color_number
    skip_style = rendered.get_style_at_offset(console, rendered.plain.index("skip"))
    assert skip_style.dim is True


def test_human_registration_receipt_keeps_parent_and_version_identity() -> None:
    """Successful multi-phase registrations never render an empty human result."""
    stdout, _ = _render_text(
        "agent.register",
        CommandResult(
            item={
                "agent": {
                    "id": "019f0000-1111-7222-8333-444444444444",
                    "name": "assistant",
                },
                "version": {
                    "id": "019f0000-1111-7222-8333-555555555555",
                    "version": 3,
                },
                "phases": {
                    "parent": {"completed": True},
                    "version": {"completed": True},
                },
            }
        ),
        rich=True,
    )

    assert "Registered" in stdout
    assert "assistant" in stdout
    assert "019f0000-1111-7222-8333-444444444444" in stdout
    assert "019f0000-1111-7222-8333-555555555555" in stdout
    assert "Phases" in stdout
