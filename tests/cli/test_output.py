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

import pytest

from kitaru.cli.output import (
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
            machine=True,
            non_interactive=True,
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
            machine=True,
            non_interactive=True,
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


def test_redaction_masks_secret_fields_and_recognizable_values() -> None:
    """Nested credentials are masked while safe credential metadata survives."""
    redacted = redact_data(
        {
            "api_key": "KITKEY_topsecret",
            "credential_stored": True,
            "credential_status": {"kind": "api_key", "renewable": True},
            "message": "Authorization: Bearer token-value",
        }
    )
    assert redacted["api_key"] == "***"
    assert redacted["credential_stored"] is True
    assert redacted["credential_status"] == {
        "kind": "api_key",
        "renewable": True,
    }
    assert "token-value" not in redacted["message"]
