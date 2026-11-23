#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Readable worker coverage updates without raw task payloads."""

import io

from kitaru.cli.output import (
    OutputContext,
    emit_event,
    reset_output_context,
    set_output_context,
)


def test_text_diagnostics_show_warning_and_recovery() -> None:
    stdout = io.StringIO()
    token = set_output_context(
        OutputContext(
            command="job.watch",
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
            "diagnostics",
            {
                "task_diagnostics": {"groups": [{"labels": {"internal": "value"}}]},
                "warnings": ["One analyzer task needs --claim analyzer."],
            },
        )
        emit_event("diagnostics", {"warnings": []})
    finally:
        reset_output_context(token)

    assert stdout.getvalue().splitlines() == [
        "Warning: One analyzer task needs --claim analyzer.",
        "Worker coverage restored.",
    ]
