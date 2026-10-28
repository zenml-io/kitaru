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
"""Deterministic text rendering for CLI output tests."""

import io

from rich.text import Text

from kitaru.cli.output import (
    CommandResult,
    OutputContext,
    emit_result,
    reset_output_context,
    set_output_context,
)


def render_text(
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
