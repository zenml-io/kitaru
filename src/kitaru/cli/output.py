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
"""Shared serialization, rendering, and error contracts for the CLI."""

import json
import re
import traceback as traceback_module
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, TextIO
from uuid import UUID

from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

OutputMode = Literal["auto", "text", "json", "jsonl"]

ERROR_EXIT_CODES: dict[str, int] = {
    "internal_error": 1,
    "interrupted": 130,
    "invalid_arguments": 2,
    "invalid_configuration": 2,
    "authentication_failed": 3,
    "not_found": 4,
    "conflict": 5,
    "interaction_required": 5,
    "network_error": 6,
    "timeout": 7,
    "remote_failed": 8,
    "partial_failure": 8,
    "remote_canceled": 9,
}

_SECRET_KEY = re.compile(
    r"^(?:authorization|credential|password|api[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|device[_-]?code|secret)$",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(r"(?i)(bearer\s+|KITKEY_|ZENPROKEY_)[^\s,;\]\}\"']+")


@dataclass(slots=True)
class CLIError(Exception):
    """Expected CLI failure with a stable machine-readable kind."""

    kind: str
    message: str
    retryable: bool = False
    details: Any = None
    hint: str | None = None
    exit_code: int | None = None

    def __post_init__(self) -> None:
        """Initialize the base exception message."""
        Exception.__init__(self, self.message)
        if self.exit_code is None:
            self.exit_code = ERROR_EXIT_CODES.get(self.kind, 1)


@dataclass(slots=True)
class CommandResult:
    """Data returned by a CLI command."""

    item: Any = None
    items: list[Any] | None = None
    page: dict[str, Any] | None = None
    warnings: list[str] = field(default_factory=list)
    links: dict[str, str] = field(default_factory=dict)
    next_actions: list[str] = field(default_factory=list)
    event: str | None = None
    exit_code: int = 0


@dataclass(slots=True)
class OutputContext:
    """Resolved output behavior for one invocation."""

    command: str
    mode: Literal["text", "json", "jsonl"]
    machine: bool
    non_interactive: bool
    debug: bool
    traceback: bool
    stdout: TextIO
    stderr: TextIO
    rich: bool


_OUTPUT_CONTEXT: ContextVar[OutputContext | None] = ContextVar(
    "kitaru_cli_output", default=None
)


def set_output_context(context: OutputContext):
    """Set the rendering context for this invocation.

    Args:
        context: Resolved rendering settings.

    Returns:
        Context variable token used to restore the previous value.
    """
    return _OUTPUT_CONTEXT.set(context)


def reset_output_context(token: Any) -> None:
    """Restore the rendering context after an invocation.

    Args:
        token: Token returned by :func:`set_output_context`.
    """
    _OUTPUT_CONTEXT.reset(token)


def get_output_context() -> OutputContext:
    """Return the current invocation's output context.

    Raises:
        RuntimeError: No CLI invocation is active.

    Returns:
        Resolved output context.
    """
    context = _OUTPUT_CONTEXT.get()
    if context is None:
        raise RuntimeError("No CLI output context is active")
    return context


def resolve_output_mode(
    requested: OutputMode, *, is_tty: bool, streaming: bool = False
) -> Literal["text", "json", "jsonl"]:
    """Resolve an output flag for a finite or streaming command.

    Args:
        requested: Requested serialization mode.
        is_tty: Whether standard output is a terminal.
        streaming: Whether the command emits append-only events.

    Raises:
        CLIError: JSONL was requested for a finite command.

    Returns:
        Resolved text, JSON, or JSONL mode.
    """
    if requested == "jsonl" and not streaming:
        raise CLIError(
            "invalid_arguments",
            "JSONL output is only available for streaming commands.",
        )
    if requested == "auto":
        if is_tty:
            return "text"
        return "jsonl" if streaming else "json"
    return requested


def emit_result(result: CommandResult) -> int:
    """Write a successful command result.

    Args:
        result: Result to serialize.

    Returns:
        Exit code carried by the result.
    """
    context = get_output_context()
    payload: dict[str, Any] = {
        "schema_version": "1",
        "command": context.command,
        "ok": True,
        "warnings": result.warnings,
        "links": result.links,
        "next_actions": result.next_actions,
    }
    if result.items is not None:
        payload["items"] = result.items
        payload["count"] = len(result.items)
        payload["page"] = result.page or {
            "limit": len(result.items),
            "next_cursor": None,
            "truncated": False,
        }
    else:
        payload["item"] = result.item
    if context.mode == "json":
        _write_json(context.stdout, payload)
    elif context.mode == "jsonl":
        payload["event"] = result.event or "result"
        _write_json(context.stdout, payload)
    else:
        _emit_text(context, result)
    return result.exit_code


def emit_event(event: str, item: Any = None) -> None:
    """Write one append-only lifecycle event for a streaming command.

    Explicit JSON suppresses intermediate events so it can return one final
    document. Text and JSONL preserve lifecycle order on stdout.

    Args:
        event: Stable event name.
        item: Event data.
    """
    context = get_output_context()
    if context.mode == "json":
        return
    if context.mode == "jsonl":
        _write_json(
            context.stdout,
            {
                "schema_version": "1",
                "command": context.command,
                "ok": True,
                "event": event,
                "item": item,
            },
        )
        return
    suffix = "" if item is None else f": {_display_value(item)}"
    print(f"{event}{suffix}", file=context.stdout, flush=True)


def emit_error(
    error: CLIError,
    *,
    exception: BaseException | None = None,
    traceback: TracebackType | None = None,
) -> int:
    """Write a failure without corrupting structured stdout.

    Args:
        error: Stable CLI failure.
        exception: Original exception for opted-in diagnostics.
        traceback: Original traceback.

    Returns:
        Stable exit code.
    """
    context = get_output_context()
    debug = None
    if (context.debug or context.traceback) and exception is not None:
        debug = "".join(
            traceback_module.format_exception(type(exception), exception, traceback)
        )
        debug = redact(debug)
    if context.mode in ("json", "jsonl"):
        body: dict[str, Any] = {
            "schema_version": "1",
            "command": context.command,
            "ok": False,
            "error": {
                "kind": error.kind,
                "message": redact(error.message),
                "retryable": error.retryable,
            },
        }
        if error.details is not None:
            body["error"]["details"] = redact_data(error.details)
        if error.hint:
            body["error"]["hint"] = redact(error.hint)
        if debug:
            body["debug"] = debug
        _write_json(context.stderr, body)
    else:
        print(f"Error: {redact(error.message)}", file=context.stderr)
        if error.hint:
            print(f"Hint: {redact(error.hint)}", file=context.stderr)
        if debug:
            print(debug.rstrip(), file=context.stderr)
    return int(error.exit_code or 1)


def write_interaction(message: str) -> None:
    """Write an interactive instruction to stderr.

    Args:
        message: Secret-safe instruction for the user.
    """
    print(redact(message), file=get_output_context().stderr)


def redact(value: str) -> str:
    """Mask recognizable credentials in a string.

    Args:
        value: Potentially sensitive string.

    Returns:
        Redacted text.
    """
    return _SECRET_VALUE.sub(lambda match: f"{match.group(1)}***", value)


def redact_data(value: Any, *, key: str | None = None) -> Any:
    """Recursively mask secret-like fields.

    Args:
        value: Value to sanitize.
        key: Field name containing the value.

    Returns:
        JSON-compatible sanitized value.
    """
    if key is not None and _SECRET_KEY.search(key):
        return "***"
    if isinstance(value, BaseModel):
        return redact_data(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return {
            str(item_key): redact_data(item, key=str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [redact_data(item) for item in value]
    if isinstance(value, str):
        return redact(value)
    return _json_value(value)


def _emit_text(context: OutputContext, result: CommandResult) -> None:
    """Render one result as rich or plain text."""
    value = result.items if result.items is not None else result.item
    if context.rich:
        console = Console(file=context.stdout, force_terminal=True)
        if isinstance(value, dict):
            table = Table(show_header=False, box=None)
            table.add_column("Field", style="bold")
            table.add_column("Value")
            for key, item in value.items():
                table.add_row(str(key), _display_value(item))
            console.print(table)
        elif isinstance(value, list):
            _emit_rich_list(console, value)
        elif value is not None:
            console.print(_display_value(value))
        for warning in result.warnings:
            Console(file=context.stderr).print(f"Warning: {warning}")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            print(f"{key}: {_display_value(item)}", file=context.stdout)
    elif isinstance(value, list):
        for item in value:
            print(_display_value(item), file=context.stdout)
    elif value is not None:
        print(_display_value(value), file=context.stdout)
    for warning in result.warnings:
        print(f"Warning: {warning}", file=context.stderr)


def _emit_rich_list(console: Console, values: list[Any]) -> None:
    """Render a list as a compact table when possible."""
    if values and all(isinstance(value, dict) for value in values):
        keys: list[str] = []
        for value in values:
            for key in value:
                if key not in keys:
                    keys.append(key)
        table = Table()
        for key in keys:
            table.add_column(key)
        for value in values:
            table.add_row(*[_display_value(value.get(key)) for key in keys])
        console.print(table)
    else:
        for value in values:
            console.print(_display_value(value))


def _display_value(value: Any) -> str:
    """Format a value for a human-oriented cell or line."""
    value = redact_data(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _write_json(stream: TextIO, payload: Any) -> None:
    """Write exactly one compact JSON document."""
    stream.write(json.dumps(redact_data(payload), separators=(",", ":")))
    stream.write("\n")
    stream.flush()


def _json_value(value: Any) -> Any:
    """Convert common model values into JSON-compatible values."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Path)):
        return str(value)
    return value
