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
import traceback as traceback_module
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Literal, TextIO

from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from kitaru.cli.presentation import HumanField, HumanView, get_human_view
from kitaru.cli.redaction import redact, redact_data

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

_ERROR_TITLES = {
    "authentication_failed": "Authentication failed",
    "conflict": "Conflict",
    "interaction_required": "Interaction required",
    "internal_error": "Internal error",
    "interrupted": "Interrupted",
    "invalid_arguments": "Invalid arguments",
    "invalid_configuration": "Invalid configuration",
    "network_error": "Network error",
    "not_found": "Not found",
    "partial_failure": "Partially completed",
    "remote_canceled": "Remote work canceled",
    "remote_failed": "Remote work failed",
    "timeout": "Timed out",
}

_DOCTOR_STATUS_STYLES = {
    "pass": "green",
    "fail": "red",
    "warn": "yellow",
    "skip": "dim",
}


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
    debug: bool
    traceback: bool
    stdout: TextIO
    stderr: TextIO
    rich: bool
    terminal_width: int | None = None


_OUTPUT_CONTEXT: ContextVar[OutputContext | None] = ContextVar(
    "kitaru_cli_output", default=None
)


def set_output_context(context: OutputContext) -> Token[OutputContext | None]:
    """Set the rendering context for this invocation.

    Args:
        context: Resolved rendering settings.

    Returns:
        Context variable token used to restore the previous value.
    """
    return _OUTPUT_CONTEXT.set(context)


def reset_output_context(token: Token[OutputContext | None]) -> None:
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
        if context.rich:
            console = _get_console(context, context.stderr)
            title = _ERROR_TITLES.get(
                error.kind, error.kind.replace("_", " ").capitalize()
            )
            console.print(f"[bold red]Error: {title}[/bold red]")
            console.print(Text(redact(error.message)))
            hint = error.hint or _get_human_error_hint(context.command, error.kind)
            if hint:
                label = Text("Try:", style="bold")
                label.append(f" {redact(hint)}")
                console.print(label)
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


def _emit_text(context: OutputContext, result: CommandResult) -> None:
    """Render one result as rich or plain text."""
    value = result.items if result.items is not None else result.item
    if context.rich:
        console = _get_console(context, context.stdout)
        view = get_human_view(context.command)
        if isinstance(value, dict) and view is not None:
            _emit_human_detail(console, value, view)
        elif isinstance(value, list) and view is not None:
            _emit_human_list(console, value, view)
        elif isinstance(value, dict):
            _emit_rich_detail(console, value)
        elif isinstance(value, list):
            _emit_rich_list(console, value)
        elif value is not None:
            console.print(Text(_display_value(value)))
        _emit_human_footer(console, result)
        for warning in result.warnings:
            Console(file=context.stderr).print(Text(f"Warning: {warning}"))
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


def _get_console(context: OutputContext, stream: TextIO) -> Console:
    """Build a Rich console using real or test-supplied terminal dimensions."""
    return Console(
        file=stream,
        force_terminal=True,
        force_jupyter=False,
        width=context.terminal_width,
        _environ={} if context.terminal_width is not None else None,
        highlight=False,
    )


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
            table.add_column(Text(key))
        for value in values:
            table.add_row(*[Text(_display_value(value.get(key))) for key in keys])
        console.print(table)
    else:
        for value in values:
            console.print(Text(_display_value(value)))


def _emit_rich_detail(console: Console, value: dict[str, Any]) -> None:
    """Render an unregistered mapping without turning nested data into columns."""
    table = Table(show_header=False, box=None)
    table.add_column("Field", style="bold")
    table.add_column("Value")
    for key, item in value.items():
        table.add_row(Text(str(key)), Text(_detail_value(item)))
    console.print(table)


def _emit_human_list(console: Console, values: list[Any], view: HumanView) -> None:
    """Render selected list fields for the available console width."""
    if not values:
        console.print(f"[dim]{view.empty_message}[/dim]")
        return
    if not all(isinstance(value, dict) for value in values):
        _emit_rich_list(console, values)
        return
    fields = tuple(
        field for field in view.fields if field.min_console_width <= console.width
    )
    if not fields:
        _emit_rich_list(console, values)
        return
    table = Table(title=view.title, title_justify="left")
    for human_field in fields:
        table.add_column(human_field.label, no_wrap=human_field.no_wrap)
    for value in values:
        table.add_row(
            *[Text(_human_field_value(value, human_field)) for human_field in fields]
        )
    console.print(table)


def _emit_human_detail(
    console: Console, value: dict[str, Any], view: HumanView
) -> None:
    """Render a selected detail summary in meaningful sections."""
    if view.renderer == "doctor":
        _emit_doctor(console, value)
        return
    if not view.sections:
        fields = tuple(
            field for field in view.fields if field.min_console_width <= console.width
        )
        _emit_human_section(console, view.title, value, fields)
        return
    for section in view.sections:
        present = tuple(
            field for field in section.fields if _has_human_field(value, field)
        )
        if present:
            _emit_human_section(console, section.title, value, present)


def _emit_doctor(console: Console, value: dict[str, Any]) -> None:
    """Render diagnostic checks as an operational checklist."""
    healthy = bool(value.get("healthy"))
    label = "healthy" if healthy else "needs attention"
    style = "green" if healthy else "red"
    console.print(f"Kitaru is [{style}]{label}[/{style}].")
    checks = value.get("checks")
    if not isinstance(checks, list) or not checks:
        return
    table = Table(title="Checks", title_justify="left")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Required")
    table.add_column("Detail")
    for check in checks:
        if not isinstance(check, dict):
            continue
        status = _display_value(check.get("status"))
        table.add_row(
            Text(_display_value(check.get("name"))),
            Text(status, style=_DOCTOR_STATUS_STYLES.get(status, "")),
            Text(_display_value(check.get("required"))),
            Text(_display_value(check.get("detail"))),
        )
    console.print(table)


def _emit_human_section(
    console: Console,
    title: str,
    value: dict[str, Any],
    fields: tuple[HumanField, ...],
) -> None:
    """Render one human detail section."""
    console.print(Rule(title, align="left", style="dim"))
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("Field", style="bold", no_wrap=True)
    table.add_column("Value")
    for human_field in fields:
        table.add_row(
            human_field.label,
            Text(_human_field_value(value, human_field, detail=True)),
        )
    console.print(table)


def _emit_human_footer(console: Console, result: CommandResult) -> None:
    """Render pagination, links, and suggested follow-up commands."""
    if result.items is not None and result.page is not None:
        noun = "item" if len(result.items) == 1 else "items"
        console.print(f"[dim]Showing {len(result.items)} {noun}.[/dim]")
    if result.page and result.page.get("next_cursor"):
        line = Text("Next cursor:", style="dim")
        line.append(f" {result.page['next_cursor']}")
        console.print(line)
    for label, link in result.links.items():
        line = Text(f"{label}:", style="dim")
        line.append(f" {link}")
        console.print(line)
    for action in result.next_actions:
        line = Text("Next:", style="dim")
        line.append(f" {action}")
        console.print(line)


def _get_human_error_hint(command: str, kind: str) -> str | None:
    """Return a bounded recovery hint for common interactive failures."""
    words = command.replace(".", " ")
    if kind == "invalid_arguments":
        if command == "cli":
            return "Run `kitaru --help` to inspect available commands."
        return f"Run `kitaru {words} --help` to check valid arguments."
    if kind == "authentication_failed":
        return "Run `kitaru login`, then retry the command."
    if kind == "network_error":
        return "Run `kitaru status` to check the selected server, then retry."
    if kind == "not_found":
        group = command.split(".", 1)[0]
        if group in {
            "agent",
            "cohort",
            "evaluation",
            "evaluator",
            "experiment",
            "importer",
            "session",
            "worker",
        }:
            return f"Run `kitaru {group} list` to inspect available records."
    return None


_MISSING = object()


def _get_human_field(value: dict[str, Any], field: HumanField) -> Any:
    """Resolve a field path, trying pipe-separated alternatives."""
    for path in field.key.split("|"):
        current: Any = value
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = _MISSING
                break
            current = current[part]
        if current is not _MISSING and current is not None:
            return current
    return _MISSING


def _has_human_field(value: dict[str, Any], field: HumanField) -> bool:
    """Return whether a selected field exists in a result mapping."""
    return _get_human_field(value, field) is not _MISSING


def _human_field_value(
    value: dict[str, Any], field: HumanField, *, detail: bool = False
) -> str:
    """Resolve and format one selected human field."""
    item = _get_human_field(value, field)
    if item is _MISSING:
        return "-"
    if field.formatter is not None:
        return field.formatter(redact_data(item))
    if detail:
        return _detail_value(item)
    return _display_value(item)


def _detail_value(value: Any) -> str:
    """Format nested detail data as readable indented JSON."""
    value = redact_data(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, sort_keys=True)
    return _display_value(value)


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
