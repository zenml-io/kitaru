"""Shared helpers for the Kitaru CLI."""

from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass
from datetime import datetime
from types import ModuleType
from typing import Annotated, Any, NoReturn

from cyclopts import Parameter
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text

from kitaru.cli_output import (
    CLIOutputFormat,
    CommandEnvelope,
    emit_command_envelope,
    normalize_output_format,
)
from kitaru.cli_output import exit_with_error as _structured_exit_with_error


def _facade_module() -> ModuleType:
    """Return the compatibility facade module used by tests and callers."""
    module = sys.modules.get("kitaru.cli")
    if module is None:
        module = importlib.import_module("kitaru.cli")
    return module


@dataclass(frozen=True)
class SnapshotSection:
    """One renderable snapshot section."""

    title: str | None
    rows: list[tuple[str, str]]


OutputFormatOption = Annotated[
    str,
    Parameter(
        alias=["-o"],
        help='Output format: "text" (default) or "json".',
    ),
]


def _format_timestamp(value: datetime | None) -> str:
    """Format optional timestamps for CLI output."""
    if value is None:
        return "not available"
    return value.isoformat(timespec="seconds")


def _is_interactive(*, stderr: bool = False) -> bool:
    """Check whether the target stream is an interactive terminal."""
    stream = sys.stderr if stderr else sys.stdout
    return hasattr(stream, "isatty") and stream.isatty()


def _value_style(value: str) -> str:
    """Choose a Rich style based on the value content."""
    if value in ("unavailable", "not set", "not started"):
        return "dim"
    if value.startswith(("http://", "https://")):
        return "underline"
    return ""


def _render_rich_snapshot(
    title: str,
    rows: list[tuple[str, str]],
    warning: str | None = None,
) -> None:
    """Render a snapshot as a styled Rich panel with key/value lines."""
    lines = Text()
    for i, (label, value) in enumerate(rows):
        lines.append(f"  {label}: ", style="bold cyan")
        lines.append(value, style=_value_style(value))
        if i < len(rows) - 1:
            lines.append("\n")

    elements: list[Text] = [lines]
    if warning:
        warn_text = Text("\n\n  Warning: ", style="bold yellow")
        warn_text.append(warning, style="yellow")
        elements.append(warn_text)

    Console().print(
        Panel(
            Group(*elements),
            title=f"[bold]{title}[/bold]",
            title_align="left",
            border_style="dim",
            expand=False,
            padding=(0, 1),
        )
    )


def _render_rich_snapshot_sections(
    title: str,
    sections: list[SnapshotSection],
    warning: str | None = None,
) -> None:
    """Render a multi-section snapshot as a styled Rich panel."""
    lines = Text()
    for index, section in enumerate(sections):
        if index > 0:
            lines.append("\n\n")
        if section.title:
            lines.append(section.title, style="bold magenta")
            lines.append("\n")
        for row_index, (label, value) in enumerate(section.rows):
            lines.append(f"  {label}: ", style="bold cyan")
            lines.append(value, style=_value_style(value))
            if row_index < len(section.rows) - 1:
                lines.append("\n")

    elements: list[Text] = [lines]
    if warning:
        warn_text = Text("\n\n  Warning: ", style="bold yellow")
        warn_text.append(warning, style="yellow")
        elements.append(warn_text)

    Console().print(
        Panel(
            Group(*elements),
            title=f"[bold]{title}[/bold]",
            title_align="left",
            border_style="dim",
            expand=False,
            padding=(0, 1),
        )
    )


def _print_success(message: str, detail: str | None = None) -> None:
    """Print a success message, styled when the terminal is interactive."""
    if _is_interactive():
        console = Console()
        console.print(Text(message, style="green"))
        if detail:
            console.print(Text(f"  {detail}", style="dim"))
    else:
        print(message)
        if detail:
            print(f"  {detail}")


def _resolve_output_format(raw_output: str) -> CLIOutputFormat:
    """Normalize a CLI output mode and fail with a text error if invalid."""
    try:
        return normalize_output_format(raw_output)
    except ValueError as exc:
        _structured_exit_with_error(
            "cli",
            str(exc),
            output=CLIOutputFormat.TEXT,
            error_type=type(exc).__name__,
        )


def _exit_with_error(
    command: str,
    message: str | None = None,
    *,
    output: CLIOutputFormat = CLIOutputFormat.TEXT,
    error_type: str | None = None,
) -> NoReturn:
    """Print a format-aware CLI error and exit with a non-zero status."""
    if message is None:
        message = command
        command = "cli"

    if output == CLIOutputFormat.JSON:
        _structured_exit_with_error(
            command,
            message,
            output=output,
            error_type=error_type,
        )

    if _is_interactive(stderr=True):
        err = Text("Error: ", style="bold red")
        err.append(message, style="red")
        Console(stderr=True).print(err)
    else:
        print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def _render_plain_snapshot(
    title: str,
    rows: list[tuple[str, str]],
    warning: str | None = None,
) -> str:
    """Render a snapshot as plain indented text for non-TTY output."""
    lines = [title]
    for label, value in rows:
        lines.append(f"  {label}: {value}")
    if warning:
        lines.append(f"  Warning: {warning}")
    return "\n".join(lines)


def _render_plain_snapshot_sections(
    title: str,
    sections: list[SnapshotSection],
    warning: str | None = None,
) -> str:
    """Render a multi-section snapshot as plain indented text."""
    lines = [title]
    for section in sections:
        if section.title:
            lines.append("")
            lines.append(section.title)
        for label, value in section.rows:
            lines.append(f"  {label}: {value}")
    if warning:
        lines.append("")
        lines.append(f"  Warning: {warning}")
    return "\n".join(lines)


def _emit_snapshot(
    title: str,
    rows: list[tuple[str, str]],
    warning: str | None = None,
) -> None:
    """Render key/value snapshots in rich or plain-text mode."""
    if _is_interactive():
        _render_rich_snapshot(title, rows, warning)
    else:
        print(_render_plain_snapshot(title, rows, warning))


def _emit_snapshot_sections(
    title: str,
    sections: list[SnapshotSection],
    warning: str | None = None,
) -> None:
    """Render multi-section snapshots in rich or plain-text mode."""
    if _is_interactive():
        _render_rich_snapshot_sections(title, sections, warning)
    else:
        print(_render_plain_snapshot_sections(title, sections, warning))


def _emit_json_item(
    command: str,
    item: dict[str, Any],
    *,
    output: CLIOutputFormat,
) -> None:
    """Emit a single structured JSON item when JSON mode is enabled."""
    emit_command_envelope(
        CommandEnvelope(command=command, item=item),
        output=output,
    )


def _emit_json_items(
    command: str,
    items: list[dict[str, Any]],
    *,
    output: CLIOutputFormat,
) -> None:
    """Emit a structured JSON list result when JSON mode is enabled."""
    emit_command_envelope(
        CommandEnvelope(command=command, items=items, count=len(items)),
        output=output,
    )
