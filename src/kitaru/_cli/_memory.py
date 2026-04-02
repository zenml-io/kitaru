"""Memory CLI commands."""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from cyclopts import Parameter

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat

from . import memory_app
from ._helpers import (
    OutputFormatOption,
    SnapshotSection,
    _emit_json_item,
    _emit_json_items,
    _emit_snapshot_sections,
    _emit_table,
    _exit_with_error,
    _facade_module,
    _print_success,
    _resolve_output_format,
)


def _require_scope(
    scope: str | None,
    *,
    command: str,
    output: CLIOutputFormat,
) -> str:
    """Validate that ``--scope`` was provided, or exit with a helpful hint."""
    if scope is not None:
        return scope
    _exit_with_error(
        command,
        "Missing required option `--scope`. "
        "Run `kitaru memory scopes` to see available scopes.",
        output=output,
    )
    raise SystemExit(1)  # unreachable; satisfies type checker


def _memory_timestamp(value: str | None) -> str:
    """Render an optional serialized timestamp for CLI output."""
    return value or "not available"


def _memory_execution_label(execution_id: str | None) -> str:
    """Render the producing execution label for one memory entry."""
    return execution_id or "detached"


def _stringify_memory_value(value: Any, *, value_format: str) -> str:
    """Render one loaded memory value for text-mode CLI output."""
    if value_format == "json":
        rendered = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False)
    else:
        rendered = str(value)
    return rendered.replace("\n", "\n    ")


def _parse_memory_cli_value(raw_value: str) -> Any:
    """Parse a CLI value as JSON when possible, else preserve the raw string."""
    try:
        return json.loads(raw_value)
    except json.JSONDecodeError:
        return raw_value


def _memory_entry_rows(entry: dict[str, Any]) -> list[tuple[str, str]]:
    """Build metadata rows for one serialized memory entry."""
    return [
        ("Key", str(entry["key"])),
        ("Scope", str(entry["scope"])),
        ("Scope type", str(entry["scope_type"])),
        ("Version", str(entry["version"])),
        ("Value type", str(entry["value_type"])),
        ("Deleted", "yes" if entry["is_deleted"] else "no"),
        ("Created", _memory_timestamp(entry.get("created_at"))),
        ("Execution", _memory_execution_label(entry.get("execution_id"))),
        ("Artifact ID", str(entry["artifact_id"])),
    ]


def _memory_list_rows(entries: list[dict[str, Any]]) -> list[list[str]]:
    """Build table rows for `kitaru memory list`."""
    return [
        [
            str(entry["key"]),
            str(entry["value_type"]),
            str(entry["version"]),
            _memory_timestamp(entry.get("created_at")),
            str(entry["scope_type"]),
            _memory_execution_label(entry.get("execution_id")),
        ]
        for entry in entries
    ]


def _memory_history_rows(entries: list[dict[str, Any]]) -> list[list[str]]:
    """Build table rows for `kitaru memory history`."""
    return [
        [
            str(entry["version"]),
            "yes" if entry["is_deleted"] else "no",
            str(entry["value_type"]),
            _memory_timestamp(entry.get("created_at")),
            _memory_execution_label(entry.get("execution_id")),
            str(entry["artifact_id"]),
        ]
        for entry in entries
    ]


def _memory_value_section(payload: dict[str, Any]) -> SnapshotSection:
    """Build the value section for `kitaru memory get` text output."""
    value_format = payload.get("value_format")
    if value_format is None or "value" not in payload:
        return SnapshotSection(
            title="Value",
            rows=[
                ("Status", "unavailable"),
                (
                    "Reason",
                    (
                        "The backend returned memory metadata without a "
                        "materialized value."
                    ),
                ),
            ],
        )

    return SnapshotSection(
        title="Value",
        rows=[
            ("Format", str(value_format)),
            (
                "Value",
                _stringify_memory_value(
                    payload["value"],
                    value_format=str(value_format),
                ),
            ),
        ],
    )


def _memory_scopes_rows(scopes: list[dict[str, Any]]) -> list[list[str]]:
    """Build table rows for `kitaru memory scopes`."""
    return [
        [
            str(s["scope"]),
            str(s["scope_type"]),
            str(s["entry_count"]),
        ]
        for s in scopes
    ]


@memory_app.command(name="scopes")
def scopes_(
    *,
    output: OutputFormatOption = "text",
) -> None:
    """List all discovered memory scopes."""
    command = "memory.scopes"
    output_format = _resolve_output_format(output)
    facade = _facade_module()
    scopes = run_with_cli_error_boundary(
        lambda: facade.scopes_memory_payload(
            facade.KitaruClient(),
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(command, scopes, output=output_format)
        return

    _emit_table(
        "Kitaru memory scopes",
        ["Scope", "Scope Type", "Entries"],
        _memory_scopes_rows(scopes),
        empty_message="no memory scopes found",
    )


@memory_app.command
def list_(
    *,
    scope: Annotated[
        str | None,
        Parameter(help="Memory scope to inspect. [required]", show_default=False),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """List active memory entries for one explicit scope."""
    command = "memory.list"
    output_format = _resolve_output_format(output)
    scope = _require_scope(scope, command=command, output=output_format)
    facade = _facade_module()
    entries = run_with_cli_error_boundary(
        lambda: facade.list_memory_payload(
            facade.KitaruClient(),
            scope=scope,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(command, entries, output=output_format)
        return

    _emit_table(
        f"Kitaru memory ({scope})",
        ["Key", "Type", "Version", "Updated", "Scope Type", "Execution"],
        _memory_list_rows(entries),
        empty_message=(
            f"none found for scope `{scope}`. "
            "Run `kitaru memory scopes` to see available scopes."
        ),
    )


@memory_app.command(name="get")
def get_(
    key: Annotated[
        str,
        Parameter(
            help="Memory key to read.",
            allow_leading_hyphen=True,
        ),
    ],
    *,
    scope: Annotated[
        str | None,
        Parameter(help="Memory scope to read from. [required]", show_default=False),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Read the latest value for one memory key in one explicit scope."""
    command = "memory.get"
    output_format = _resolve_output_format(output)
    scope = _require_scope(scope, command=command, output=output_format)
    facade = _facade_module()
    payload = run_with_cli_error_boundary(
        lambda: facade.get_memory_payload(
            facade.KitaruClient(),
            key=key,
            scope=scope,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if payload is None:
        _exit_with_error(
            command,
            f"No memory entry found for key `{key}` in scope `{scope}`.",
            output=output_format,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, payload, output=output_format)
        return

    _emit_snapshot_sections(
        "Kitaru memory",
        [
            SnapshotSection(title="Metadata", rows=_memory_entry_rows(payload)),
            _memory_value_section(payload),
        ],
    )


@memory_app.command(name="set")
def set_(
    key: Annotated[
        str,
        Parameter(
            help="Memory key to write.",
            allow_leading_hyphen=True,
        ),
    ],
    value: Annotated[
        str,
        Parameter(
            help=(
                "Memory value. Parsed as JSON when possible; otherwise stored as a "
                "raw string."
            ),
            allow_leading_hyphen=True,
        ),
    ],
    *,
    scope: Annotated[
        str | None,
        Parameter(help="Memory scope to write into. [required]", show_default=False),
    ] = None,
    scope_type: Annotated[
        Literal["namespace", "flow", "execution"] | None,
        Parameter(
            help=(
                "Optional scope classification metadata: namespace, flow, or execution."
            )
        ),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Write one memory value into one explicit scope."""
    command = "memory.set"
    output_format = _resolve_output_format(output)
    scope = _require_scope(scope, command=command, output=output_format)
    facade = _facade_module()
    payload = run_with_cli_error_boundary(
        lambda: facade.set_memory_payload(
            facade.KitaruClient(),
            key=key,
            value=_parse_memory_cli_value(value),
            scope=scope,
            scope_type=scope_type,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, payload, output=output_format)
        return

    _print_success(
        f"Saved memory: {payload['key']}",
        detail=(
            f"Scope: {payload['scope']} ({payload['scope_type']}) | "
            f"Version: {payload['version']} | Type: {payload['value_type']}"
        ),
    )


@memory_app.command(name="delete")
def delete_(
    key: Annotated[
        str,
        Parameter(
            help="Memory key to soft-delete.",
            allow_leading_hyphen=True,
        ),
    ],
    *,
    scope: Annotated[
        str | None,
        Parameter(help="Memory scope to delete from. [required]", show_default=False),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Soft-delete one memory key from one explicit scope."""
    command = "memory.delete"
    output_format = _resolve_output_format(output)
    scope = _require_scope(scope, command=command, output=output_format)
    facade = _facade_module()
    payload = run_with_cli_error_boundary(
        lambda: facade.delete_memory_payload(
            facade.KitaruClient(),
            key=key,
            scope=scope,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if payload is None:
        _exit_with_error(
            command,
            f"No memory entry found for key `{key}` in scope `{scope}`.",
            output=output_format,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, payload, output=output_format)
        return

    _print_success(
        f"Deleted memory: {payload['key']}",
        detail=(
            f"Scope: {payload['scope']} ({payload['scope_type']}) | "
            f"Tombstone version: {payload['version']}"
        ),
    )


@memory_app.command(name="history")
def history_(
    key: Annotated[
        str,
        Parameter(
            help="Memory key whose version history to inspect.",
            allow_leading_hyphen=True,
        ),
    ],
    *,
    scope: Annotated[
        str | None,
        Parameter(help="Memory scope to inspect. [required]", show_default=False),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Show all versions for one memory key in one explicit scope."""
    command = "memory.history"
    output_format = _resolve_output_format(output)
    scope = _require_scope(scope, command=command, output=output_format)
    facade = _facade_module()
    entries = run_with_cli_error_boundary(
        lambda: facade.history_memory_payload(
            facade.KitaruClient(),
            key=key,
            scope=scope,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if not entries:
        _exit_with_error(
            command,
            f"No memory history found for key `{key}` in scope `{scope}`.",
            output=output_format,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(command, entries, output=output_format)
        return

    _emit_table(
        f"Kitaru memory history ({scope}/{key})",
        ["Version", "Deleted", "Type", "Updated", "Execution", "Artifact ID"],
        _memory_history_rows(entries),
    )
