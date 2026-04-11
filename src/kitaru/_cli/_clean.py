"""Cleanup CLI commands for resetting Kitaru state."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from kitaru._cleanup import (
    CleanScope,
    CleanupPlan,
    CleanupPreviewEntry,
    CleanupResult,
    PreviewEntryType,
    build_cleanup_plan,
    build_cleanup_preview_result,
    execute_cleanup_plan,
    format_size,
    serialize_cleanup_result,
)
from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat

from . import clean_app
from ._helpers import (
    OutputFormatOption,
    SnapshotSection,
    _emit_json_item,
    _emit_snapshot_sections,
    _exit_with_error,
    _is_input_interactive,
    _print_warning,
    _resolve_output_format,
)

# ---------------------------------------------------------------------------
# Text rendering
# ---------------------------------------------------------------------------


def _render_preview_entry(
    entry: CleanupPreviewEntry,
    *,
    indent: int = 4,
) -> list[str]:
    """Render one preview entry as indented text lines."""
    prefix = " " * indent
    lines: list[str] = []

    size_str = f" ({format_size(entry.size_bytes)})" if entry.size_bytes else ""
    note_str = f" — {entry.note}" if entry.note else ""

    if entry.entry_type == PreviewEntryType.BACKUP:
        lines.append(f"{prefix}Backup: {entry.path}{note_str}")
    else:
        lines.append(f"{prefix}{entry.path}{size_str}{note_str}")

    for child in entry.children:
        lines.extend(_render_preview_entry(child, indent=indent + 2))

    return lines


def _render_cleanup_preview(plan: CleanupPlan) -> None:
    """Render the dry-run preview to stdout."""
    lines: list[str] = ["Would delete:"]

    if plan.scope in (CleanScope.GLOBAL, CleanScope.ALL):
        global_entries = [
            e
            for e in plan.preview_entries
            if e.path == plan.global_config_root
            or e.entry_type == PreviewEntryType.BACKUP
        ]
        if global_entries:
            lines.append("  Global config:")
            for entry in global_entries:
                lines.extend(_render_preview_entry(entry, indent=4))

    project_entries = [
        e
        for e in plan.preview_entries
        if plan.project_config_path and e.path == plan.project_config_path
    ]
    if project_entries:
        lines.append("  Project config:")
        for entry in project_entries:
            lines.extend(_render_preview_entry(entry, indent=4))

    if plan.local_server_would_stop and plan.local_server_status:
        lines.append(f"  Local server: {plan.local_server_status} (would be stopped)")

    lines.append(f"  Total: ~{format_size(plan.total_bytes)}")

    print("\n".join(lines))


def _render_cleanup_result(result: CleanupResult) -> None:
    """Render the cleanup result to stdout."""
    if result.aborted:
        print("Aborted.")
        return

    rows: list[tuple[str, str]] = [
        ("Scope", result.scope.value),
    ]
    if result.backup_path:
        rows.append(("Database backup", result.backup_path))
    if result.deleted_paths:
        for path in result.deleted_paths:
            rows.append(("Deleted", path))
    if result.local_server_stopped:
        rows.append(("Local server", "stopped"))
    if result.reinitialized_project:
        rows.append(("Project re-initialized", "yes"))

    sections = [SnapshotSection(title=None, rows=rows)]
    warning = "\n".join(result.warnings) if result.warnings else None

    _emit_snapshot_sections("Kitaru clean", sections, warning)


# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------


def _prompt_yes_no(message: str) -> bool:
    """Prompt the user for yes/no confirmation."""
    if not _is_input_interactive():
        return False
    try:
        response = input(f"{message} [y/N] ")
    except (EOFError, KeyboardInterrupt):
        return False
    return response.strip().lower() in ("y", "yes")


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


def _run_clean(
    scope: CleanScope,
    *,
    yes: bool,
    dry_run: bool,
    force: bool,
    output: str,
) -> None:
    """Shared implementation for all clean subcommands."""
    command = f"clean.{scope.value}"
    output_format = _resolve_output_format(output)

    plan = run_with_cli_error_boundary(
        lambda: build_cleanup_plan(scope),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(Exception,),
    )

    from kitaru.analytics import AnalyticsEvent, track

    if dry_run:
        dry_result = build_cleanup_preview_result(plan)

        track(
            AnalyticsEvent.CLEAN_COMPLETED,
            {"scope": scope.value, "dry_run": True},
        )

        if output_format == CLIOutputFormat.JSON:
            _emit_json_item(
                command,
                serialize_cleanup_result(dry_result),
                output=output_format,
            )
            return
        if plan.custom_config_path_warning:
            _print_warning(plan.custom_config_path_warning)
        _render_cleanup_preview(plan)
        return

    if not yes and not _is_input_interactive():
        _exit_with_error(
            command,
            "Non-interactive environment requires --yes to proceed with cleanup.",
            output=output_format,
        )

    if plan.custom_config_path_warning:
        _print_warning(plan.custom_config_path_warning)

    result = run_with_cli_error_boundary(
        lambda: execute_cleanup_plan(
            plan,
            yes=yes,
            force=force,
            prompt_confirm=_prompt_yes_no,
            prompt_reinitialize=_prompt_yes_no,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(Exception,),
    )

    if not result.aborted:
        track(
            AnalyticsEvent.CLEAN_COMPLETED,
            {"scope": scope.value, "dry_run": False},
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_cleanup_result(result),
            output=output_format,
        )
        return

    _render_cleanup_result(result)


@clean_app.command
def project(
    *,
    yes: Annotated[
        bool,
        Parameter(
            alias=["-y"],
            help="Skip confirmation prompt.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        Parameter(help="Show what would be deleted without deleting."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Remove the project-local .kitaru/ directory."""
    _run_clean(CleanScope.PROJECT, yes=yes, dry_run=dry_run, force=False, output=output)


@clean_app.command(name="global")
def global_(
    *,
    yes: Annotated[
        bool,
        Parameter(
            alias=["-y"],
            help="Skip confirmation prompt.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        Parameter(help="Show what would be deleted without deleting."),
    ] = False,
    force: Annotated[
        bool,
        Parameter(help="Required when cleanup would destroy model registry aliases."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Reset all local Kitaru and ZenML state on this machine."""
    _run_clean(CleanScope.GLOBAL, yes=yes, dry_run=dry_run, force=force, output=output)


@clean_app.command(name="all")
def all_(
    *,
    yes: Annotated[
        bool,
        Parameter(
            alias=["-y"],
            help="Skip confirmation prompt.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        Parameter(help="Show what would be deleted without deleting."),
    ] = False,
    force: Annotated[
        bool,
        Parameter(help="Required when cleanup would destroy model registry aliases."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Reset all local state: project + global."""
    _run_clean(CleanScope.ALL, yes=yes, dry_run=dry_run, force=force, output=output)
