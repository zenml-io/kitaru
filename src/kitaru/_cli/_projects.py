"""Project CLI commands."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat
from kitaru.config import KITARU_PROJECT_ENV, ProjectInfo
from kitaru.inspection import serialize_project

from . import project_app
from ._dependencies import cli_dependencies
from ._helpers import (
    DEFAULT_LIST_PAGE,
    DEFAULT_LIST_SIZE,
    OutputFormatOption,
    PaginationPageOption,
    PaginationSizeOption,
    _emit_json_item,
    _emit_json_items,
    _emit_pagination_note,
    _emit_snapshot,
    _emit_warning,
    _exit_with_error,
    _print_success,
    _print_warning,
    _resolve_output_format,
    _validate_pagination,
)


def _warn_project_deprecated(output: CLIOutputFormat) -> None:
    """Warn text users while keeping legacy JSON streams parseable."""
    if output == CLIOutputFormat.JSON:
        return
    _emit_warning(
        "`kitaru project` is deprecated; use `kitaru agents` instead.",
        output=output,
    )


def _project_list_rows(projects: list[ProjectInfo]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru project list`."""
    if not projects:
        return [("Projects", "none found")]

    return [
        (
            project.name,
            f"{project.id}{' (active)' if project.is_active else ''}",
        )
        for project in projects
    ]


def _current_project_rows(project: ProjectInfo) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru project current`."""
    return [
        ("Project", project.name),
        ("Project ID", project.id),
    ]


def _project_show_rows(project: ProjectInfo) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru project show`."""
    return [
        ("Name", project.name),
        ("ID", project.id),
        ("Display name", project.display_name or "not set"),
        ("Description", project.description or "not set"),
        ("Active", "yes" if project.is_active else "no"),
    ]


@project_app.command
def list_(
    *,
    page: PaginationPageOption = DEFAULT_LIST_PAGE,
    size: PaginationSizeOption = DEFAULT_LIST_SIZE,
    output: OutputFormatOption = "text",
) -> None:
    """List Kitaru projects visible to the current user."""
    command = "project.list"
    output_format = _resolve_output_format(output)
    _warn_project_deprecated(output_format)
    page, size = _validate_pagination(
        page=page,
        size=size,
        command=command,
        output=output_format,
    )
    projects = run_with_cli_error_boundary(
        lambda: cli_dependencies().list_projects(page=page, size=size),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_project(project) for project in projects],
            output=output_format,
        )
        return

    if page > DEFAULT_LIST_PAGE and not projects:
        rows: list[tuple[str, str]] = [("Projects", f"no items on page {page}")]
    else:
        rows = _project_list_rows(projects)
    _emit_snapshot("Kitaru projects", rows)
    _emit_pagination_note(
        page=page,
        size=size,
        returned_count=len(projects),
        output=output_format,
    )


@project_app.command
def current(output: OutputFormatOption = "text") -> None:
    """Show the active Kitaru project."""
    command = "project.current"
    output_format = _resolve_output_format(output)
    _warn_project_deprecated(output_format)
    project = run_with_cli_error_boundary(
        cli_dependencies().current_project,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_project(project), output=output_format)
        return

    _emit_snapshot("Kitaru project", _current_project_rows(project))


@project_app.command
def show(
    name_or_id: Annotated[
        str,
        Parameter(help="Project name or ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Show a Kitaru project by name or ID."""
    command = "project.show"
    output_format = _resolve_output_format(output)
    _warn_project_deprecated(output_format)
    project = run_with_cli_error_boundary(
        lambda: cli_dependencies().get_project(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_project(project), output=output_format)
        return

    _emit_snapshot("Kitaru project", _project_show_rows(project))


@project_app.command
def create(
    name: Annotated[
        str,
        Parameter(help="Project name."),
    ],
    *,
    no_activate: Annotated[
        bool | None,
        Parameter(help="Create without activating the project."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Create a Kitaru project on ZenML Pro/Cloud, activating it by default."""
    command = "project.create"
    output_format = _resolve_output_format(output)
    _warn_project_deprecated(output_format)
    result = run_with_cli_error_boundary(
        lambda: cli_dependencies().create_project(
            name,
            activate=not no_activate,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        payload = serialize_project(result.project)
        payload["previous_active_project"] = result.previous_active_project
        payload["activated"] = result.activated
        _emit_json_item(command, payload, output=output_format)
        return

    _print_success(f"Created project: {result.project.name}")
    if result.activated and result.project.is_active:
        if result.previous_active_project is not None:
            print(
                "Activated project: "
                f"{result.previous_active_project} → {result.project.name}"
            )
        else:
            print(f"Activated project: {result.project.name}")
    elif result.activated:
        _print_warning(
            "Project activation is still overridden by the environment.",
            f"Unset or update {KITARU_PROJECT_ENV} to use {result.project.name}.",
        )


@project_app.command
def use(
    name_or_id: Annotated[
        str,
        Parameter(help="Project name or ID to activate."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Use a Kitaru project on ZenML Pro/Cloud as the active default."""
    command = "project.use"
    output_format = _resolve_output_format(output)
    _warn_project_deprecated(output_format)
    project = run_with_cli_error_boundary(
        lambda: cli_dependencies().use_project(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_project(project), output=output_format)
        return

    _print_success(
        f"Activated project: {project.name}",
        detail=f"Project ID: {project.id}",
    )


@project_app.command
def delete(
    name_or_id: Annotated[
        str,
        Parameter(help="Project name or ID to delete."),
    ],
    *,
    yes: Annotated[
        bool,
        Parameter(help="Confirm project deletion."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Delete a Kitaru project on ZenML Pro/Cloud by name or ID."""
    command = "project.delete"
    output_format = _resolve_output_format(output)
    _warn_project_deprecated(output_format)
    if not yes:
        _exit_with_error(
            command,
            f"Kitaru will not delete project '{name_or_id}' without explicit "
            "confirmation. Re-run with --yes if you want to delete it.",
            output=output_format,
        )

    result = run_with_cli_error_boundary(
        lambda: cli_dependencies().delete_project(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_project(result.deleted_project),
            output=output_format,
        )
        return

    _print_success(f"Deleted project: {result.deleted_project.name}")
