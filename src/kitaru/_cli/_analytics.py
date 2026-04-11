"""CLI commands for managing analytics preferences."""

from __future__ import annotations

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat

from . import analytics_app
from ._helpers import (
    OutputFormatOption,
    _emit_json_item,
    _emit_snapshot,
    _exit_with_error,
    _print_success,
    _resolve_output_format,
)


def _get_analytics_opt_in() -> bool:
    """Read the persisted analytics preference from GlobalConfiguration."""
    from zenml.config.global_config import GlobalConfiguration

    return GlobalConfiguration().analytics_opt_in


def _set_analytics_opt_in(value: bool) -> None:
    """Persist the analytics preference to GlobalConfiguration."""
    from zenml.config.global_config import GlobalConfiguration

    GlobalConfiguration().analytics_opt_in = value


def _toggle_analytics(
    value: bool,
    *,
    output: str,
    success_message: str,
    detail: str | None = None,
) -> None:
    """Shared implementation for opt-in / opt-out commands."""
    command = f"analytics.opt-{'in' if value else 'out'}"
    output_format = _resolve_output_format(output)

    run_with_cli_error_boundary(
        lambda: _set_analytics_opt_in(value),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(Exception,),
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            {"analytics_opt_in": value},
            output=output_format,
        )
        return

    _print_success(success_message, detail=detail)


@analytics_app.command
def status(*, output: OutputFormatOption = "text") -> None:
    """Show the current analytics preference."""
    command = "analytics.status"
    output_format = _resolve_output_format(output)

    opted_in = run_with_cli_error_boundary(
        _get_analytics_opt_in,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(Exception,),
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            {"analytics_opt_in": opted_in},
            output=output_format,
        )
        return

    label = "enabled" if opted_in else "disabled"
    _emit_snapshot("Kitaru analytics", [("Status", label)])


@analytics_app.command(name="opt-in")
def opt_in(*, output: OutputFormatOption = "text") -> None:
    """Enable anonymous usage analytics."""
    _toggle_analytics(True, output=output, success_message="Analytics enabled.")


@analytics_app.command(name="opt-out")
def opt_out(*, output: OutputFormatOption = "text") -> None:
    """Disable anonymous usage analytics."""
    _toggle_analytics(
        False,
        output=output,
        success_message="Analytics disabled.",
        detail="Persisted and applies to all surfaces including MCP.",
    )
