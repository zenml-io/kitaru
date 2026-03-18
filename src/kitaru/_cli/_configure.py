"""Persisted CLI preference commands."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat
from kitaru.config import _parse_bool_env

from . import configure_app
from ._helpers import (
    MachineModeOption,
    OutputFormatOption,
    _emit_json_item,
    _exit_with_error,
    _facade_module,
    _machine_mode_context,
    _print_success,
    _resolve_output_and_machine_mode,
)


def _parse_machine_mode_value(raw_value: str) -> bool:
    """Parse a persisted machine_mode setting value."""
    normalized = raw_value.strip()
    if not normalized:
        raise ValueError(
            "`machine_mode` requires a boolean value. Use true/false/1/0/yes/no/on/off."
        )
    return _parse_bool_env("machine_mode", normalized)


@configure_app.command
def set_(
    key: Annotated[
        str,
        Parameter(help="Persisted setting key (currently only `machine_mode`)."),
    ],
    value: Annotated[
        str,
        Parameter(help="Setting value."),
    ],
    *,
    output: OutputFormatOption = "text",
    machine: MachineModeOption = None,
) -> None:
    """Persist one Kitaru CLI/runtime preference."""
    command = "configure.set"
    output_format, machine_mode = _resolve_output_and_machine_mode(output, machine)
    normalized_key = key.strip()
    with _machine_mode_context(machine_mode):
        if normalized_key != "machine_mode":
            _exit_with_error(
                command,
                (
                    f"Unsupported setting `{key}`. "
                    "Only `machine_mode` is supported right now."
                ),
                output=output_format,
            )

        try:
            parsed_value = _parse_machine_mode_value(value)
        except ValueError as exc:
            _exit_with_error(command, str(exc), output=output_format)

        stored_value = run_with_cli_error_boundary(
            lambda: _facade_module().set_global_machine_mode(parsed_value),
            command=command,
            output=output_format,
            exit_with_error=_exit_with_error,
            machine_mode=machine_mode,
            handled_exceptions=(ValueError,),
        )

        payload = {"key": "machine_mode", "value": stored_value}
        if output_format == CLIOutputFormat.JSON:
            _emit_json_item(command, payload, output=output_format)
            return

        rendered_value = "true" if stored_value else "false"
        _print_success(f"Saved setting: machine_mode = {rendered_value}")
