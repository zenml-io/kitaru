"""Project initialization CLI command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from kitaru._env import KITARU_REPOSITORY_DIRECTORY_NAME
from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat

from . import app
from ._helpers import (
    OutputFormatOption,
    _emit_json_item,
    _exit_with_error,
    _print_success,
    _resolve_output_format,
)

_LEGACY_REPOSITORY_DIRECTORY_NAME = ".zen"


@app.command
def init(
    path: Annotated[
        str | None,
        Parameter(
            help="Directory to initialize. Defaults to the current working directory.",
        ),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Initialize a Kitaru project in the current directory."""
    command = "init"
    output_format = _resolve_output_format(output)

    target = Path(path).resolve() if path else Path.cwd().resolve()

    if not target.is_dir():
        _exit_with_error(
            command,
            f"Target path is not a directory: {target}",
            output=output_format,
        )

    for marker_name in (
        KITARU_REPOSITORY_DIRECTORY_NAME,
        _LEGACY_REPOSITORY_DIRECTORY_NAME,
    ):
        candidate = target / marker_name
        if candidate.exists():
            _exit_with_error(
                command,
                f"Already initialized: {candidate} exists.",
                output=output_format,
            )

    from zenml.client import Client

    from kitaru.analytics import AnalyticsEvent, track

    run_with_cli_error_boundary(
        lambda: Client.initialize(root=target),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(Exception,),
    )

    track(AnalyticsEvent.PROJECT_INITIALIZED, {"used_cwd": path is None})

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            {
                "path": str(target),
                "repository_directory": KITARU_REPOSITORY_DIRECTORY_NAME,
            },
            output=output_format,
        )
        return

    _print_success(
        f"Initialized Kitaru project in {target}",
        detail=f"Created {target / KITARU_REPOSITORY_DIRECTORY_NAME}",
    )
