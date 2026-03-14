"""Model-alias CLI commands."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from kitaru.cli_output import CLIOutputFormat
from kitaru.config import ModelAliasEntry
from kitaru.inspection import serialize_model_alias

from . import model_app
from ._helpers import (
    OutputFormatOption,
    _emit_json_item,
    _emit_json_items,
    _emit_snapshot,
    _exit_with_error,
    _facade_module,
    _print_success,
    _resolve_output_format,
)


def _model_rows(entries: list[ModelAliasEntry]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru model list`."""
    if not entries:
        return [("Models", "none found")]

    rows: list[tuple[str, str]] = []
    for entry in entries:
        detail = entry.model
        if entry.secret:
            detail += f" (secret={entry.secret})"
        if entry.is_default:
            detail += " [default]"
        rows.append((entry.alias, detail))

    return rows


@model_app.command
def register(
    alias: Annotated[
        str,
        Parameter(help="Local alias name (for example `fast`)."),
    ],
    *,
    model: Annotated[
        str,
        Parameter(
            help="Concrete LiteLLM model identifier (for example openai/gpt-4o-mini)."
        ),
    ],
    secret: Annotated[
        str | None,
        Parameter(help="Optional secret name/ID containing provider credentials."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Register or update a local model alias used by `kitaru.llm()`."""
    command = "model.register"
    output_format = _resolve_output_format(output)
    facade = _facade_module()
    try:
        if secret is not None:
            facade._resolve_secret_exact(facade.Client(), secret)
        alias_entry = facade.register_model_alias(alias, model=model, secret=secret)
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_model_alias(alias_entry),
            output=output_format,
        )
        return

    detail = f"Model: {alias_entry.model}"
    if alias_entry.secret:
        detail += f" | Secret: {alias_entry.secret}"
    if alias_entry.is_default:
        detail += " | Default alias"

    _print_success(
        f"Saved model alias: {alias_entry.alias}",
        detail=detail,
    )


@model_app.command
def list___(output: OutputFormatOption = "text") -> None:
    """List local model aliases used by `kitaru.llm()`."""
    command = "model.list"
    output_format = _resolve_output_format(output)
    try:
        aliases = _facade_module().list_model_aliases()
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_model_alias(entry) for entry in aliases],
            output=output_format,
        )
        return

    _emit_snapshot("Kitaru models", _model_rows(aliases))
