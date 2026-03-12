"""Shared CLI output helpers for text and JSON modes."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, NoReturn


class CLIOutputFormat(StrEnum):
    """Supported CLI output formats."""

    TEXT = "text"
    JSON = "json"


@dataclass(frozen=True)
class CommandEnvelope:
    """Structured success payload for a CLI command."""

    command: str
    item: dict[str, Any] | None = None
    items: list[dict[str, Any]] | None = None
    count: int | None = None


@dataclass(frozen=True)
class ErrorEnvelope:
    """Structured error payload for a CLI command."""

    command: str
    error: dict[str, Any]


def normalize_output_format(raw: str) -> CLIOutputFormat:
    """Normalize and validate a CLI output format string."""
    normalized = raw.strip().lower()
    if normalized not in {CLIOutputFormat.TEXT.value, CLIOutputFormat.JSON.value}:
        raise ValueError("`--output` must be either `text` or `json`.")
    return CLIOutputFormat(normalized)


def emit_command_envelope(
    envelope: CommandEnvelope,
    *,
    output: CLIOutputFormat,
) -> None:
    """Emit a structured command result in the requested format."""
    if output == CLIOutputFormat.JSON:
        payload = {
            key: value for key, value in asdict(envelope).items() if value is not None
        }
        print(json.dumps(payload))


def emit_json_error(
    command: str,
    message: str,
    *,
    error_type: str | None = None,
) -> None:
    """Emit a structured JSON error envelope to stderr."""
    payload = asdict(
        ErrorEnvelope(
            command=command,
            error={
                "message": message,
                "type": error_type,
            },
        )
    )
    payload["error"] = {
        key: value for key, value in payload["error"].items() if value is not None
    }
    print(json.dumps(payload), file=sys.stderr)


def exit_with_error(
    command: str,
    message: str,
    *,
    output: CLIOutputFormat,
    error_type: str | None = None,
) -> NoReturn:
    """Emit a format-aware error and exit non-zero."""
    if output == CLIOutputFormat.JSON:
        emit_json_error(command, message, error_type=error_type)
    else:
        print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)
