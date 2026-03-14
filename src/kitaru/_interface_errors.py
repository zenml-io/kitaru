"""Shared interface-layer error translation helpers for CLI and MCP."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import NoReturn, Protocol, TypeVar

from kitaru.cli_output import CLIOutputFormat

T = TypeVar("T")


class CLIErrorEmitter(Protocol):
    """Callable signature for CLI error emitters."""

    def __call__(
        self,
        command: str,
        message: str,
        *,
        output: CLIOutputFormat,
        error_type: str | None = None,
    ) -> NoReturn: ...


@dataclass(frozen=True)
class InterfaceErrorDetails:
    """Normalized user-facing error details for interface boundaries."""

    message: str
    error_type: str | None


def translate_to_user_error(exc: BaseException) -> InterfaceErrorDetails:
    """Translate one exception into user-facing error details."""
    return InterfaceErrorDetails(
        message=str(exc),
        error_type=type(exc).__name__,
    )


def run_with_cli_error_boundary(
    operation: Callable[[], T],
    *,
    command: str,
    output: CLIOutputFormat,
    exit_with_error: CLIErrorEmitter,
    handled_exceptions: tuple[type[Exception], ...] = (Exception,),
    translator: Callable[[Exception], InterfaceErrorDetails] = translate_to_user_error,
) -> T:
    """Run one CLI operation and emit a consistent error on handled failure."""
    try:
        return operation()
    except handled_exceptions as exc:
        details = translator(exc)
        exit_with_error(
            command,
            details.message,
            output=output,
            error_type=details.error_type,
        )


def run_with_mcp_error_boundary(
    operation: Callable[[], T],
    *,
    handled_exceptions: tuple[type[Exception], ...] = (Exception,),
    translator: Callable[[Exception], BaseException] | None = None,
) -> T:
    """Run one MCP operation and preserve passthrough exception behavior."""
    try:
        return operation()
    except handled_exceptions as exc:
        if translator is None:
            raise
        raise translator(exc) from exc
