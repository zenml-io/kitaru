"""Regression tests for shared interface error boundaries."""

from __future__ import annotations

from typing import NoReturn

import pytest

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat


def _unexpected_exit_with_error(*args: object, **kwargs: object) -> NoReturn:
    """Fail the test if the CLI error emitter is called unexpectedly."""
    raise AssertionError("The CLI error emitter should not have been called.")


def test_run_with_cli_error_boundary_does_not_catch_keyboard_interrupt() -> None:
    """KeyboardInterrupt should bypass the generic CLI boundary helper."""

    def _raise_keyboard_interrupt() -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_with_cli_error_boundary(
            _raise_keyboard_interrupt,
            command="executions.logs",
            output=CLIOutputFormat.TEXT,
            exit_with_error=_unexpected_exit_with_error,
        )


def test_run_with_cli_error_boundary_does_not_catch_system_exit() -> None:
    """SystemExit should bypass the generic CLI boundary helper."""

    def _raise_system_exit() -> None:
        raise SystemExit(2)

    with pytest.raises(SystemExit) as exc_info:
        run_with_cli_error_boundary(
            _raise_system_exit,
            command="executions.logs",
            output=CLIOutputFormat.TEXT,
            exit_with_error=_unexpected_exit_with_error,
        )

    assert exc_info.value.code == 2
