"""Tests for the kitaru CLI."""

from importlib.metadata import version as get_version

import pytest

from kitaru.cli import app


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """--version prints the package version and exits."""
    with pytest.raises(SystemExit) as exc_info:
        app(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    expected_version = get_version("kitaru")
    assert expected_version in captured.out


def test_short_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """-V also prints the version."""
    with pytest.raises(SystemExit) as exc_info:
        app(["-V"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    expected_version = get_version("kitaru")
    assert expected_version in captured.out


def test_help_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """--help prints help text and exits."""
    with pytest.raises(SystemExit) as exc_info:
        app(["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "kitaru" in captured.out.lower()


def test_no_args_shows_help(capsys: pytest.CaptureFixture[str]) -> None:
    """Invoking with no arguments shows help output."""
    with pytest.raises(SystemExit) as exc_info:
        app([])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "kitaru" in captured.out.lower()
