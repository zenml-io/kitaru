"""Shell-free command and protected source checks for experiment exports."""

import re
import shlex
from pathlib import PurePosixPath

from ._sanitize import EphemeralSanitizer
from .models import ExportError, SourceInventory
from .source import source_file_bytes

_SHELL_SYNTAX = re.compile(r"[;&|<>`$\n\r]")
_COMMAND_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_PYTHON_NAME = re.compile(r"^python(?:3(?:\.\d+)?)?$")
_SHELLS = frozenset(
    {
        "ash",
        "bash",
        "csh",
        "dash",
        "fish",
        "ksh",
        "powershell",
        "pwsh",
        "sh",
        "tcsh",
        "zsh",
    }
)
_UV_FLAGS_WITH_VALUE = frozenset({"--directory", "--package", "--project"})
_UV_FLAGS = frozenset({"--frozen", "--locked", "--no-project", "--no-sync"})
_UV_PYTHON_FLAGS = frozenset({"-p", "--python"})
_UV_PYTHON_OVERRIDE_MESSAGE = (
    "Export v1 does not allow uv run to select a Python runtime."
)


def _validate_program(argv: tuple[str, ...]) -> None:
    program = PurePosixPath(argv[0]).name
    if program in _SHELLS:
        raise ExportError(
            "unsupported_run_command",
            "Export v1 does not support shell interpreter commands.",
        )
    if _PYTHON_NAME.fullmatch(program):
        if len(argv) < 2 or argv[1] in {"-c", "-"}:
            raise ExportError(
                "unsupported_run_command",
                "Python commands must select a script or module, not inline code.",
            )
        if argv[1] == "-m" and len(argv) < 3:
            raise ExportError(
                "unsupported_run_command",
                "Python -m commands must select a module.",
            )
        return
    if "/" in argv[0] or _COMMAND_NAME.fullmatch(argv[0]) is None:
        raise ExportError(
            "unsupported_run_command",
            "Export v1 supports Python, uv run, or an installed console entrypoint.",
        )


def _validate_uv(argv: tuple[str, ...]) -> None:
    if len(argv) < 3 or argv[1] != "run":
        raise ExportError(
            "unsupported_run_command",
            "uv commands must use the shell-free uv run form.",
        )
    index = 2
    while index < len(argv) and argv[index].startswith("-"):
        flag = argv[index]
        if (
            flag in _UV_PYTHON_FLAGS
            or flag.startswith("--python=")
            or (flag.startswith("-p") and not flag.startswith("--"))
        ):
            raise ExportError("unsupported_run_command", _UV_PYTHON_OVERRIDE_MESSAGE)
        if flag in _UV_FLAGS:
            index += 1
            continue
        if flag in _UV_FLAGS_WITH_VALUE and index + 1 < len(argv):
            index += 2
            continue
        raise ExportError(
            "unsupported_run_command",
            "The registered uv run command uses an unsupported option.",
        )
    if index >= len(argv):
        raise ExportError(
            "unsupported_run_command",
            "uv run must select a Python command or installed console entrypoint.",
        )
    _validate_program(argv[index:])


def parse_command_argv(
    command: str,
    *,
    sanitizer: EphemeralSanitizer,
) -> tuple[str, ...]:
    """Parse one registered command into validated shell-free argv."""
    sanitizer.reject_text(
        command,
        code="protected_value_in_command",
        message=(
            "Protected runtime material appears in the registered command; "
            "export cannot rewrite executable material safely."
        ),
    )
    if not command.strip() or _SHELL_SYNTAX.search(command):
        raise ExportError(
            "unsupported_run_command",
            "Export v1 supports shell-free Python, uv run, or console commands only.",
        )
    try:
        parsed = tuple(shlex.split(command, posix=True))
    except ValueError as error:
        raise ExportError(
            "unsupported_run_command",
            "The registered command is not valid shell-free argv.",
        ) from error
    if not parsed or any("\x00" in argument for argument in parsed):
        raise ExportError(
            "unsupported_run_command",
            "The registered command is not valid shell-free argv.",
        )
    program = PurePosixPath(parsed[0]).name
    if program == "uv":
        _validate_uv(parsed)
    else:
        _validate_program(parsed)
    return parsed


def reject_protected_source(
    source: SourceInventory,
    *,
    sanitizer: EphemeralSanitizer,
) -> None:
    """Fail when protected values occur in source paths or executable bytes."""
    sanitizer.reject_text(
        str(source.root),
        code="protected_value_in_path",
        message="Protected runtime material appears in a source path.",
    )
    for file in source.files:
        sanitizer.reject_text(
            file.path,
            code="protected_value_in_path",
            message="Protected runtime material appears in a source path.",
        )
        content = source_file_bytes(source, file.path)
        sanitizer.reject_bytes(
            content,
            code="protected_value_in_source",
            message=(
                "Protected runtime material appears in agent source; export cannot "
                "rewrite executable material safely."
            ),
        )
