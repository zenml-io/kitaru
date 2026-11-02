#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Reading of files named by CLI options."""

from pathlib import Path

from kitaru.cli.output import CLIError


def read_utf8_file(path: Path, option: str) -> str:
    """Read a regular UTF-8 file named by a CLI option, rejecting stdin.

    Args:
        path: File named on the command line.
        option: Option name used in error messages, such as ``--sessions-file``.

    Raises:
        CLIError: The path is ``-``, is not a regular file, is not UTF-8, or
            cannot be read.

    Returns:
        The file content.
    """
    if str(path) == "-":
        raise CLIError("invalid_arguments", f"{option} does not accept stdin ('-').")
    if not path.is_file():
        raise CLIError(
            "invalid_arguments",
            f"{option} must be an existing regular UTF-8 text file.",
        )
    # Drop the OS error chain because it carries the file path into tracebacks.
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        raise CLIError(
            "invalid_arguments", f"{option} must contain valid UTF-8 text."
        ) from None
    except OSError as error:
        reason = error.strerror or type(error).__name__
        raise CLIError(
            "invalid_arguments", f"{option} could not be read: {reason}."
        ) from None
