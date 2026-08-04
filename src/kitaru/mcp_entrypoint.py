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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Base-safe console boundary for the optional Kitaru MCP server."""

import importlib
import sys
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version
from typing import Protocol, cast

_INSTALL_GUIDANCE = "Install the Kitaru MCP server with pip install 'kitaru[mcp]'"
_HELP = """usage: kitaru-mcp [--help] [--version] [SERVER OPTIONS]

Run the Kitaru MCP server over standard input and output.

options:
  -h, --help                 show this help message and exit
  --version                  show the installed Kitaru version and exit
  --mode MODE                read-only (default), standard, or destructive
  --server URL               fixed server URL for this process
  --context NAME             fixed persisted context for this process
  --timeout SECONDS          HTTP timeout (default: 30)
  --handler-timeout SECONDS  total handler timeout (default: 120)
  --retries COUNT            HTTP retry count (default: 3)
  --pool-size COUNT          HTTP connection pool size (default: 20)
  --max-concurrency COUNT    concurrent handler limit (default: 10)
  --debug                    enable redacted stderr diagnostics

Install the optional server dependency with pip install 'kitaru[mcp]'.
"""


class _ServerMain(Protocol):
    """Callable boundary implemented by the optional MCP runtime."""

    def __call__(self, argv: Sequence[str] | None = None) -> int: ...


def main(argv: Sequence[str] | None = None) -> int:
    """Handle base-safe flags, then lazily enter the optional MCP runtime."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if "--help" in arguments or "-h" in arguments:
        print(_HELP, end="")
        return 0
    if "--version" in arguments:
        print(_get_package_version())
        return 0

    try:
        importlib.import_module("mcp.server")
    except ModuleNotFoundError as error:
        if error.name in {"mcp", "mcp.server"}:
            print(_INSTALL_GUIDANCE, file=sys.stderr)
            return 2
        raise

    try:
        server_module = importlib.import_module("kitaru.mcp.server")
    except ModuleNotFoundError as error:
        if error.name == "kitaru.mcp.server":
            print(
                "The Kitaru MCP runtime is not available in this build.",
                file=sys.stderr,
            )
            return 2
        raise
    server_main = cast(_ServerMain, server_module.main)
    return server_main(arguments)


def _get_package_version() -> str:
    """Return the installed version without importing an optional frontend."""
    try:
        return version("kitaru")
    except PackageNotFoundError:
        return "unknown"
