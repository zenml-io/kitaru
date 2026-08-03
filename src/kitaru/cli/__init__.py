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
"""Lazy entry point for the optional Kitaru command-line interface."""

import sys
from collections.abc import Sequence

_EXTRA_MODULES = {"cyclopts", "rich", "yaml", "packaging"}
_EXTRA_HINT = "Install the Kitaru CLI with `pip install 'kitaru[cli]'`."


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI without importing optional dependencies eagerly.

    Args:
        argv: Arguments to parse. Defaults to ``sys.argv[1:]``.

    Returns:
        Process exit code.
    """
    try:
        from kitaru.cli.app import main as run
    except ModuleNotFoundError as error:
        if error.name not in _EXTRA_MODULES:
            raise
        print(_EXTRA_HINT, file=sys.stderr)
        return 2
    return run(argv)
