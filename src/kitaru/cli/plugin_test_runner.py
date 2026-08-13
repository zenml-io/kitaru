#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Internal child-process runner for local plugin validation."""

import argparse
import contextlib
import inspect
import io
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from kitaru.task.plugins import load_plugin_entrypoint

_CAPTURE_LIMIT = 32 * 1024


class _BoundedText(io.TextIOBase):
    """Text sink retaining only a bounded tail."""

    def __init__(self, limit: int = _CAPTURE_LIMIT) -> None:
        """Initialize the bounded sink."""
        self._limit = limit
        self._value = ""

    def write(self, value: str) -> int:
        """Append text and discard the oldest excess characters."""
        self._value = (self._value + value)[-self._limit :]
        return len(value)

    def get_value(self) -> str:
        """Return retained text."""
        return self._value


def main(argv: Sequence[str] | None = None) -> int:
    """Load a plugin, validate its signature, and optionally call an importer."""
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=("importer", "evaluator"))
    parser.add_argument("path", type=Path)
    parser.add_argument("entrypoint")
    parser.add_argument("--payload", type=Path)
    parser.add_argument("--params", default="{}")
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args(argv)

    plugin_stdout = _BoundedText()
    plugin_stderr = _BoundedText()
    result: dict[str, Any] = {"loaded": False, "invoked": False}
    with (
        contextlib.redirect_stdout(plugin_stdout),
        contextlib.redirect_stderr(plugin_stderr),
    ):
        callable_ = load_plugin_entrypoint(
            args.path, args.entrypoint, args.kind.title()
        )
        signature = inspect.signature(callable_)
        if args.kind == "importer":
            signature.bind(b"", {})
        else:
            signature.bind(object())
        result["loaded"] = True
        if args.kind == "importer" and args.payload is not None:
            from kitaru.api_models.v1.imports import ImportFailure
            from kitaru.task.importer import ImportedSession, call_parser

            params = json.loads(args.params)
            sessions = 0
            failures = 0
            items = 0
            for item in call_parser(callable_, args.payload.read_bytes(), params):
                sessions += isinstance(item, ImportedSession)
                failures += isinstance(item, ImportFailure)
                items += 1
            result.update(
                invoked=True,
                sessions=sessions,
                failures=failures,
                items=items,
            )
    result["stdout"] = plugin_stdout.get_value()
    result["stderr"] = plugin_stderr.get_value()
    args.result.write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
