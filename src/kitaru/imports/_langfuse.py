"""Streaming readers for Langfuse observation exports."""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any


class LangfuseImportError(ValueError):
    """Raised when a Langfuse export cannot be imported safely."""


def read_langfuse_jsonl(path: str | Path) -> Iterator[dict[str, Any]]:
    """Yield observations from a Langfuse JSONL export one line at a time.

    Args:
        path: Path to a Langfuse observations export.

    Yields:
        One decoded observation object per non-empty line.

    Raises:
        LangfuseImportError: If a line is invalid JSON or not an object.
    """
    source = Path(path)
    with source.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise LangfuseImportError(
                    f"Invalid JSON in {source} at line {line_number}: {exc.msg}."
                ) from exc
            if not isinstance(row, dict):
                raise LangfuseImportError(
                    f"Expected a JSON object in {source} at line {line_number}."
                )
            yield row
