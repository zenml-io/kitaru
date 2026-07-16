"""Streaming readers for Langfuse observation exports."""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any


class LangfuseImportError(ValueError):
    """Raised when a Langfuse export cannot be imported safely."""


def _reject_json_constant(constant: str) -> Any:
    raise ValueError(f"non-finite JSON constant {constant!r} is not supported")


# json.loads builds a fresh decoder whenever keyword options are passed, so
# share one for the per-line and per-payload hot paths.
_STRICT_DECODER = json.JSONDecoder(parse_constant=_reject_json_constant)


def strict_json_loads(value: str) -> Any:
    """Decode JSON while rejecting the NaN/Infinity extensions.

    Non-finite constants are not valid JSON, and NaN in particular breaks
    equality-based duplicate detection downstream (NaN != NaN), so exports
    containing them are refused at the parsing boundary.
    """
    return _STRICT_DECODER.decode(value)


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
                row = strict_json_loads(line)
            # RecursionError: json.loads overflows the stack on deeply nested
            # payloads, and that is a property of the untrusted line too.
            except (RecursionError, ValueError) as exc:
                message = exc.msg if isinstance(exc, json.JSONDecodeError) else str(exc)
                raise LangfuseImportError(
                    f"Invalid JSON in {source} at line {line_number}: {message}."
                ) from exc
            if not isinstance(row, dict):
                raise LangfuseImportError(
                    f"Expected a JSON object in {source} at line {line_number}."
                )
            yield row
