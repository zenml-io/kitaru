"""Streaming readers for Langfuse observation exports."""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class LangfuseImportError(ValueError):
    """Raised when a Langfuse export cannot be imported safely."""


@dataclass(frozen=True)
class LangfuseSourceRecord:
    """One parsed Langfuse row with its exact source text and position."""

    raw_text: str
    row: dict[str, Any]
    line_number: int
    source_order: int


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


def read_langfuse_jsonl_records(
    path: str | Path,
) -> Iterator[LangfuseSourceRecord]:
    """Yield Langfuse rows with exact source text and physical positions.

    Args:
        path: Path to a Langfuse observations export.

    Yields:
        One source record per non-empty line, in file order.

    Raises:
        LangfuseImportError: If a line is invalid JSON or not an object.
    """
    source = Path(path)
    source_order = 0
    with source.open(encoding="utf-8", newline="") as file:
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
            yield LangfuseSourceRecord(
                raw_text=line,
                row=row,
                line_number=line_number,
                source_order=source_order,
            )
            source_order += 1


def read_langfuse_jsonl(path: str | Path) -> Iterator[dict[str, Any]]:
    """Yield decoded observations from a Langfuse JSONL export.

    This compatibility reader intentionally yields dictionaries. Use
    :func:`read_langfuse_jsonl_records` when exact source evidence is needed.
    """
    for record in read_langfuse_jsonl_records(path):
        yield record.row
