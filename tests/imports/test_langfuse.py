"""Tests for reading Langfuse observation exports."""

import json
from pathlib import Path

import pytest

from kitaru.imports import LangfuseImportError, read_langfuse_jsonl

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_observations.jsonl"


def test_reader_streams_jsonl_rows() -> None:
    rows = list(read_langfuse_jsonl(FIXTURE))

    assert len(rows) == 9
    assert rows[0]["traceId"] == "trace-complete"


def test_reader_reports_malformed_line_number(tmp_path: Path) -> None:
    path = tmp_path / "malformed.jsonl"
    path.write_text('{}\n{"broken"\n', encoding="utf-8")

    with pytest.raises(LangfuseImportError, match="line 2"):
        list(read_langfuse_jsonl(path))


def test_reader_rejects_non_object_rows(tmp_path: Path) -> None:
    path = tmp_path / "array.jsonl"
    path.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")

    with pytest.raises(LangfuseImportError, match="JSON object"):
        list(read_langfuse_jsonl(path))
