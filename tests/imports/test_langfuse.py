"""Tests for reading Langfuse observation exports."""

import json
from pathlib import Path

import pytest

from kitaru.imports import (
    LangfuseImportError,
    read_langfuse_jsonl,
    read_langfuse_jsonl_records,
)

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_observations.jsonl"


def test_reader_streams_jsonl_rows() -> None:
    rows = list(read_langfuse_jsonl(FIXTURE))

    assert len(rows) == 9
    assert rows[0]["traceId"] == "trace-complete"


def test_record_reader_preserves_exact_text_line_and_source_order() -> None:
    records = list(read_langfuse_jsonl_records(FIXTURE))

    assert records[0].line_number == 1
    assert records[0].source_order == 0
    assert records[0].raw_text.endswith("\n")
    assert records[0].row["traceId"] == "trace-complete"


def test_record_reader_preserves_crlf_and_unterminated_final_line(
    tmp_path: Path,
) -> None:
    path = tmp_path / "line-endings.jsonl"
    path.write_bytes(b'{"id":"first"}\r\n{"id":"last"}')

    records = list(read_langfuse_jsonl_records(path))

    assert [record.raw_text for record in records] == [
        '{"id":"first"}\r\n',
        '{"id":"last"}',
    ]


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


def test_reader_reports_deeply_nested_line_as_import_error(tmp_path: Path) -> None:
    depth = 200_000
    path = tmp_path / "nested.jsonl"
    path.write_text(
        '{"payload": ' + "[" * depth + "]" * depth + "}\n", encoding="utf-8"
    )

    # json.loads raises RecursionError (not JSONDecodeError) on this input;
    # the reader must still fail with a clean per-line import error.
    with pytest.raises(LangfuseImportError, match="line 1"):
        list(read_langfuse_jsonl(path))


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_reader_rejects_non_finite_json_constants(
    tmp_path: Path, constant: str
) -> None:
    path = tmp_path / "nonfinite.jsonl"
    path.write_text(f'{{"latencyMs": {constant}}}\n', encoding="utf-8")

    with pytest.raises(LangfuseImportError, match="non-finite JSON constant"):
        list(read_langfuse_jsonl(path))
