"""JSONL source helpers for imported-input Replay Verify cases."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kitaru._replay_verify_imported_models import (
    ImportedCaseValidation,
    ImportedReplayCase,
    imported_case_from_mapping,
    to_plain_data,
)
from kitaru._replay_verify_imported_validation import (
    summarize_validations,
    validate_imported_cases,
)


@dataclass(frozen=True)
class ImportedCaseJsonlLoadResult:
    """Cases loaded from JSONL plus validation and summary output."""

    cases: list[ImportedReplayCase]
    validations: list[ImportedCaseValidation]
    summary: dict[str, Any]


def read_imported_cases_jsonl(path: str | Path) -> list[ImportedReplayCase]:
    """Read neutral/imported-case JSONL into ``ImportedReplayCase`` records."""
    jsonl_path = Path(path)
    cases: list[ImportedReplayCase] = []
    with jsonl_path.open(encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                msg = f"Invalid JSON on line {line_number} of {jsonl_path}: {exc.msg}"
                raise ValueError(msg) from exc
            if not isinstance(row, dict):
                msg = f"Expected JSON object on line {line_number} of {jsonl_path}"
                raise ValueError(msg)
            cases.append(imported_case_from_mapping(row))
    return cases


def validate_imported_cases_jsonl(
    path: str | Path,
    *,
    expected_runner_entrypoint: str | None = None,
    expected_corpus_index_version: str | None = None,
    allowed_tool_names: set[str] | None = None,
) -> ImportedCaseJsonlLoadResult:
    """Read, validate, and summarize imported-case JSONL."""
    cases = read_imported_cases_jsonl(path)
    validation_kwargs: dict[str, Any] = {
        "expected_runner_entrypoint": expected_runner_entrypoint,
        "allowed_tool_names": allowed_tool_names,
    }
    if expected_corpus_index_version is not None:
        validation_kwargs["expected_corpus_index_version"] = (
            expected_corpus_index_version
        )
    validations = validate_imported_cases(cases, **validation_kwargs)
    summary = summarize_validations(validations)
    summary.update(_source_summary(cases))
    return ImportedCaseJsonlLoadResult(
        cases=cases,
        validations=validations,
        summary=summary,
    )


def write_imported_cases_jsonl(
    cases: list[ImportedReplayCase],
    path: str | Path,
) -> None:
    """Write imported cases as JSONL using the neutral schema field names."""
    jsonl_path = Path(path)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("w", encoding="utf-8") as file:
        for case in cases:
            file.write(json.dumps(to_plain_data(case), default=str, sort_keys=True))
            file.write("\n")


def _source_summary(cases: list[ImportedReplayCase]) -> dict[str, Any]:
    reason_counts = Counter(
        reason
        for case in cases
        for reason in _source_import_reasons(case.raw_source_payload)
    )
    source_counts = Counter(case.source_ref.source_system for case in cases)
    return {
        "source_system_counts": dict(source_counts),
        "source_import_reason_counts": dict(reason_counts),
    }


def _source_import_reasons(raw_source_payload: dict[str, Any]) -> list[str]:
    reasons = raw_source_payload.get("source_import_reasons")
    if not isinstance(reasons, list):
        return []
    return [str(reason) for reason in reasons]
