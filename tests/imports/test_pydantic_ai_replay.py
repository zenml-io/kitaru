"""Tests for the versioned PydanticAI replay-evidence bundle."""

import json
from dataclasses import replace
from pathlib import Path

import kitaru.imports._pydantic_ai_replay as replay_module
from kitaru.imports import (
    ReplayDiagnosticCode,
    ReplayPartKind,
    ReplayReadinessStatus,
    build_pydantic_ai_replay_evidence,
    build_raw_imported_evidence,
    read_langfuse_jsonl_records,
)
from kitaru.imports._normalization import normalize_langfuse_records

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_replay_evidence.jsonl"


def _evidence(trace_id: str):
    records = [
        record
        for record in read_langfuse_jsonl_records(FIXTURE)
        if record.row["traceId"] == trace_id
    ]
    normalized = normalize_langfuse_records(
        records,
        project_id="source-project",
    )[0]
    raw = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=normalized.records,
    )
    return build_pydantic_ai_replay_evidence(
        normalized.trace,
        raw_evidence=raw,
    )


def test_complete_explicit_tool_evidence_is_preserved_without_claiming_ready() -> None:
    evidence = _evidence("trace-alias")
    parts = [
        part
        for observation in evidence.bundle.observations
        for part in observation.parts
    ]
    kinds = [part.kind for part in parts]

    assert [part.message_index for part in parts] == [0, 1, 1, 2, 3]
    assert kinds == [
        ReplayPartKind.USER_PROMPT,
        ReplayPartKind.MODEL_TEXT,
        ReplayPartKind.TOOL_CALL,
        ReplayPartKind.TOOL_RESULT,
        ReplayPartKind.MODEL_TEXT,
    ]
    assert (
        evidence.readiness.root_input_candidate_rerun.status
        is ReplayReadinessStatus.READY
    )
    assert (
        evidence.readiness.model_message_reconstruction.status
        is ReplayReadinessStatus.UNKNOWN
    )
    assert (
        evidence.readiness.tool_result_boundary_reconstruction.status
        is ReplayReadinessStatus.UNKNOWN
    )
    assert (
        evidence.readiness.recorded_response_matching.status
        is ReplayReadinessStatus.UNKNOWN
    )
    assert (
        evidence.readiness.candidate_tool_compatibility.status
        is ReplayReadinessStatus.UNKNOWN
    )


def test_identical_evidence_produces_identical_bundle_hashes() -> None:
    first = _evidence("trace-alias")
    second = _evidence("trace-alias")

    assert first == second
    assert first.bundle.bundle_digest == second.bundle.bundle_digest
    assert first.bundle.raw_evidence_digest == second.bundle.raw_evidence_digest


def test_omitted_root_input_is_unsupported() -> None:
    evidence = _evidence("trace-unstamped")

    assert evidence.bundle.root_input_present is False
    assert (
        evidence.readiness.root_input_candidate_rerun.status
        is ReplayReadinessStatus.UNSUPPORTED
    )
    assert ReplayDiagnosticCode.ROOT_INPUT_MISSING in {
        diagnostic.code for diagnostic in evidence.bundle.diagnostics
    }


def test_redacted_incomplete_tool_evidence_reports_typed_diagnostics() -> None:
    evidence = _evidence("trace-redacted-tool")
    codes = {diagnostic.code for diagnostic in evidence.bundle.diagnostics}

    assert ReplayDiagnosticCode.TOOL_ARGUMENTS_REDACTED in codes
    assert ReplayDiagnosticCode.TOOL_CALL_WITHOUT_RESULT in codes
    assert (
        evidence.readiness.tool_result_boundary_reconstruction.status
        is ReplayReadinessStatus.UNSUPPORTED
    )


def test_unsupported_parts_and_duplicate_calls_are_not_coerced() -> None:
    records = list(read_langfuse_jsonl_records(FIXTURE))
    base = next(record for record in records if record.row["traceId"] == "trace-alias")
    row = {
        **base.row,
        "id": "unsupported-root",
        "traceId": "trace-unsupported",
        "input": {
            "messages": [
                {
                    "role": "assistant",
                    "content": [{"type": "image", "url": "not-preserved"}],
                    "tool_calls": [
                        {
                            "id": "duplicate",
                            "function": {"name": "one", "arguments": {}},
                        },
                        {
                            "id": "duplicate",
                            "function": {"name": "two", "arguments": {}},
                        },
                    ],
                }
            ]
        },
    }
    record = replace(
        base,
        raw_text=json.dumps(row, separators=(",", ":")) + "\n",
        row=row,
    )
    normalized = normalize_langfuse_records(
        [record],
        project_id="source-project",
    )[0]
    raw = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=normalized.records,
    )
    evidence = build_pydantic_ai_replay_evidence(
        normalized.trace,
        raw_evidence=raw,
    )
    codes = {diagnostic.code for diagnostic in evidence.bundle.diagnostics}

    assert ReplayDiagnosticCode.UNSUPPORTED_MESSAGE_PART in codes
    assert ReplayDiagnosticCode.DUPLICATE_TOOL_CALL in codes


def test_long_identifiers_and_large_diagnostic_sets_are_bounded() -> None:
    records = list(read_langfuse_jsonl_records(FIXTURE))
    base = next(record for record in records if record.row["traceId"] == "trace-alias")
    long_id = "observation-" + "x" * 10_000
    row = {
        **base.row,
        "id": long_id,
        "traceId": "trace-large-diagnostics",
        "input": {
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": f"unsupported-{index}"} for index in range(300)
                    ],
                }
            ]
        },
    }
    record = replace(
        base,
        raw_text=json.dumps(row, separators=(",", ":")) + "\n",
        row=row,
    )
    normalized = normalize_langfuse_records(
        [record],
        project_id="source-project",
    )[0]
    raw = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=normalized.records,
    )

    first = build_pydantic_ai_replay_evidence(
        normalized.trace,
        raw_evidence=raw,
    )
    second = build_pydantic_ai_replay_evidence(
        normalized.trace,
        raw_evidence=raw,
    )
    diagnostics = first.bundle.diagnostics
    codes = {diagnostic.code for diagnostic in diagnostics}

    assert len(diagnostics) == 32
    assert ReplayDiagnosticCode.IDENTIFIER_TRUNCATED in codes
    assert ReplayDiagnosticCode.DIAGNOSTIC_LIMIT_EXCEEDED in codes
    assert all(
        diagnostic.observation_id is None or len(diagnostic.observation_id) <= 256
        for diagnostic in diagnostics
    )
    assert first.bundle.bundle_digest == second.bundle.bundle_digest


def test_contract_module_does_not_import_pydantic_ai_runtime() -> None:
    imported_modules = {
        value.__name__
        for value in replay_module.__dict__.values()
        if hasattr(value, "__name__") and hasattr(value, "__package__")
    }

    assert not any(
        name == "pydantic_ai" or name.startswith("pydantic_ai.")
        for name in imported_modules
    )
