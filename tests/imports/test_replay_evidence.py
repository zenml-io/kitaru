"""Tests for raw evidence, provider stamps, and attribution contracts."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from kitaru.imports import (
    EvidenceRedactionStatus,
    ProviderVersionStampKind,
    ReplayDiagnosticCode,
    SourceAttributionStatus,
    build_raw_imported_evidence,
    classify_source_attribution,
    extract_langfuse_provider_stamps,
    read_langfuse_jsonl_records,
)
from kitaru.imports._normalization import normalize_langfuse_records

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_replay_evidence.jsonl"
GIT_SHA = "0123456789abcdef0123456789abcdef01234567"


def _records_by_trace():
    records = list(read_langfuse_jsonl_records(FIXTURE))
    grouped = {}
    for record in records:
        grouped.setdefault(record.row["traceId"], []).append(record)
    return grouped


def test_raw_evidence_preserves_exact_rows_and_hashes_deterministically() -> None:
    records = _records_by_trace()["trace-conflict"]
    normalized = normalize_langfuse_records(
        records,
        project_id="source-project",
    )[0]

    first = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=normalized.records,
    )
    second = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=tuple(reversed(normalized.records)),
    )

    assert first == second
    assert first.raw_content_sha256 == second.raw_content_sha256
    assert [row.line_number for row in first.rows] == [4, 5]
    assert first.rows[0].raw_text.endswith("\n")
    assert first.rows[0].parsed_object["id"] == "conflict-root"


def test_raw_evidence_reports_redaction_and_is_frozen() -> None:
    records = _records_by_trace()["trace-redacted-tool"]
    normalized = normalize_langfuse_records(
        records,
        project_id="source-project",
    )[0]
    evidence = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=normalized.records,
    )

    assert evidence.redaction_status is EvidenceRedactionStatus.PARTIALLY_REDACTED
    with pytest.raises(ValidationError, match="frozen"):
        evidence.raw_content_sha256 = "0" * 64  # ty: ignore[invalid-assignment]


def test_provider_stamps_accept_exact_alias_and_full_git_sha() -> None:
    grouped = _records_by_trace()
    alias_stamps = extract_langfuse_provider_stamps(
        [record.row for record in grouped["trace-alias"]]
    )
    sha_stamps = extract_langfuse_provider_stamps(
        [record.row for record in grouped["trace-sha"]]
    )

    assert alias_stamps[0].kind is ProviderVersionStampKind.TRACE_VERSION
    assert alias_stamps[0].value == "prod"
    assert sha_stamps[0].kind is ProviderVersionStampKind.GIT_SHA
    assert (
        classify_source_attribution(
            alias_stamps,
            git_sha=GIT_SHA,
            aliases=("prod",),
        ).status
        is SourceAttributionStatus.SOURCE_VERIFIED
    )
    assert (
        classify_source_attribution(
            sha_stamps,
            git_sha=GIT_SHA,
            aliases=("prod",),
        ).status
        is SourceAttributionStatus.SOURCE_VERIFIED
    )


def test_missing_stamps_are_caller_attributed_without_fuzzy_matching() -> None:
    grouped = _records_by_trace()
    unstamped = extract_langfuse_provider_stamps(
        [record.row for record in grouped["trace-unstamped"]]
    )
    prefixed_sha = extract_langfuse_provider_stamps(
        [
            {
                "traceId": "trace-prefix",
                "traceMetadata": {"git_sha": GIT_SHA[:12]},
            }
        ]
    )

    assert (
        classify_source_attribution(
            unstamped,
            git_sha=GIT_SHA,
            aliases=("prod",),
        ).status
        is SourceAttributionStatus.CALLER_ATTRIBUTED
    )
    conflict = classify_source_attribution(
        prefixed_sha,
        git_sha=GIT_SHA,
        aliases=("prod",),
    )
    assert conflict.status is SourceAttributionStatus.CONFLICT
    assert conflict.diagnostics[0].code is ReplayDiagnosticCode.SOURCE_VERSION_CONFLICT


def test_provider_stamp_metadata_is_bounded_with_typed_diagnostics() -> None:
    long_value = "x" * 10_000
    long_stamps = extract_langfuse_provider_stamps([{"traceVersion": long_value}])
    long_attribution = classify_source_attribution(
        long_stamps,
        git_sha=GIT_SHA,
        aliases=("prod",),
    )

    many_stamps = extract_langfuse_provider_stamps(
        [{"traceVersion": f"version-{index}"} for index in range(100)]
    )
    many_attribution = classify_source_attribution(
        many_stamps,
        git_sha=GIT_SHA,
        aliases=("prod",),
    )

    assert len(long_attribution.stamps[0].value) < 100
    assert long_attribution.stamps[0].truncated is True
    assert ReplayDiagnosticCode.SOURCE_VERSION_STAMP_TRUNCATED in {
        diagnostic.code for diagnostic in long_attribution.diagnostics
    }
    assert len(many_stamps) == 33
    assert len(many_attribution.stamps) == 32
    assert ReplayDiagnosticCode.SOURCE_VERSION_STAMP_LIMIT_EXCEEDED in {
        diagnostic.code for diagnostic in many_attribution.diagnostics
    }


def test_mixed_supported_stamps_conflict() -> None:
    records = _records_by_trace()["trace-conflict"]
    stamps = extract_langfuse_provider_stamps([record.row for record in records])

    attribution = classify_source_attribution(
        stamps,
        git_sha=GIT_SHA,
        aliases=("prod",),
    )

    assert [stamp.value for stamp in attribution.stamps] == ["prod", "other"]
    assert attribution.status is SourceAttributionStatus.CONFLICT
