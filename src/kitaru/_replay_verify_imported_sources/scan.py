"""Scan-mode validation for arbitrary (uninstrumented) Langfuse observations.

Scan mode points the importer at observation rows from any Langfuse project,
including applications that never emitted Kitaru's replay trace contract. The
goal is a fidelity checklist ("here is what this trace is missing before it can
be verified"), not errors or support-copilot registry noise. By default no
tool-registry allowlist is assumed and the stale-corpus check is skipped.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from kitaru._replay_verify_imported_models import (
    ImportedCaseValidation,
    ImportedReplayCase,
)
from kitaru._replay_verify_imported_sources.langfuse import (
    cases_from_langfuse_observations,
)
from kitaru._replay_verify_imported_validation import (
    NO_TOOL_REGISTRY_EXPECTATION,
    NoToolRegistryExpectation,
    summarize_validations,
    validate_imported_cases,
)


@dataclass(frozen=True)
class LangfuseScanResult:
    """Cases, validations, and summaries for a Langfuse observation scan."""

    cases: list[ImportedReplayCase]
    validations: list[ImportedCaseValidation]
    summary: dict[str, Any]
    source_import_summary: dict[str, Any]


def scan_langfuse_observations(
    observation_rows: Iterable[Mapping[str, Any]],
    *,
    base_url: str | None = None,
    source_ref: str | None = None,
    allowed_tool_names: set[str]
    | NoToolRegistryExpectation
    | None = NO_TOOL_REGISTRY_EXPECTATION,
    expected_corpus_index_version: str | None = None,
    expected_runner_entrypoint: str | None = None,
) -> LangfuseScanResult:
    """Import and validate arbitrary Langfuse observation rows.

    Mirrors ``validate_imported_cases_jsonl`` but with scan-friendly defaults:
    no tool-registry expectation and no expected corpus index version, so
    traces from uninstrumented applications produce a fidelity report instead
    of ``unknown_tool`` / ``stale_corpus_index_version`` noise. Pass an explicit
    ``allowed_tool_names`` set (or ``None`` for the demo registry default) to
    restore registry checking.
    """
    cases = cases_from_langfuse_observations(
        observation_rows,
        base_url=base_url,
        source_ref=source_ref,
    )
    validations = validate_imported_cases(
        cases,
        expected_runner_entrypoint=expected_runner_entrypoint,
        expected_corpus_index_version=expected_corpus_index_version,
        allowed_tool_names=allowed_tool_names,
    )
    summary = summarize_validations(validations)
    source_import_summary = _source_import_summary(cases)
    summary["ignored_observation_count"] = source_import_summary[
        "ignored_observation_count"
    ]
    return LangfuseScanResult(
        cases=cases,
        validations=validations,
        summary=summary,
        source_import_summary=source_import_summary,
    )


def _source_import_summary(cases: list[ImportedReplayCase]) -> dict[str, Any]:
    reason_counts = Counter(
        reason for case in cases for reason in _source_import_reasons(case)
    )
    source_counts = Counter(case.source_ref.source_system for case in cases)
    return {
        "source_system_counts": dict(source_counts),
        "source_import_reason_counts": dict(reason_counts),
        "ignored_observation_count": sum(
            _ignored_observation_count(case) for case in cases
        ),
    }


def _source_import_reasons(case: ImportedReplayCase) -> list[str]:
    reasons = case.raw_source_payload.get("source_import_reasons")
    if not isinstance(reasons, list):
        return []
    return [str(reason) for reason in reasons]


def _ignored_observation_count(case: ImportedReplayCase) -> int:
    import_summary = case.raw_source_payload.get("source_import_summary")
    if not isinstance(import_summary, Mapping):
        return 0
    value = import_summary.get("ignored_observation_count")
    return value if isinstance(value, int) else 0
