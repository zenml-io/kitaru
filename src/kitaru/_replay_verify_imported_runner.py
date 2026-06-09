"""Local runner contract for imported-input Replay Verify cases.

The runner starts from neutral imported records. It validates source evidence first,
then runs fresh local baseline and candidate callables only for cases that passed
fidelity and safety checks. This is not deterministic Kitaru checkpoint replay.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from kitaru._replay_verify_imported_models import (
    DEFAULT_COMPARISON_FIELDS,
    IMPORTED_INPUT_EXECUTION_MODE,
    IMPORTED_VERDICT_CAUTION,
    IMPORTED_VERDICT_HOLD,
    IMPORTED_VERDICT_SHIP,
    ImportedCaseValidation,
    ImportedReplayCase,
    ImportedVerdictState,
    ImportedVerificationReport,
    RunnerContract,
    execution_mode_detail,
    to_plain_data,
)
from kitaru._replay_verify_imported_validation import (
    EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
    NoToolRegistryExpectation,
    case_is_rag,
    dedupe,
    validate_imported_case,
)

RunnerRole = Literal["baseline", "candidate"]
CaseRunStatus = Literal["stopped", "completed", "failed"]

RECORDED_RESPONSE_CONTROL_STATUS = "unavailable"


@dataclass(frozen=True)
class ImportedRunnerInvocation:
    """Per-call context passed to baseline and candidate implementations."""

    case_id: str
    role: RunnerRole
    runner_id: str
    root_input: Any
    available_tools: tuple[str, ...]
    config: Mapping[str, Any] = field(default_factory=dict)
    comparison_fields: tuple[str, ...] = DEFAULT_COMPARISON_FIELDS
    execution_mode: str = IMPORTED_INPUT_EXECUTION_MODE


@dataclass(frozen=True)
class ImportedRunnerOutput:
    """Normalized output returned by a local baseline or candidate runner."""

    payload: Any
    metadata: Mapping[str, Any] = field(default_factory=dict)
    unsafe_live_execution_count: int = 0


RunnerReturn = Mapping[str, Any] | ImportedRunnerOutput
ImportedRunnerCallable = Callable[
    [ImportedReplayCase, ImportedRunnerInvocation], RunnerReturn
]


@dataclass(frozen=True)
class FieldComparison:
    """Comparison for one structured field."""

    field: str
    baseline_value: Any
    comparison_value: Any
    matches: bool


@dataclass(frozen=True)
class ImportedCaseRunResult:
    """Execution and comparison result for one imported case."""

    case_id: str
    status: CaseRunStatus
    verdict: ImportedVerdictState
    validation: ImportedCaseValidation
    candidate_executed: bool
    cohort: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    trace_contract_version: str | None = None
    baseline_output: Any = None
    candidate_output: Any = None
    observed_vs_baseline: list[FieldComparison] = field(default_factory=list)
    candidate_vs_baseline: list[FieldComparison] = field(default_factory=list)
    unsafe_live_execution_count: int = 0
    stop_reasons: list[str] = field(default_factory=list)
    error: str | None = None


def verify_imported_cases(
    cases: Sequence[ImportedReplayCase],
    *,
    baseline_runner: ImportedRunnerCallable,
    candidate_runner: ImportedRunnerCallable,
    baseline_config: Mapping[str, Any] | None = None,
    candidate_config: Mapping[str, Any] | None = None,
    report_name: str = "Imported Replay Verification",
    execution_mode: str = IMPORTED_INPUT_EXECUTION_MODE,
    expected_runner_entrypoint: str | None = None,
    expected_corpus_index_version: str = EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
    allowed_tool_names: set[str] | NoToolRegistryExpectation | None = None,
    comparison_fields: Sequence[str] | None = None,
    created_at: str | None = None,
) -> ImportedVerificationReport:
    """Validate and run imported cases through local baseline/candidate callables.

    Candidate execution is fail-closed: a candidate callable is invoked only after
    validation has marked the case eligible and the baseline callable has returned
    successfully.
    """
    if isinstance(allowed_tool_names, NoToolRegistryExpectation):
        raise ValueError(
            "Scan-mode NO_TOOL_REGISTRY_EXPECTATION is not allowed for candidate "
            "execution: without a tool-registry allowlist there is no safety "
            "check on which tools the candidate may run. Pass an explicit "
            "allowed_tool_names set (or None for the default registry)."
        )
    fields = tuple(comparison_fields or _comparison_fields_from_cases(cases))
    baseline_settings = dict(baseline_config or {})
    candidate_settings = dict(candidate_config or {})
    results: list[ImportedCaseRunResult] = []
    validations: list[ImportedCaseValidation] = []

    for case in cases:
        validation = validate_imported_case(
            case,
            expected_runner_entrypoint=expected_runner_entrypoint,
            expected_corpus_index_version=expected_corpus_index_version,
            allowed_tool_names=allowed_tool_names,
        )
        validations.append(validation)
        if not validation.candidate_execution_allowed:
            results.append(_stopped_result(case, validation))
            continue

        baseline_result = _call_runner(
            baseline_runner,
            case,
            role="baseline",
            runner_id=_runner_id(case, fallback="baseline"),
            config=baseline_settings,
            comparison_fields=fields,
            execution_mode=execution_mode,
        )
        if isinstance(baseline_result, _RunnerFailure):
            results.append(
                _baseline_failed_result(
                    case,
                    validation,
                    baseline_result,
                )
            )
            continue
        if baseline_result.unsafe_live_execution_count:
            results.append(
                _baseline_unsafe_result(
                    case,
                    validation,
                    baseline_result,
                )
            )
            continue

        candidate_result = _call_runner(
            candidate_runner,
            case,
            role="candidate",
            runner_id=_runner_id(case, fallback="candidate"),
            config=candidate_settings,
            comparison_fields=fields,
            execution_mode=execution_mode,
        )
        if isinstance(candidate_result, _RunnerFailure):
            results.append(
                _candidate_failed_result(
                    case,
                    validation,
                    baseline_result,
                    candidate_result,
                    fields=fields,
                )
            )
            continue

        observed_vs_baseline = compare_structured_fields(
            case.observed_output,
            baseline_result.payload,
            fields,
        )
        candidate_vs_baseline = compare_structured_fields(
            baseline_result.payload,
            candidate_result.payload,
            fields,
        )
        unsafe_live_count = (
            baseline_result.unsafe_live_execution_count
            + candidate_result.unsafe_live_execution_count
        )
        results.append(
            ImportedCaseRunResult(
                case_id=case.case_id,
                status="completed",
                verdict=_completed_verdict(
                    observed_vs_baseline=observed_vs_baseline,
                    candidate_vs_baseline=candidate_vs_baseline,
                    unsafe_live_execution_count=unsafe_live_count,
                ),
                validation=validation,
                candidate_executed=True,
                cohort=case.cohort,
                labels=dict(case.labels),
                trace_contract_version=case.trace_contract.trace_contract_version,
                baseline_output=baseline_result.payload,
                candidate_output=candidate_result.payload,
                observed_vs_baseline=observed_vs_baseline,
                candidate_vs_baseline=candidate_vs_baseline,
                unsafe_live_execution_count=unsafe_live_count,
            )
        )

    summary = summarize_run_results(
        cases=cases,
        validations=validations,
        results=results,
        execution_mode=execution_mode,
        comparison_fields=fields,
    )
    return ImportedVerificationReport(
        name=report_name,
        created_at=created_at or datetime.now(UTC).isoformat(),
        source_system=_source_system(cases),
        execution_mode=execution_mode,
        cases=validations,
        summary=summary,
        runner_contract=_report_runner_contract(
            cases,
            execution_mode=execution_mode,
            comparison_fields=fields,
        ),
    )


def compare_structured_fields(
    baseline: Any,
    comparison: Any,
    fields: Sequence[str] = DEFAULT_COMPARISON_FIELDS,
) -> list[FieldComparison]:
    """Compare named structured fields between two output payloads."""
    comparisons: list[FieldComparison] = []
    for field_name in fields:
        baseline_value = _field_value(baseline, field_name)
        comparison_value = _field_value(comparison, field_name)
        comparisons.append(
            FieldComparison(
                field=field_name,
                baseline_value=baseline_value,
                comparison_value=comparison_value,
                matches=baseline_value == comparison_value,
            )
        )
    return comparisons


def summarize_run_results(
    *,
    cases: Sequence[ImportedReplayCase],
    validations: Sequence[ImportedCaseValidation],
    results: Sequence[ImportedCaseRunResult],
    execution_mode: str,
    comparison_fields: Sequence[str],
) -> dict[str, Any]:
    """Build product-shaped counts for imported-input verification reports."""
    eligibility_counts = Counter(item.fidelity.eligibility for item in validations)
    safety_counts = Counter(item.safety.status for item in validations)
    stopped = [result for result in results if not result.candidate_executed]
    validation_stopped = [
        result
        for result in results
        if not result.validation.candidate_execution_allowed
    ]
    completed = [result for result in results if result.status == "completed"]
    observed_mismatch_case_ids = [
        result.case_id
        for result in completed
        if _has_mismatch(result.observed_vs_baseline)
    ]
    candidate_drift_case_ids = [
        result.case_id
        for result in completed
        if _has_mismatch(result.candidate_vs_baseline)
    ]
    unsafe_live_execution_count = sum(
        result.unsafe_live_execution_count for result in results
    )
    stopped_case_reasons = {result.case_id: result.stop_reasons for result in stopped}
    failed_case_reasons = {
        result.case_id: result.stop_reasons
        for result in results
        if result.status == "failed"
    }
    verdict_counts = Counter(result.verdict for result in results)
    rag_coverage = _rag_metadata_coverage(cases, validations)
    per_case = [to_plain_data(result) for result in results]
    cohorts = sorted({case.cohort for case in cases if case.cohort is not None})
    trace_contract_versions = sorted(
        {
            case.trace_contract.trace_contract_version
            for case in cases
            if case.trace_contract.trace_contract_version is not None
        }
    )
    return {
        "source_system": _source_system(cases),
        "execution_mode": execution_mode,
        "mode_detail": execution_mode_detail(execution_mode),
        "recorded_response_control_status": RECORDED_RESPONSE_CONTROL_STATUS,
        "recorded_response_control_unavailable": True,
        "comparison_fields": list(comparison_fields),
        "imported_count": len(cases),
        "case_count": len(cases),
        "eligible_count": eligibility_counts.get("eligible", 0),
        "partial_count": eligibility_counts.get("partial", 0),
        "ineligible_count": eligibility_counts.get("ineligible", 0),
        "non_comparable_count": eligibility_counts.get("non_comparable", 0),
        "unsafe_count": eligibility_counts.get("unsafe_ineligible", 0),
        "stopped_count": len(stopped),
        "candidate_execution_count": sum(
            1 for result in results if result.candidate_executed
        ),
        "candidate_executions_for_stopped_cases": sum(
            1 for result in validation_stopped if result.candidate_executed
        ),
        "unsafe_live_execution_count": unsafe_live_execution_count,
        "observed_vs_baseline_mismatch_count": len(observed_mismatch_case_ids),
        "observed_vs_baseline_mismatch_case_ids": observed_mismatch_case_ids,
        "candidate_vs_baseline_drift_count": len(candidate_drift_case_ids),
        "candidate_vs_baseline_drift_case_ids": candidate_drift_case_ids,
        "eligibility_counts": dict(eligibility_counts),
        "safety_status_counts": dict(safety_counts),
        "verdict_counts": dict(verdict_counts),
        "overall_verdict": _overall_verdict(results),
        "cohorts": cohorts,
        "trace_contract_versions": trace_contract_versions,
        "rag_metadata_coverage": rag_coverage,
        "stopped_case_reasons": stopped_case_reasons,
        "failed_case_reasons": failed_case_reasons,
        "case_results": per_case,
    }


@dataclass(frozen=True)
class _RunnerFailure:
    message: str


def _call_runner(
    runner: ImportedRunnerCallable,
    case: ImportedReplayCase,
    *,
    role: RunnerRole,
    runner_id: str,
    config: Mapping[str, Any],
    comparison_fields: tuple[str, ...],
    execution_mode: str,
) -> ImportedRunnerOutput | _RunnerFailure:
    invocation = ImportedRunnerInvocation(
        case_id=case.case_id,
        role=role,
        runner_id=runner_id,
        root_input=case.root_input,
        available_tools=tuple(case.trace_contract.available_tools or ()),
        config=config,
        comparison_fields=comparison_fields,
        execution_mode=execution_mode,
    )
    try:
        return _normalize_runner_output(runner(case, invocation))
    except Exception as exc:  # pragma: no cover - exact exception type is app-owned.
        return _RunnerFailure(message=f"{type(exc).__name__}: {exc}")


def _normalize_runner_output(value: RunnerReturn) -> ImportedRunnerOutput:
    if isinstance(value, ImportedRunnerOutput):
        # Do not trust the runner-reported count: a runner under test could
        # under-report live side effects, so rescan its payload and keep the
        # larger of the two counts.
        scanned_count = _unsafe_live_execution_count(value.payload)
        unsafe_live_count = max(value.unsafe_live_execution_count, scanned_count)
        if unsafe_live_count == value.unsafe_live_execution_count:
            return value
        return ImportedRunnerOutput(
            payload=value.payload,
            metadata=value.metadata,
            unsafe_live_execution_count=unsafe_live_count,
        )
    payload = dict(value)
    metadata = payload.get("metadata", {})
    return ImportedRunnerOutput(
        payload=payload,
        metadata=dict(metadata) if isinstance(metadata, Mapping) else {},
        unsafe_live_execution_count=_unsafe_live_execution_count(payload),
    )


def _unsafe_live_execution_count(payload: Any) -> int:
    if not isinstance(payload, Mapping):
        return 0
    explicit_count = payload.get("unsafe_live_execution_count")
    if isinstance(explicit_count, int):
        return explicit_count
    if payload.get("unsafe_live_execution") is True:
        return 1
    tool_results = payload.get("tool_results", [])
    if not isinstance(tool_results, list):
        return 0
    return sum(
        1
        for item in tool_results
        if isinstance(item, Mapping) and item.get("executed_live") is True
    )


def _stopped_result(
    case: ImportedReplayCase,
    validation: ImportedCaseValidation,
) -> ImportedCaseRunResult:
    return ImportedCaseRunResult(
        case_id=case.case_id,
        status="stopped",
        verdict=IMPORTED_VERDICT_HOLD,
        validation=validation,
        candidate_executed=False,
        cohort=case.cohort,
        labels=dict(case.labels),
        trace_contract_version=case.trace_contract.trace_contract_version,
        stop_reasons=list(validation.stop_reasons),
    )


def _baseline_failed_result(
    case: ImportedReplayCase,
    validation: ImportedCaseValidation,
    failure: _RunnerFailure,
) -> ImportedCaseRunResult:
    reason = "baseline_runner_failed"
    return ImportedCaseRunResult(
        case_id=case.case_id,
        status="failed",
        verdict=IMPORTED_VERDICT_HOLD,
        validation=validation,
        candidate_executed=False,
        cohort=case.cohort,
        labels=dict(case.labels),
        trace_contract_version=case.trace_contract.trace_contract_version,
        stop_reasons=dedupe([*validation.stop_reasons, reason]),
        error=failure.message,
    )


def _baseline_unsafe_result(
    case: ImportedReplayCase,
    validation: ImportedCaseValidation,
    baseline: ImportedRunnerOutput,
) -> ImportedCaseRunResult:
    return ImportedCaseRunResult(
        case_id=case.case_id,
        status="stopped",
        verdict=IMPORTED_VERDICT_HOLD,
        validation=validation,
        candidate_executed=False,
        cohort=case.cohort,
        labels=dict(case.labels),
        trace_contract_version=case.trace_contract.trace_contract_version,
        baseline_output=baseline.payload,
        unsafe_live_execution_count=baseline.unsafe_live_execution_count,
        stop_reasons=["baseline_unsafe_live_execution_detected"],
    )


def _candidate_failed_result(
    case: ImportedReplayCase,
    validation: ImportedCaseValidation,
    baseline: ImportedRunnerOutput,
    failure: _RunnerFailure,
    *,
    fields: Sequence[str],
) -> ImportedCaseRunResult:
    return ImportedCaseRunResult(
        case_id=case.case_id,
        status="failed",
        verdict=IMPORTED_VERDICT_HOLD,
        validation=validation,
        candidate_executed=True,
        cohort=case.cohort,
        labels=dict(case.labels),
        trace_contract_version=case.trace_contract.trace_contract_version,
        baseline_output=baseline.payload,
        observed_vs_baseline=compare_structured_fields(
            case.observed_output,
            baseline.payload,
            fields,
        ),
        unsafe_live_execution_count=baseline.unsafe_live_execution_count,
        stop_reasons=["candidate_runner_failed"],
        error=failure.message,
    )


def _completed_verdict(
    *,
    observed_vs_baseline: Sequence[FieldComparison],
    candidate_vs_baseline: Sequence[FieldComparison],
    unsafe_live_execution_count: int,
) -> ImportedVerdictState:
    if unsafe_live_execution_count:
        return IMPORTED_VERDICT_HOLD
    if _has_mismatch(candidate_vs_baseline):
        return IMPORTED_VERDICT_HOLD
    if _has_mismatch(observed_vs_baseline):
        return IMPORTED_VERDICT_CAUTION
    return IMPORTED_VERDICT_SHIP


def _overall_verdict(
    results: Sequence[ImportedCaseRunResult],
) -> ImportedVerdictState:
    # An empty cohort proves nothing, so fail closed instead of shipping.
    if not results:
        return IMPORTED_VERDICT_HOLD
    verdicts = {result.verdict for result in results}
    if IMPORTED_VERDICT_HOLD in verdicts:
        return IMPORTED_VERDICT_HOLD
    if IMPORTED_VERDICT_CAUTION in verdicts:
        return IMPORTED_VERDICT_CAUTION
    return IMPORTED_VERDICT_SHIP


def _has_mismatch(comparisons: Sequence[FieldComparison]) -> bool:
    return any(not item.matches for item in comparisons)


def _field_value(payload: Any, field_name: str) -> Any:
    if isinstance(payload, Mapping):
        return to_plain_data(payload.get(field_name))
    return None


def _comparison_fields_from_cases(
    cases: Sequence[ImportedReplayCase],
) -> tuple[str, ...]:
    for case in cases:
        if case.runner_contract is not None:
            return case.runner_contract.comparison_fields
    return DEFAULT_COMPARISON_FIELDS


def _runner_id(case: ImportedReplayCase, *, fallback: str) -> str:
    if case.runner_contract is None:
        return fallback
    if fallback == "baseline" and case.runner_contract.baseline_id:
        return case.runner_contract.baseline_id
    if fallback == "candidate" and case.runner_contract.candidate_id:
        return case.runner_contract.candidate_id
    return case.runner_contract.entrypoint or fallback


def _source_system(cases: Sequence[ImportedReplayCase]) -> str:
    systems = sorted({case.source_ref.source_system for case in cases})
    if not systems:
        return "unknown"
    if len(systems) == 1:
        return systems[0]
    return "mixed"


def _report_runner_contract(
    cases: Sequence[ImportedReplayCase],
    *,
    execution_mode: str,
    comparison_fields: tuple[str, ...],
) -> RunnerContract | None:
    for case in cases:
        if case.runner_contract is not None:
            return RunnerContract(
                entrypoint=case.runner_contract.entrypoint,
                execution_mode=execution_mode,
                baseline_id=case.runner_contract.baseline_id,
                candidate_id=case.runner_contract.candidate_id,
                comparison_fields=comparison_fields,
            )
    return None


def _rag_metadata_coverage(
    cases: Sequence[ImportedReplayCase],
    validations: Sequence[ImportedCaseValidation],
) -> dict[str, Any]:
    rag_case_ids = [case.case_id for case in cases if case_is_rag(case)]
    validation_by_case = {validation.case_id: validation for validation in validations}
    complete_case_ids = [
        case_id
        for case_id in rag_case_ids
        if validation_by_case[case_id].fidelity.recovered_fields.get("retrieval")
    ]
    incomplete_case_ids = [
        case_id for case_id in rag_case_ids if case_id not in complete_case_ids
    ]
    return {
        "available": bool(rag_case_ids),
        "rag_case_count": len(rag_case_ids),
        "complete_count": len(complete_case_ids),
        "missing_or_incomplete_count": len(incomplete_case_ids),
        "complete_case_ids": complete_case_ids,
        "missing_or_incomplete_case_ids": incomplete_case_ids,
    }
