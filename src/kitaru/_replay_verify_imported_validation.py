"""Validation for imported-input replay verification cases."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from kitaru._replay_verify_imported_models import (
    IMPORTED_ELIGIBLE,
    IMPORTED_INELIGIBLE,
    IMPORTED_NON_COMPARABLE,
    IMPORTED_PARTIAL,
    IMPORTED_UNSAFE_INELIGIBLE,
    IMPORTED_VERDICT_HOLD,
    IMPORTED_VERDICT_SHIP,
    SAFETY_STATUS_MOCKED,
    SAFETY_STATUS_SAFE,
    SAFETY_STATUS_UNSAFE_INELIGIBLE,
    FidelityReport,
    ImportedCaseValidation,
    ImportedEligibilityState,
    ImportedReplayCase,
    ImportedVerdictState,
    RecordedCall,
    RetrievalContext,
    SafetyAssessment,
    TenantContext,
)

EXPECTED_CURRENT_CORPUS_INDEX_VERSION = "support-kb-2026-06-06-a"
DEFAULT_ALLOWED_TOOL_NAMES = {
    "get_account_profile",
    "lookup_subscription",
    "lookup_invoice",
    "lookup_order_status",
    "retrieve_policy",
    "search_help_center",
    "search_knowledge_base",
    "send_email",
    "create_support_ticket",
    "update_plan",
    "refund_credit",
    "rotate_api_key",
    "issue_service_credit",
}
WRITE_LIKE_TOOL_NAME_HINTS = (
    "send",
    "create",
    "update",
    "delete",
    "charge",
    "refund",
    "post",
    "email",
    "publish",
    "enqueue",
    "webhook",
)
SAFE_SIDE_EFFECT_STATUSES = {"safe"}
MOCKED_OR_BLOCKED_SIDE_EFFECT_STATUSES = {"mocked", "blocked", "unsafe_ineligible"}
PERMISSION_FAILURE_REASONS = {
    "permission_mismatch_cross_tenant_document",
    "permission_scope_mismatch",
}
PARTIAL_REASONS = {
    "redacted_llm_input_output",
    "out_of_order_observations_sorted_before_import",
    "partial_langfuse_ingestion_after_polling_window",
}


def validate_imported_case(
    case: ImportedReplayCase,
    *,
    expected_runner_entrypoint: str | None = None,
    expected_corpus_index_version: str = EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
    allowed_tool_names: set[str] | None = None,
) -> ImportedCaseValidation:
    """Validate one imported case and decide whether a candidate may run."""
    allowed_tools = _allowed_tool_names(allowed_tool_names)
    is_rag = case_is_rag(case)
    recorded_tools = observed_tool_names(case.recorded_calls)
    declared_tools = declared_tool_names(case)
    availability_errors = _availability_errors(case.trace_contract.available_tools)
    registry_errors = _registry_errors(
        case.trace_contract.available_tools,
        allowed_tool_names=allowed_tools,
    )
    available_tools = case.trace_contract.available_tools
    available_tool_set = set(available_tools) if available_tools is not None else None
    tool_mismatches = (
        [tool for tool in declared_tools if tool not in available_tool_set]
        if available_tool_set is not None
        else []
    )
    missing_recorded_tool_calls = [
        tool for tool in declared_tools if tool not in recorded_tools
    ]
    retrieval_reasons = validate_retrieval_context(
        case.retrieval_context,
        tenant_context=case.tenant_context,
        expected_corpus_index_version=expected_corpus_index_version,
        case_is_rag=is_rag,
    )
    safety = assess_case_safety(
        case,
        allowed_tool_names=allowed_tools,
        extra_unsafe_reasons=[
            reason
            for reason in retrieval_reasons
            if reason in PERMISSION_FAILURE_REASONS
        ],
    )
    runner_expected = expected_runner_entrypoint
    local_runner_present = _runner_available(case, expected_entrypoint=runner_expected)
    recovered = {
        "trace_id": bool(case.source_ref.source_id),
        "input": case.root_input is not None,
        "observed_output_or_signal": case.observed_output is not None,
        "available_tools": case.trace_contract.available_tools is not None,
        "tool_calls": bool(recorded_tools),
        "retrieval": _retrieval_recovered(
            case.retrieval_context,
            retrieval_reasons=retrieval_reasons,
            case_is_rag=is_rag,
        ),
        "model": bool(case.trace_contract.model),
        "prompt_or_config": bool(
            case.trace_contract.prompt_version and case.trace_contract.prompt_hash
        ),
        "usage_cost_latency": any(
            call.usage is not None or call.cost is not None or call.latency is not None
            for call in case.recorded_calls
        ),
        "side_effect_status_controlled": safety.status
        in {SAFETY_STATUS_SAFE, SAFETY_STATUS_MOCKED},
        "local_runner": local_runner_present,
    }
    reasons: list[str] = []
    reasons.extend(availability_errors)
    reasons.extend(registry_errors)
    reasons.extend(
        f"observed_write_like_tool_not_available:{tool}"
        if is_write_like_tool_name(tool)
        else f"observed_tool_not_available:{tool}"
        for tool in tool_mismatches
    )
    reasons.extend(
        f"missing_recorded_write_like_tool_call:{tool}"
        if is_write_like_tool_name(tool)
        else f"missing_recorded_tool_call:{tool}"
        for tool in missing_recorded_tool_calls
    )
    reasons.extend(retrieval_reasons)
    if not case.source_ref.source_id:
        reasons.append("missing_trace_id")
    if case.root_input is None:
        reasons.append("missing_root_input")
    if case.observed_output is None:
        reasons.append("missing_observed_output_or_evaluator_signal")
    if _has_redacted_io(case):
        reasons.append("redacted_llm_input_output")
    if safety.status == SAFETY_STATUS_UNSAFE_INELIGIBLE:
        reasons.extend(safety.reasons)
    reasons.extend(_source_import_reasons(case))
    if not local_runner_present:
        reasons.append("missing_local_runner")
    reasons = dedupe(reasons)

    eligibility, level, verdict = _classify(
        reasons=reasons,
        recovered=recovered,
        safety=safety,
        has_root_input=case.root_input is not None,
        has_observed_output=case.observed_output is not None,
    )
    if eligibility == IMPORTED_ELIGIBLE:
        reasons = dedupe([*reasons, "required_fields_recovered"])
    score = _fidelity_score(recovered, case_is_rag=is_rag)
    missing = [name for name, present in recovered.items() if not present]
    fidelity = FidelityReport(
        recovered_fields=recovered,
        missing_fields=missing,
        eligibility=eligibility,
        score=score,
        level=level,
        reasons=reasons,
        verdict=verdict,
    )
    candidate_allowed = case_can_execute_candidate(fidelity, safety)
    return ImportedCaseValidation(
        case_id=case.case_id,
        fidelity=fidelity,
        safety=safety,
        candidate_execution_allowed=candidate_allowed,
        stop_reasons=[] if candidate_allowed else reasons,
    )


def assess_case_safety(
    case: ImportedReplayCase,
    *,
    allowed_tool_names: set[str] | None = None,
    extra_unsafe_reasons: Sequence[str] = (),
) -> SafetyAssessment:
    """Classify side-effect safety for observed tool and retrieval calls."""
    allowed_tools = _allowed_tool_names(allowed_tool_names)
    unsafe_tools: list[str] = []
    ambiguous_tools: list[str] = []
    mocked_or_blocked_tools: list[str] = []
    safe_tools: list[str] = []
    observed_tools: list[str] = []
    live_execution_seen = False
    reasons: list[str] = []

    for call in case.recorded_calls:
        if call.kind not in {"tool", "retrieval"}:
            continue
        observed_tools.append(call.name)
        output = call.output_payload if isinstance(call.output_payload, Mapping) else {}
        side_effect_status_raw = output.get("side_effect_status") or call.metadata.get(
            "side_effect_status"
        )
        side_effect_status = str(side_effect_status_raw or "")
        executed_live_raw = output.get("executed_live")
        executed_live = bool(executed_live_raw)
        live_execution_seen = live_execution_seen or executed_live
        write_like = is_write_like_tool_name(call.name)
        if (call.name not in allowed_tools and write_like) or executed_live:
            unsafe_tools.append(call.name)
        elif (
            (write_like and not side_effect_status_raw)
            or (write_like and executed_live_raw is not False)
            or side_effect_status == "ambiguous"
        ):
            ambiguous_tools.append(call.name)
        elif side_effect_status in MOCKED_OR_BLOCKED_SIDE_EFFECT_STATUSES:
            mocked_or_blocked_tools.append(call.name)
        elif side_effect_status in SAFE_SIDE_EFFECT_STATUSES or (
            not write_like and not side_effect_status
        ):
            safe_tools.append(call.name)
        else:
            ambiguous_tools.append(call.name)

    if case.trace_contract.side_effect_policy == SAFETY_STATUS_UNSAFE_INELIGIBLE:
        unsafe_tools.extend(
            tool for tool in observed_tools if is_write_like_tool_name(tool)
        )
    unsafe_tools = sorted(set(unsafe_tools))
    ambiguous_tools = sorted(set(ambiguous_tools))
    mocked_or_blocked_tools = sorted(set(mocked_or_blocked_tools))
    safe_tools = sorted(set(safe_tools))
    if unsafe_tools:
        reasons.append("unsafe_or_unknown_write_like_tool_blocked")
    if ambiguous_tools:
        reasons.append("ambiguous_side_effect_status_write_like_tool")
    reasons.extend(extra_unsafe_reasons)
    status = SAFETY_STATUS_SAFE
    if unsafe_tools or ambiguous_tools or extra_unsafe_reasons:
        status = SAFETY_STATUS_UNSAFE_INELIGIBLE
    elif mocked_or_blocked_tools:
        status = SAFETY_STATUS_MOCKED
    return SafetyAssessment(
        status=status,
        safe_tools=safe_tools,
        mocked_or_blocked_tools=mocked_or_blocked_tools,
        unsafe_tools=sorted(set([*unsafe_tools, *ambiguous_tools])),
        observed_tool_names=observed_tools,
        live_execution_blocked=not live_execution_seen,
        reasons=dedupe(reasons),
    )


def validate_retrieval_context(
    retrieval: RetrievalContext | None,
    *,
    tenant_context: TenantContext | None,
    expected_corpus_index_version: str = EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
    case_is_rag: bool,
) -> list[str]:
    """Return explicit RAG downgrade reasons for a case."""
    if not case_is_rag:
        return []
    reasons: list[str] = []
    required_values = {
        "corpus_index_version": retrieval.corpus_index_version if retrieval else None,
        "retriever_name": retrieval.retriever_name if retrieval else None,
        "returned_document_ids": retrieval.returned_document_ids if retrieval else [],
        "returned_chunk_ids": retrieval.returned_chunk_ids if retrieval else [],
        "permission_scope": retrieval.permission_scope if retrieval else None,
    }
    for field_name, value in required_values.items():
        if not value:
            reasons.append(f"missing_rag_metadata:{field_name}")
    if retrieval is None:
        return reasons
    if retrieval.corpus_index_version != expected_corpus_index_version:
        reasons.append("stale_corpus_index_version")
    if tenant_context is not None:
        if retrieval.tenant_id and retrieval.tenant_id != tenant_context.tenant_id:
            reasons.append("permission_mismatch_cross_tenant_document")
        if (
            retrieval.permission_scope
            and retrieval.permission_scope != tenant_context.permission_scope
        ):
            reasons.append("permission_scope_mismatch")
    return dedupe(reasons)


def case_can_execute_candidate(
    fidelity: FidelityReport,
    safety: SafetyAssessment,
) -> bool:
    """Return whether a local runner may call the candidate implementation."""
    return (
        fidelity.eligibility == IMPORTED_ELIGIBLE
        and safety.status != SAFETY_STATUS_UNSAFE_INELIGIBLE
    )


def validate_imported_cases(
    cases: Sequence[ImportedReplayCase],
    *,
    expected_runner_entrypoint: str | None = None,
    expected_corpus_index_version: str = EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
    allowed_tool_names: set[str] | None = None,
) -> list[ImportedCaseValidation]:
    """Validate multiple imported cases."""
    return [
        validate_imported_case(
            case,
            expected_runner_entrypoint=expected_runner_entrypoint,
            expected_corpus_index_version=expected_corpus_index_version,
            allowed_tool_names=allowed_tool_names,
        )
        for case in cases
    ]


def summarize_validations(
    validations: Sequence[ImportedCaseValidation],
) -> dict[str, Any]:
    """Build a compact summary for imported verification reports."""
    eligibility_counts = Counter(item.fidelity.eligibility for item in validations)
    safety_counts = Counter(item.safety.status for item in validations)
    reason_counts = Counter(
        reason for item in validations for reason in item.fidelity.reasons
    )
    stopped = [item for item in validations if not item.candidate_execution_allowed]
    return {
        "case_count": len(validations),
        "eligibility_counts": dict(eligibility_counts),
        "safety_status_counts": dict(safety_counts),
        "candidate_allowed_count": len(validations) - len(stopped),
        "stopped_count": len(stopped),
        "top_reasons": dict(reason_counts.most_common()),
        "stopped_case_ids": [item.case_id for item in stopped],
    }


def observed_tool_names(calls: Sequence[RecordedCall]) -> list[str]:
    """Return tool/retrieval call names in observed order."""
    return [call.name for call in calls if call.kind in {"tool", "retrieval"}]


def declared_tool_names(case: ImportedReplayCase) -> list[str]:
    """Return tool names claimed by recorded calls, output, or trace metadata."""
    names: list[str] = []
    names.extend(observed_tool_names(case.recorded_calls))
    if isinstance(case.observed_output, Mapping):
        output_tool_names = case.observed_output.get("tool_names")
        if isinstance(output_tool_names, list):
            names.extend(str(tool_name) for tool_name in output_tool_names)
    names.extend(case.trace_contract.application_tool_names)
    return dedupe(names)


def is_write_like_tool_name(tool_name: str) -> bool:
    """Return whether the name looks like a side-effecting tool."""
    lowered = tool_name.lower()
    return any(hint in lowered for hint in WRITE_LIKE_TOOL_NAME_HINTS)


def dedupe(items: Sequence[str]) -> list[str]:
    """Keep first occurrence of each string."""
    return list(dict.fromkeys(items))


def _allowed_tool_names(allowed_tool_names: set[str] | None) -> set[str]:
    if allowed_tool_names is None:
        return DEFAULT_ALLOWED_TOOL_NAMES
    return allowed_tool_names


def _source_import_reasons(case: ImportedReplayCase) -> list[str]:
    reasons = case.raw_source_payload.get("source_import_reasons")
    if not isinstance(reasons, list):
        return []
    return [str(reason) for reason in reasons]


def _availability_errors(available_tools: list[str] | None) -> list[str]:
    if available_tools is None:
        return ["missing_available_tools"]
    return []


def _registry_errors(
    available_tools: list[str] | None,
    *,
    allowed_tool_names: set[str],
) -> list[str]:
    if available_tools is None:
        return []
    errors: list[str] = []
    for tool_name in available_tools:
        if tool_name not in allowed_tool_names:
            errors.append(f"unknown_tool:{tool_name}")
            if is_write_like_tool_name(tool_name):
                errors.append("unsafe_or_unknown_write_like_tool_blocked")
    return errors


def _runner_available(
    case: ImportedReplayCase,
    *,
    expected_entrypoint: str | None,
) -> bool:
    if case.runner_contract is None or not case.runner_contract.entrypoint:
        return False
    if expected_entrypoint is None:
        return True
    return case.runner_contract.entrypoint == expected_entrypoint


def _retrieval_recovered(
    retrieval: RetrievalContext | None,
    *,
    retrieval_reasons: Sequence[str],
    case_is_rag: bool,
) -> bool:
    if not case_is_rag:
        return False
    return retrieval is not None and not any(
        reason.startswith("missing_rag_metadata") for reason in retrieval_reasons
    )


def case_is_rag(case: ImportedReplayCase) -> bool:
    if case.retrieval_context is not None:
        return True
    if any(call.kind == "retrieval" for call in case.recorded_calls):
        return True
    tool_names = set(case.trace_contract.available_tools or [])
    tool_names.update(case.trace_contract.application_tool_names)
    if isinstance(case.observed_output, Mapping):
        output_tool_names = case.observed_output.get("tool_names")
        if isinstance(output_tool_names, list):
            tool_names.update(str(tool_name) for tool_name in output_tool_names)
        retrieval_document_ids = case.observed_output.get("retrieval_document_ids")
        if isinstance(retrieval_document_ids, list) and retrieval_document_ids:
            return True
    return any(name.startswith(("retrieve", "search_")) for name in tool_names)


def _has_redacted_io(case: ImportedReplayCase) -> bool:
    if case.root_input == {"user_message": "[REDACTED]"}:
        return True
    if case.observed_output == "[REDACTED]":
        return True
    if isinstance(case.observed_output, Mapping):
        return case.observed_output.get("response") == "[REDACTED]"
    return False


def _classify(
    *,
    reasons: Sequence[str],
    recovered: Mapping[str, bool],
    safety: SafetyAssessment,
    has_root_input: bool,
    has_observed_output: bool,
) -> tuple[
    ImportedEligibilityState,
    Literal["high", "medium", "low"],
    ImportedVerdictState,
]:
    if (
        safety.status == SAFETY_STATUS_UNSAFE_INELIGIBLE
        or any(reason in PERMISSION_FAILURE_REASONS for reason in reasons)
        or any(
            reason.startswith("missing_recorded_write_like_tool_call:")
            for reason in reasons
        )
        or any(
            reason.startswith("observed_write_like_tool_not_available:")
            for reason in reasons
        )
    ):
        return IMPORTED_UNSAFE_INELIGIBLE, "low", IMPORTED_VERDICT_HOLD
    if not has_root_input or "missing_trace_id" in reasons:
        return IMPORTED_INELIGIBLE, "low", IMPORTED_VERDICT_HOLD
    if any(reason in PARTIAL_REASONS for reason in reasons) or any(
        reason.startswith("duplicate_observations_deduplicated:") for reason in reasons
    ):
        return IMPORTED_PARTIAL, "medium", IMPORTED_VERDICT_HOLD
    if (
        not has_observed_output
        or not recovered["available_tools"]
        or not recovered["local_runner"]
        or any(reason.startswith("unknown_tool:") for reason in reasons)
        or any(reason.startswith("observed_tool_not_available:") for reason in reasons)
        or any(reason.startswith("missing_recorded_tool_call:") for reason in reasons)
        or any(reason.startswith("missing_rag_metadata:") for reason in reasons)
        or "stale_corpus_index_version" in reasons
    ):
        return IMPORTED_NON_COMPARABLE, "medium", IMPORTED_VERDICT_HOLD
    return IMPORTED_ELIGIBLE, "high", IMPORTED_VERDICT_SHIP


def _fidelity_score(recovered: Mapping[str, bool], *, case_is_rag: bool) -> float:
    score = 0.0
    score += 0.18 if recovered["trace_id"] else 0.0
    score += 0.18 if recovered["input"] else 0.0
    score += 0.18 if recovered["observed_output_or_signal"] else 0.0
    score += 0.14 if recovered["available_tools"] else 0.0
    score += 0.12 if recovered["side_effect_status_controlled"] else 0.0
    score += 0.10 if recovered["prompt_or_config"] else 0.0
    score += 0.10 if recovered["local_runner"] else 0.0
    if case_is_rag:
        score += 0.10 if recovered["retrieval"] else 0.0
    return round(min(score, 1.0), 3)
