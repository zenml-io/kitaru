"""Tests for imported-input Replay Verify runner contract."""

from __future__ import annotations

from typing import Any

import pytest

from kitaru._replay_verify_imported_models import (
    FIXTURE_HARNESS_EXECUTION_MODE,
    IMPORTED_INPUT_EXECUTION_MODE,
    ImportedCaseSourceRef,
    ImportedReplayCase,
    RecordedCall,
    ReplayTraceContract,
    RetrievalContext,
    RunnerContract,
    TenantContext,
)
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerInvocation,
    ImportedRunnerOutput,
    verify_imported_cases,
)
from kitaru._replay_verify_imported_validation import NO_TOOL_REGISTRY_EXPECTATION

RUNNER_ENTRYPOINT = "run_support_copilot_case"


def _observed_output(
    *,
    policy_label: str = "support_policy",
    risk_status: str = "safe",
    tool_names: list[str] | None = None,
    retrieval_document_ids: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "policy_label": policy_label,
        "risk_status": risk_status,
        "tool_names": tool_names or [],
        "retrieval_document_ids": retrieval_document_ids or [],
    }


def _case(
    case_id: str,
    *,
    root_input: Any = None,
    observed_output: Any = None,
    available_tools: list[str] | None = None,
    recorded_calls: list[RecordedCall] | None = None,
    retrieval_context: RetrievalContext | None = None,
    runner_entrypoint: str | None = RUNNER_ENTRYPOINT,
    raw_source_payload: dict[str, Any] | None = None,
    cohort: str | None = None,
    labels: dict[str, str] | None = None,
) -> ImportedReplayCase:
    return ImportedReplayCase(
        case_id=case_id,
        source_ref=ImportedCaseSourceRef(
            source_system="langfuse",
            source_id=f"trace-{case_id}",
            observation_ids=[f"obs-{case_id}"],
        ),
        root_input=root_input,
        observed_output=observed_output,
        recorded_calls=recorded_calls or [],
        trace_contract=ReplayTraceContract(
            trace_contract_version="trace-contract-v1",
            app_name="support-copilot",
            app_version="2026-06-07",
            model="openai-chat:gpt-5",
            prompt_version="support-copilot-v1",
            prompt_hash="abc123",
            available_tools=available_tools,
            side_effect_policy="safe",
        ),
        runner_contract=RunnerContract(entrypoint=runner_entrypoint)
        if runner_entrypoint is not None
        else None,
        tenant_context=TenantContext(
            tenant_id="tenant-alpha",
            permission_scope="tenant:tenant-alpha:member",
        ),
        retrieval_context=retrieval_context,
        cohort=cohort,
        labels=labels or {},
        raw_source_payload=raw_source_payload or {},
    )


def _unsafe_tool_call(name: str) -> RecordedCall:
    return RecordedCall(
        kind="tool",
        name=name,
        output_payload={
            "tool_name": name,
            "side_effect_status": "safe",
            "executed_live": False,
        },
    )


def _bad_rag_metadata() -> RetrievalContext:
    return RetrievalContext(
        query="Show tenant admin docs",
        retriever_name="support_copilot_kb_retriever",
        corpus_index_version="support-kb-old",
        returned_document_ids=["doc-private"],
        returned_chunk_ids=["chunk-private"],
        tenant_id="tenant-beta",
        permission_scope="tenant:tenant-beta:admin",
    )


def _runner_payload(invocation: ImportedRunnerInvocation) -> dict[str, Any]:
    return {
        **_observed_output(),
        "runner_role": invocation.role,
        "available_tools_seen": list(invocation.available_tools),
        "config_model": invocation.config.get("model"),
    }


def test_runner_validates_cases_and_does_not_call_candidate_for_stopped_cases() -> None:
    calls: list[tuple[str, str]] = []
    candidate_invocations: list[ImportedRunnerInvocation] = []
    cases = [
        _case(
            "eligible",
            root_input={"user_message": "What is the policy?"},
            observed_output=_observed_output(),
            available_tools=["lookup_invoice"],
            cohort="support-emea",
            labels={"team": "support", "priority": "p1"},
        ),
        _case(
            "missing-output",
            root_input={"user_message": "What is the policy?"},
            observed_output=None,
            available_tools=[],
        ),
        _case(
            "missing-runner",
            root_input={"user_message": "What is the policy?"},
            observed_output=_observed_output(),
            available_tools=[],
            runner_entrypoint=None,
        ),
        _case(
            "unsafe-tool",
            root_input={"user_message": "Send a live email."},
            observed_output=_observed_output(tool_names=["send_live_email"]),
            available_tools=["send_live_email"],
            recorded_calls=[_unsafe_tool_call("send_live_email")],
        ),
        _case(
            "bad-rag",
            root_input={"user_message": "Show tenant admin docs."},
            observed_output=_observed_output(
                tool_names=["search_knowledge_base"],
                retrieval_document_ids=["doc-private"],
            ),
            available_tools=["search_knowledge_base"],
            recorded_calls=[
                RecordedCall(kind="retrieval", name="search_knowledge_base")
            ],
            retrieval_context=_bad_rag_metadata(),
        ),
        _case(
            "partial-import",
            root_input={"user_message": "What is the policy?"},
            observed_output=_observed_output(),
            available_tools=[],
            raw_source_payload={
                "source_import_reasons": [
                    "partial_langfuse_ingestion_after_polling_window"
                ]
            },
        ),
    ]

    def baseline(
        case: ImportedReplayCase,
        invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        calls.append((case.case_id, invocation.role))
        return _runner_payload(invocation)

    def candidate(
        case: ImportedReplayCase,
        invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        calls.append((case.case_id, invocation.role))
        candidate_invocations.append(invocation)
        return _runner_payload(invocation)

    report = verify_imported_cases(
        cases,
        baseline_runner=baseline,
        candidate_runner=candidate,
        baseline_config={"model": "baseline-model"},
        candidate_config={"model": "candidate-model"},
        created_at="2026-06-07T12:00:00+00:00",
    )

    assert report.execution_mode == IMPORTED_INPUT_EXECUTION_MODE
    assert calls == [("eligible", "baseline"), ("eligible", "candidate")]
    allowed_case_ids = [
        item.case_id for item in report.cases if item.candidate_execution_allowed
    ]
    assert allowed_case_ids == ["eligible"]
    assert len(candidate_invocations) == 1
    assert candidate_invocations[0].available_tools == ("lookup_invoice",)
    assert candidate_invocations[0].config == {"model": "candidate-model"}
    assert report.summary["imported_count"] == 6
    assert report.summary["candidate_execution_count"] == 1
    assert report.summary["candidate_executions_for_stopped_cases"] == 0
    assert report.summary["stopped_count"] == 5
    assert report.summary["non_comparable_count"] == 2
    assert report.summary["unsafe_count"] == 2
    assert report.summary["partial_count"] == 1
    assert (
        "missing_observed_output_or_evaluator_signal"
        in report.summary["stopped_case_reasons"]["missing-output"]
    )
    assert (
        "missing_local_runner"
        in report.summary["stopped_case_reasons"]["missing-runner"]
    )
    assert (
        "unsafe_or_unknown_write_like_tool_blocked"
        in report.summary["stopped_case_reasons"]["unsafe-tool"]
    )
    assert report.summary["rag_metadata_coverage"] == {
        "available": True,
        "rag_case_count": 1,
        "complete_count": 1,
        "missing_or_incomplete_count": 0,
        "complete_case_ids": ["bad-rag"],
        "missing_or_incomplete_case_ids": [],
    }
    assert report.summary["overall_verdict"] == "hold"
    assert report.summary["failed_case_reasons"] == {}
    assert report.summary["cohorts"] == ["support-emea"]
    assert report.summary["trace_contract_versions"] == ["trace-contract-v1"]
    case_results = {item["case_id"]: item for item in report.summary["case_results"]}
    assert case_results["eligible"]["cohort"] == "support-emea"
    assert case_results["eligible"]["labels"] == {
        "team": "support",
        "priority": "p1",
    }
    assert case_results["eligible"]["trace_contract_version"] == "trace-contract-v1"
    assert case_results["missing-output"]["cohort"] is None
    assert case_results["missing-output"]["labels"] == {}
    assert (
        case_results["missing-output"]["trace_contract_version"] == "trace-contract-v1"
    )


def test_runner_reports_structured_field_drift_and_unsafe_live_execution() -> None:
    case = _case(
        "drift",
        root_input={"user_message": "What is the policy?"},
        observed_output=_observed_output(),
        available_tools=[],
    )

    def baseline(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _observed_output()

    def candidate(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> ImportedRunnerOutput:
        return ImportedRunnerOutput(
            payload=_observed_output(risk_status="needs_review"),
            unsafe_live_execution_count=1,
        )

    report = verify_imported_cases(
        [case],
        baseline_runner=baseline,
        candidate_runner=candidate,
        created_at="2026-06-07T12:00:00+00:00",
    )

    assert report.summary["observed_vs_baseline_mismatch_count"] == 0
    assert report.summary["candidate_vs_baseline_drift_count"] == 1
    assert report.summary["candidate_vs_baseline_drift_case_ids"] == ["drift"]
    assert report.summary["unsafe_live_execution_count"] == 1
    assert report.summary["verdict_counts"] == {"hold": 1}
    assert report.summary["overall_verdict"] == "hold"


def test_baseline_unsafe_live_execution_stops_before_candidate_execution() -> None:
    candidate_calls: list[str] = []
    case = _case(
        "baseline-unsafe",
        root_input={"user_message": "What is the policy?"},
        observed_output=_observed_output(),
        available_tools=[],
    )

    def baseline(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> ImportedRunnerOutput:
        return ImportedRunnerOutput(
            payload=_observed_output(),
            unsafe_live_execution_count=1,
        )

    def candidate(
        case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        candidate_calls.append(case.case_id)
        return _observed_output()

    report = verify_imported_cases(
        [case],
        baseline_runner=baseline,
        candidate_runner=candidate,
        created_at="2026-06-07T12:00:00+00:00",
    )

    assert candidate_calls == []
    assert report.summary["candidate_execution_count"] == 0
    assert report.summary["unsafe_live_execution_count"] == 1
    assert report.summary["stopped_case_reasons"] == {
        "baseline-unsafe": ["baseline_unsafe_live_execution_detected"]
    }
    assert report.summary["verdict_counts"] == {"hold": 1}


def test_baseline_failure_stops_before_candidate_execution() -> None:
    candidate_calls: list[str] = []
    case = _case(
        "baseline-fails",
        root_input={"user_message": "What is the policy?"},
        observed_output=_observed_output(),
        available_tools=[],
    )

    def baseline(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        raise RuntimeError("baseline unavailable")

    def candidate(
        case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        candidate_calls.append(case.case_id)
        return _observed_output()

    report = verify_imported_cases(
        [case],
        baseline_runner=baseline,
        candidate_runner=candidate,
        created_at="2026-06-07T12:00:00+00:00",
    )

    assert candidate_calls == []
    assert report.summary["candidate_execution_count"] == 0
    assert report.summary["stopped_case_reasons"] == {
        "baseline-fails": ["baseline_runner_failed"]
    }
    assert report.summary["failed_case_reasons"] == {
        "baseline-fails": ["baseline_runner_failed"]
    }
    assert report.summary["case_results"][0]["error"] == (
        "RuntimeError: baseline unavailable"
    )


def test_candidate_failure_recorded_in_failed_not_stopped_reasons() -> None:
    case = _case(
        "candidate-fails",
        root_input={"user_message": "What is the policy?"},
        observed_output=_observed_output(),
        available_tools=[],
    )

    def baseline(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _observed_output()

    def candidate(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        raise RuntimeError("candidate exploded")

    report = verify_imported_cases(
        [case],
        baseline_runner=baseline,
        candidate_runner=candidate,
        created_at="2026-06-07T12:00:00+00:00",
    )

    # The candidate did execute, so the case is failed but not stopped.
    assert report.summary["stopped_case_reasons"] == {}
    assert report.summary["failed_case_reasons"] == {
        "candidate-fails": ["candidate_runner_failed"]
    }
    assert report.summary["overall_verdict"] == "hold"


def test_overall_verdict_prefers_hold_then_caution_then_ship() -> None:
    def baseline(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _observed_output()

    def candidate(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _observed_output()

    ship_report = verify_imported_cases(
        [
            _case(
                "clean",
                root_input={"user_message": "What is the policy?"},
                observed_output=_observed_output(),
                available_tools=[],
            )
        ],
        baseline_runner=baseline,
        candidate_runner=candidate,
        created_at="2026-06-07T12:00:00+00:00",
    )
    assert ship_report.summary["overall_verdict"] == "ship"

    caution_report = verify_imported_cases(
        [
            _case(
                "clean",
                root_input={"user_message": "What is the policy?"},
                observed_output=_observed_output(),
                available_tools=[],
            ),
            _case(
                "observed-drift",
                root_input={"user_message": "What is the policy?"},
                observed_output=_observed_output(risk_status="needs_review"),
                available_tools=[],
            ),
        ],
        baseline_runner=baseline,
        candidate_runner=candidate,
        created_at="2026-06-07T12:00:00+00:00",
    )
    assert caution_report.summary["verdict_counts"] == {"ship": 1, "caution": 1}
    assert caution_report.summary["overall_verdict"] == "caution"


def test_candidate_dataclass_output_with_live_tool_results_is_rescanned() -> None:
    """The harness must not trust a runner-reported zero count.

    A runner returning ImportedRunnerOutput whose payload contains live tool
    results but whose unsafe_live_execution_count field says 0 must still be
    detected and held.
    """
    case = _case(
        "under-reported",
        root_input={"user_message": "What is the policy?"},
        observed_output=_observed_output(),
        available_tools=[],
    )

    def baseline(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _observed_output()

    def candidate(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> ImportedRunnerOutput:
        return ImportedRunnerOutput(
            payload={
                **_observed_output(),
                "tool_results": [
                    {"tool_name": "send_live_email", "executed_live": True},
                    {"tool_name": "lookup_invoice", "executed_live": False},
                ],
            },
            unsafe_live_execution_count=0,
        )

    report = verify_imported_cases(
        [case],
        baseline_runner=baseline,
        candidate_runner=candidate,
        created_at="2026-06-07T12:00:00+00:00",
    )

    assert report.summary["unsafe_live_execution_count"] == 1
    assert report.summary["verdict_counts"] == {"hold": 1}
    assert report.summary["overall_verdict"] == "hold"


def test_baseline_dataclass_output_with_live_tool_results_stops_candidate() -> None:
    """Rescanning also applies to the baseline, before the candidate runs."""
    candidate_calls: list[str] = []
    case = _case(
        "baseline-under-reported",
        root_input={"user_message": "What is the policy?"},
        observed_output=_observed_output(),
        available_tools=[],
    )

    def baseline(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> ImportedRunnerOutput:
        return ImportedRunnerOutput(
            payload={
                **_observed_output(),
                "tool_results": [
                    {"tool_name": "send_live_email", "executed_live": True}
                ],
            },
            unsafe_live_execution_count=0,
        )

    def candidate(
        case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        candidate_calls.append(case.case_id)
        return _observed_output()

    report = verify_imported_cases(
        [case],
        baseline_runner=baseline,
        candidate_runner=candidate,
        created_at="2026-06-07T12:00:00+00:00",
    )

    assert candidate_calls == []
    assert report.summary["unsafe_live_execution_count"] == 1
    assert report.summary["stopped_case_reasons"] == {
        "baseline-under-reported": ["baseline_unsafe_live_execution_detected"]
    }
    assert report.summary["verdict_counts"] == {"hold": 1}


def test_scan_mode_allowlist_is_rejected_for_candidate_execution() -> None:
    def runner(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _observed_output()

    with pytest.raises(ValueError, match="NO_TOOL_REGISTRY_EXPECTATION"):
        verify_imported_cases(
            [
                _case(
                    "scan-mode",
                    root_input={"user_message": "What is the policy?"},
                    observed_output=_observed_output(),
                    available_tools=[],
                )
            ],
            baseline_runner=runner,
            candidate_runner=runner,
            allowed_tool_names=NO_TOOL_REGISTRY_EXPECTATION,
            created_at="2026-06-07T12:00:00+00:00",
        )


def test_empty_cohort_overall_verdict_is_hold() -> None:
    def runner(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _observed_output()

    report = verify_imported_cases(
        [],
        baseline_runner=runner,
        candidate_runner=runner,
        created_at="2026-06-07T12:00:00+00:00",
    )

    assert report.summary["case_count"] == 0
    assert report.summary["verdict_counts"] == {}
    assert report.summary["overall_verdict"] == "hold"


def test_runner_distinguishes_fixture_harness_mode() -> None:
    case = _case(
        "fixture",
        root_input={"user_message": "What is the policy?"},
        observed_output=_observed_output(),
        available_tools=[],
    )

    def runner(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _observed_output()

    report = verify_imported_cases(
        [case],
        baseline_runner=runner,
        candidate_runner=runner,
        execution_mode=FIXTURE_HARNESS_EXECUTION_MODE,
        created_at="2026-06-07T12:00:00+00:00",
    )

    assert report.execution_mode == FIXTURE_HARNESS_EXECUTION_MODE
    assert report.summary["execution_mode"] == FIXTURE_HARNESS_EXECUTION_MODE
    assert report.summary["mode_detail"] == (
        "Fixture evidence; not a real-agent candidate comparison."
    )
