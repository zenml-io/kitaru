"""Tests for imported-input Replay Verify validation."""

from __future__ import annotations

from typing import Any

from kitaru._replay_verify_imported_models import (
    ImportedCaseSourceRef,
    ImportedReplayCase,
    RecordedCall,
    RecordedCallKind,
    ReplayTraceContract,
    RetrievalContext,
    RunnerContract,
    TenantContext,
    imported_case_from_mapping,
)
from kitaru._replay_verify_imported_validation import validate_imported_case

RUNNER_ENTRYPOINT = "run_support_copilot_case"


def _case(
    *,
    root_input: Any = None,
    observed_output: Any = None,
    available_tools: list[str] | None = None,
    recorded_calls: list[RecordedCall] | None = None,
    retrieval_context: RetrievalContext | None = None,
    side_effect_policy: str | None = "safe",
    runner_entrypoint: str | None = RUNNER_ENTRYPOINT,
) -> ImportedReplayCase:
    return ImportedReplayCase(
        case_id="case-1",
        source_ref=ImportedCaseSourceRef(
            source_system="langfuse",
            source_id="trace-1",
            observation_ids=["obs-1"],
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
            side_effect_policy=side_effect_policy,
        ),
        runner_contract=RunnerContract(entrypoint=runner_entrypoint),
        tenant_context=TenantContext(
            tenant_id="tenant-alpha",
            workspace_id="workspace-alpha",
            user_id="user-alpha",
            role="member",
            permission_scope="tenant:tenant-alpha:member",
        ),
        retrieval_context=retrieval_context,
    )


def _observed_output(*, tool_names: list[str] | None = None) -> dict[str, Any]:
    return {
        "policy_label": "support_policy",
        "risk_status": "safe",
        "tool_names": tool_names or [],
        "retrieval_document_ids": [],
    }


def _tool_call(
    name: str,
    *,
    side_effect_status: str = "safe",
    executed_live: bool = False,
    kind: RecordedCallKind = "tool",
) -> RecordedCall:
    return RecordedCall(
        kind=kind,
        name=name,
        output_payload={
            "tool_name": name,
            "side_effect_status": side_effect_status,
            "executed_live": executed_live,
        },
    )


def _valid_rag_metadata() -> RetrievalContext:
    return RetrievalContext(
        query="How do I rotate an API key?",
        retriever_name="support_copilot_kb_retriever",
        corpus_index_version="support-kb-2026-06-06-a",
        top_k=4,
        returned_document_ids=["doc-1", "doc-2"],
        returned_chunk_ids=["chunk-1", "chunk-2"],
        chunk_hashes=["hash-1", "hash-2"],
        tenant_id="tenant-alpha",
        permission_scope="tenant:tenant-alpha:member",
        retrieval_timestamp="2026-06-07T10:00:00Z",
    )


def test_legacy_mapping_without_trace_id_is_ineligible() -> None:
    case = imported_case_from_mapping(
        {
            "case_id": "legacy-no-trace",
            "source_system": "fixture-jsonl",
            "input_payload": {"user_message": "What is the policy?"},
            "observed_output": _observed_output(),
            "config_hints": {
                "trace_contract_version": "trace-contract-v1",
                "app_name": "support-copilot",
                "app_version": "2026-06-07",
                "model": "openai-chat:gpt-5",
                "prompt_version": "support-copilot-v1",
                "prompt_hash": "abc123",
                "available_tool_names": [],
                "local_runner_entrypoint": RUNNER_ENTRYPOINT,
            },
        }
    )

    result = validate_imported_case(case)

    assert case.source_ref.source_id == ""
    assert result.fidelity.eligibility == "ineligible"
    assert "missing_trace_id" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_neutral_mapping_without_source_id_is_ineligible() -> None:
    case = imported_case_from_mapping(
        {
            "case_id": "neutral-no-source-id",
            "source_ref": {"source_system": "fixture-jsonl"},
            "root_input": {"user_message": "What is the policy?"},
            "observed_output": _observed_output(),
            "trace_contract": {
                "trace_contract_version": "trace-contract-v1",
                "app_name": "support-copilot",
                "app_version": "2026-06-07",
                "model": "openai-chat:gpt-5",
                "prompt_version": "support-copilot-v1",
                "prompt_hash": "abc123",
                "available_tools": [],
            },
            "runner_contract": {"entrypoint": RUNNER_ENTRYPOINT},
        }
    )

    result = validate_imported_case(case)

    assert case.source_ref.source_id == ""
    assert result.fidelity.eligibility == "ineligible"
    assert "missing_trace_id" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_missing_input_is_ineligible_and_stops_candidate() -> None:
    result = validate_imported_case(
        _case(
            root_input=None,
            observed_output=_observed_output(),
            available_tools=[],
        )
    )

    assert result.fidelity.eligibility == "ineligible"
    assert "missing_root_input" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False
    assert result.stop_reasons == result.fidelity.reasons


def test_missing_output_is_non_comparable() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "What is the policy?"},
            observed_output=None,
            available_tools=[],
        )
    )

    assert result.fidelity.eligibility == "non_comparable"
    assert "missing_observed_output_or_evaluator_signal" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_missing_available_tools_is_non_comparable() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Can you look up invoice INV-1?"},
            observed_output=_observed_output(tool_names=["lookup_invoice"]),
            available_tools=None,
            recorded_calls=[_tool_call("lookup_invoice")],
        )
    )

    assert result.fidelity.eligibility == "non_comparable"
    assert "missing_available_tools" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_unsafe_write_like_tool_is_unsafe_ineligible() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Send a live email."},
            observed_output=_observed_output(tool_names=["send_live_email"]),
            available_tools=["send_live_email"],
            recorded_calls=[_tool_call("send_live_email")],
            side_effect_policy="unsafe_ineligible",
        )
    )

    assert result.fidelity.eligibility == "unsafe_ineligible"
    assert result.safety.status == "unsafe_ineligible"
    assert "unknown_tool:send_live_email" in result.fidelity.reasons
    assert "unsafe_or_unknown_write_like_tool_blocked" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_observed_read_only_tool_not_available_is_non_comparable() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Can you look up invoice INV-1?"},
            observed_output=_observed_output(tool_names=["lookup_invoice"]),
            available_tools=[],
            recorded_calls=[_tool_call("lookup_invoice")],
        )
    )

    assert result.fidelity.eligibility == "non_comparable"
    assert "observed_tool_not_available:lookup_invoice" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_observed_write_like_tool_not_available_is_unsafe_ineligible() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Create a support ticket."},
            observed_output=_observed_output(tool_names=["create_support_ticket"]),
            available_tools=[],
            recorded_calls=[_tool_call("create_support_ticket")],
        )
    )

    assert result.fidelity.eligibility == "unsafe_ineligible"
    assert (
        "observed_write_like_tool_not_available:create_support_ticket"
        in result.fidelity.reasons
    )
    assert result.candidate_execution_allowed is False


def test_missing_recorded_read_only_tool_call_is_non_comparable() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Can you look up invoice INV-1?"},
            observed_output=_observed_output(tool_names=["lookup_invoice"]),
            available_tools=["lookup_invoice"],
            recorded_calls=[],
        )
    )

    assert result.fidelity.eligibility == "non_comparable"
    assert "missing_recorded_tool_call:lookup_invoice" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_missing_recorded_write_like_tool_call_is_unsafe_ineligible() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Create a support ticket."},
            observed_output=_observed_output(tool_names=["create_support_ticket"]),
            available_tools=["create_support_ticket"],
            recorded_calls=[],
        )
    )

    assert result.fidelity.eligibility == "unsafe_ineligible"
    assert (
        "missing_recorded_write_like_tool_call:create_support_ticket"
        in result.fidelity.reasons
    )
    assert result.candidate_execution_allowed is False


def test_write_like_tool_without_side_effect_status_is_unsafe_ineligible() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Create a support ticket."},
            observed_output=_observed_output(tool_names=["create_support_ticket"]),
            available_tools=["create_support_ticket"],
            recorded_calls=[
                RecordedCall(
                    kind="tool",
                    name="create_support_ticket",
                    output_payload={"executed_live": False},
                )
            ],
        )
    )

    assert result.fidelity.eligibility == "unsafe_ineligible"
    assert "ambiguous_side_effect_status_write_like_tool" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_write_like_tool_without_explicit_not_live_flag_is_unsafe_ineligible() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Create a support ticket."},
            observed_output=_observed_output(tool_names=["create_support_ticket"]),
            available_tools=["create_support_ticket"],
            recorded_calls=[
                RecordedCall(
                    kind="tool",
                    name="create_support_ticket",
                    output_payload={"side_effect_status": "mocked"},
                )
            ],
        )
    )

    assert result.fidelity.eligibility == "unsafe_ineligible"
    assert "ambiguous_side_effect_status_write_like_tool" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_mocked_write_like_tool_with_explicit_not_live_flag_is_eligible() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "Create a support ticket."},
            observed_output=_observed_output(tool_names=["create_support_ticket"]),
            available_tools=["create_support_ticket"],
            recorded_calls=[
                _tool_call(
                    "create_support_ticket",
                    side_effect_status="mocked",
                    executed_live=False,
                )
            ],
        )
    )

    assert result.fidelity.eligibility == "eligible"
    assert result.safety.status == "mocked"
    assert result.candidate_execution_allowed is True


def test_valid_rag_metadata_is_eligible() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "How do I rotate an API key?"},
            observed_output={
                **_observed_output(tool_names=["search_knowledge_base"]),
                "retrieval_document_ids": ["doc-1", "doc-2"],
            },
            available_tools=["search_knowledge_base"],
            recorded_calls=[_tool_call("search_knowledge_base", kind="retrieval")],
            retrieval_context=_valid_rag_metadata(),
        )
    )

    assert result.fidelity.eligibility == "eligible"
    assert result.fidelity.verdict == "ship"
    assert "required_fields_recovered" in result.fidelity.reasons
    assert result.candidate_execution_allowed is True


def test_retrieval_call_without_retrieval_context_is_non_comparable() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "What is the refund policy?"},
            observed_output=_observed_output(tool_names=["retrieve_policy"]),
            available_tools=["retrieve_policy"],
            recorded_calls=[_tool_call("retrieve_policy", kind="retrieval")],
            retrieval_context=None,
        )
    )

    assert result.fidelity.eligibility == "non_comparable"
    assert "missing_rag_metadata:retriever_name" in result.fidelity.reasons
    assert "missing_rag_metadata:returned_document_ids" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_observed_retrieval_documents_without_context_are_non_comparable() -> None:
    result = validate_imported_case(
        _case(
            root_input={"user_message": "What is the refund policy?"},
            observed_output={
                **_observed_output(),
                "retrieval_document_ids": ["doc-refund-policy"],
            },
            available_tools=[],
            recorded_calls=[],
            retrieval_context=None,
        )
    )

    assert result.fidelity.eligibility == "non_comparable"
    assert "missing_rag_metadata:retriever_name" in result.fidelity.reasons
    assert "missing_rag_metadata:returned_document_ids" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_incomplete_rag_metadata_is_non_comparable() -> None:
    incomplete_rag = RetrievalContext(
        query="How do I rotate an API key?",
        retriever_name="support_copilot_kb_retriever",
        corpus_index_version="support-kb-2026-06-06-a",
        tenant_id="tenant-alpha",
        permission_scope="tenant:tenant-alpha:member",
    )

    result = validate_imported_case(
        _case(
            root_input={"user_message": "How do I rotate an API key?"},
            observed_output=_observed_output(tool_names=["search_knowledge_base"]),
            available_tools=["search_knowledge_base"],
            recorded_calls=[_tool_call("search_knowledge_base", kind="retrieval")],
            retrieval_context=incomplete_rag,
        )
    )

    assert result.fidelity.eligibility == "non_comparable"
    assert "missing_rag_metadata:returned_document_ids" in result.fidelity.reasons
    assert "missing_rag_metadata:returned_chunk_ids" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_stale_rag_metadata_and_permission_mismatch_stop_candidate() -> None:
    bad_rag = RetrievalContext(
        query="How do I rotate an API key?",
        retriever_name="support_copilot_kb_retriever",
        corpus_index_version="support-kb-old",
        top_k=4,
        returned_document_ids=["doc-1", "doc-2"],
        returned_chunk_ids=["chunk-1", "chunk-2"],
        chunk_hashes=["hash-1", "hash-2"],
        tenant_id="tenant-beta",
        permission_scope="tenant:tenant-beta:admin",
        retrieval_timestamp="2026-06-07T10:00:00Z",
    )

    result = validate_imported_case(
        _case(
            root_input={"user_message": "Show the tenant admin guide."},
            observed_output=_observed_output(tool_names=["search_knowledge_base"]),
            available_tools=["search_knowledge_base"],
            recorded_calls=[_tool_call("search_knowledge_base", kind="retrieval")],
            retrieval_context=bad_rag,
        )
    )

    assert result.fidelity.eligibility == "unsafe_ineligible"
    assert "stale_corpus_index_version" in result.fidelity.reasons
    assert "permission_mismatch_cross_tenant_document" in result.fidelity.reasons
    assert "permission_scope_mismatch" in result.fidelity.reasons
    assert result.candidate_execution_allowed is False


def test_candidate_runner_is_not_called_for_stopped_case() -> None:
    calls: list[str] = []
    result = validate_imported_case(
        _case(
            root_input={"user_message": "What is the policy?"},
            observed_output=None,
            available_tools=[],
        )
    )

    if result.candidate_execution_allowed:
        calls.append("candidate")

    assert result.fidelity.eligibility == "non_comparable"
    assert calls == []


def test_trust_boundary_jsonl_shape_maps_without_exec_id() -> None:
    raw = {
        "case_id": "tb-valid-001",
        "cohort": "trust_boundary",
        "source_system": "langfuse",
        "trace_id": "trace-1",
        "observation_ids": ["obs-1"],
        "input_payload": {"user_message": "What is the policy?"},
        "observed_output": _observed_output(),
        "recorded_calls": [],
        "config_hints": {
            "app_name": "support-copilot",
            "app_version": "2026-06-07",
            "trace_contract_version": "trace-contract-v1",
            "model": "openai-chat:gpt-5",
            "prompt_version": "support-copilot-v1",
            "prompt_hash": "abc123",
            "available_tool_names": [],
            "local_runner_entrypoint": RUNNER_ENTRYPOINT,
        },
    }

    case = imported_case_from_mapping(raw)
    result = validate_imported_case(case)

    assert "exec_id" not in raw
    assert case.source_ref.source_id == "trace-1"
    assert result.fidelity.eligibility == "eligible"
