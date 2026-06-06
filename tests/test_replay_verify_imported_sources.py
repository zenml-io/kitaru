"""Tests for imported-input Replay Verify source adapters."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from kitaru._replay_verify_imported_models import to_plain_data
from kitaru._replay_verify_imported_sources.jsonl import (
    validate_imported_cases_jsonl,
    write_imported_cases_jsonl,
)
from kitaru._replay_verify_imported_sources.langfuse import (
    cases_from_langfuse_observations,
)
from kitaru._replay_verify_imported_validation import validate_imported_case

RUNNER_ENTRYPOINT = "run_support_copilot_case"


def _observed_output(tool_names: list[str] | None = None) -> dict[str, Any]:
    return {
        "policy_label": "support_policy",
        "risk_status": "safe",
        "tool_names": tool_names or [],
        "retrieval_document_ids": [],
    }


def _root_row(
    *,
    trace_id: str,
    case_id: str,
    observation_id: str = "root-1",
    start_time: str = "2026-06-07T10:00:00Z",
    available_tools: list[str] | None = None,
    output: dict[str, Any] | None = None,
    extra_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = {
        "case_id": case_id,
        "cohort": "source-tests",
        "app_name": "support-copilot",
        "app_version": "2026-06-07",
        "trace_contract_version": "trace-contract-v1",
        "model": "openai-chat:gpt-5",
        "prompt_version": "support-copilot-v1",
        "prompt_hash": "abc123",
        "available_tools_json": json.dumps(available_tools or []),
        "local_runner_entrypoint": RUNNER_ENTRYPOINT,
        "side_effect_policy": "safe",
        "tenant_id": "tenant-alpha",
        "permission_scope": "tenant:tenant-alpha:member",
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    return {
        "id": observation_id,
        "traceId": trace_id,
        "traceName": f"trust-boundary-{case_id}",
        "name": "trust-boundary-case",
        "type": "SPAN",
        "startTime": start_time,
        "input": json.dumps(
            {"root_input": {"user_message": f"Question for {case_id}"}}
        ),
        "output": json.dumps(output or _observed_output(available_tools)),
        "metadata": json.dumps(metadata),
    }


def _tool_row(
    *,
    trace_id: str,
    observation_id: str = "tool-1",
    root_id: str = "root-1",
    start_time: str = "2026-06-07T10:01:00Z",
    name: str = "lookup_invoice",
) -> dict[str, Any]:
    return {
        "id": observation_id,
        "traceId": trace_id,
        "parentObservationId": root_id,
        "name": name,
        "type": "TOOL",
        "startTime": start_time,
        "input": json.dumps({"invoice_id": "INV-1"}),
        "output": json.dumps(
            {
                "invoice_id": "INV-1",
                "side_effect_status": "safe",
                "executed_live": False,
            }
        ),
        "metadata": json.dumps({"side_effect_status": "safe"}),
    }


def test_jsonl_rows_map_to_imported_cases_without_exec_id(tmp_path: Path) -> None:
    path = tmp_path / "imported_cases.jsonl"
    row = {
        "case_id": "jsonl-case-1",
        "cohort": "source-tests",
        "source_system": "fixture-jsonl",
        "trace_id": "trace-jsonl-1",
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
    path.write_text(json.dumps(row) + "\n")

    result = validate_imported_cases_jsonl(path)

    assert "exec_id" not in row
    assert len(result.cases) == 1
    assert result.cases[0].source_ref.source_id == "trace-jsonl-1"
    assert result.validations[0].fidelity.eligibility == "eligible"
    assert result.summary["case_count"] == 1


def test_langfuse_rows_group_by_trace_id_and_produce_neutral_cases() -> None:
    rows = [
        _root_row(
            trace_id="trace-a",
            case_id="case-a",
            available_tools=["lookup_invoice"],
            output=_observed_output(["lookup_invoice"]),
        ),
        _tool_row(trace_id="trace-a", name="lookup_invoice"),
        _root_row(trace_id="trace-b", case_id="case-b", observation_id="root-b"),
    ]

    cases = cases_from_langfuse_observations(
        rows,
        base_url="https://cloud.langfuse.com",
        source_ref="fixture/langfuse.json",
    )

    assert {case.case_id for case in cases} == {"case-a", "case-b"}
    case_a = next(case for case in cases if case.case_id == "case-a")
    assert case_a.source_ref.source_system == "langfuse"
    assert case_a.source_ref.source_id == "trace-a"
    assert case_a.source_ref.raw_source_ref == "fixture/langfuse.json"
    assert case_a.root_input == {"user_message": "Question for case-a"}
    assert case_a.trace_contract.available_tools == ["lookup_invoice"]
    assert [call.kind for call in case_a.recorded_calls] == ["tool"]
    assert case_a.recorded_calls[0].name == "lookup_invoice"


def test_langfuse_duplicate_ids_and_out_of_order_rows_preserve_reasons() -> None:
    rows = [
        _tool_row(
            trace_id="trace-dup",
            observation_id="tool-dup",
            start_time="2026-06-07T10:02:00Z",
        ),
        _root_row(
            trace_id="trace-dup",
            case_id="case-dup",
            available_tools=["lookup_invoice"],
            output=_observed_output(["lookup_invoice"]),
        ),
        _tool_row(
            trace_id="trace-dup",
            observation_id="tool-dup",
            start_time="2026-06-07T10:02:00Z",
        ),
    ]

    [case] = cases_from_langfuse_observations(rows)
    validation = validate_imported_case(case)

    assert case.source_ref.observation_ids == ["root-1", "tool-dup"]
    assert [call.observation_id for call in case.recorded_calls] == ["tool-dup"]
    assert case.raw_source_payload["source_import_reasons"] == [
        "duplicate_observations_deduplicated:tool-dup",
        "out_of_order_observations_sorted_before_import",
    ]
    assert "duplicate_observations_deduplicated:tool-dup" in validation.fidelity.reasons
    assert (
        "out_of_order_observations_sorted_before_import" in validation.fidelity.reasons
    )
    assert validation.fidelity.eligibility == "partial"
    assert validation.candidate_execution_allowed is False


def test_langfuse_partial_ingestion_reason_is_preserved() -> None:
    rows = [
        _root_row(
            trace_id="trace-partial",
            case_id="case-partial",
            available_tools=["lookup_invoice"],
            extra_metadata={"ingestion_status": "partial"},
        )
    ]

    [case] = cases_from_langfuse_observations(rows)
    validation = validate_imported_case(case)

    assert case.raw_source_payload["source_import_reasons"] == [
        "partial_langfuse_ingestion_after_polling_window"
    ]
    assert (
        "partial_langfuse_ingestion_after_polling_window" in validation.fidelity.reasons
    )
    assert validation.fidelity.eligibility == "partial"


def test_written_neutral_jsonl_reads_back_and_summarizes_source_reasons(
    tmp_path: Path,
) -> None:
    rows = [
        _root_row(
            trace_id="trace-roundtrip",
            case_id="case-roundtrip",
            available_tools=[],
        )
    ]
    [case] = cases_from_langfuse_observations(
        rows,
        partial_trace_ids={"trace-roundtrip"},
    )
    path = tmp_path / "neutral_cases.jsonl"

    write_imported_cases_jsonl([case], path)
    result = validate_imported_cases_jsonl(path)

    assert result.cases[0].source_ref.source_id == "trace-roundtrip"
    assert result.cases[0].root_input == {"user_message": "Question for case-roundtrip"}
    assert result.summary["source_import_reason_counts"] == {
        "partial_langfuse_ingestion_after_polling_window": 1
    }


def test_neutral_jsonl_top_level_source_reasons_are_preserved(
    tmp_path: Path,
) -> None:
    [case] = cases_from_langfuse_observations(
        [
            _root_row(
                trace_id="trace-top-level-reasons",
                case_id="case-top-level-reasons",
                available_tools=[],
            )
        ]
    )
    payload = to_plain_data(case)
    payload.pop("raw_source_payload")
    payload["source_import_reasons"] = [
        "partial_langfuse_ingestion_after_polling_window"
    ]
    path = tmp_path / "neutral_top_level_reasons.jsonl"
    path.write_text(json.dumps(payload) + "\n")

    result = validate_imported_cases_jsonl(path)

    assert result.cases[0].raw_source_payload["source_import_reasons"] == [
        "partial_langfuse_ingestion_after_polling_window"
    ]
    assert result.validations[0].fidelity.eligibility == "partial"


def test_langfuse_root_declared_read_tool_without_child_blocks() -> None:
    [case] = cases_from_langfuse_observations(
        [
            _root_row(
                trace_id="trace-missing-read-tool",
                case_id="case-missing-read-tool",
                available_tools=["lookup_invoice"],
                output=_observed_output(["lookup_invoice"]),
                extra_metadata={
                    "application_tool_names_json": json.dumps(["lookup_invoice"]),
                },
            )
        ]
    )

    validation = validate_imported_case(case)

    assert case.trace_contract.application_tool_names == ["lookup_invoice"]
    assert validation.fidelity.eligibility == "non_comparable"
    assert "missing_recorded_tool_call:lookup_invoice" in validation.fidelity.reasons
    assert validation.candidate_execution_allowed is False


def test_langfuse_root_declared_write_like_tool_without_child_row_is_unsafe() -> None:
    [case] = cases_from_langfuse_observations(
        [
            _root_row(
                trace_id="trace-missing-write-tool",
                case_id="case-missing-write-tool",
                available_tools=["create_support_ticket"],
                output=_observed_output(["create_support_ticket"]),
                extra_metadata={
                    "application_tool_names_json": json.dumps(
                        ["create_support_ticket"]
                    ),
                },
            )
        ]
    )

    validation = validate_imported_case(case)

    assert case.trace_contract.application_tool_names == ["create_support_ticket"]
    assert validation.fidelity.eligibility == "unsafe_ineligible"
    assert (
        "missing_recorded_write_like_tool_call:create_support_ticket"
        in validation.fidelity.reasons
    )
    assert validation.candidate_execution_allowed is False


def test_langfuse_retrieval_metadata_json_string_is_parsed() -> None:
    retrieval = {
        "query": "refund exceptions enterprise accounts",
        "retriever_name": "support_copilot_kb_retriever",
        "corpus_index_version": "support-kb-2026-06-06-a",
        "returned_document_ids": ["doc-refund-policy"],
        "returned_chunk_ids": ["chunk-refund-policy-1"],
        "tenant_id": "tenant-alpha",
        "permission_scope": "tenant:tenant-alpha:member",
    }
    [case] = cases_from_langfuse_observations(
        [
            _root_row(
                trace_id="trace-rag-json",
                case_id="case-rag-json",
                available_tools=["search_knowledge_base"],
                output={
                    **_observed_output(["search_knowledge_base"]),
                    "retrieval_document_ids": ["doc-refund-policy"],
                },
                extra_metadata={
                    "retrieval_metadata_json": json.dumps(retrieval),
                },
            ),
            _tool_row(
                trace_id="trace-rag-json",
                name="search_knowledge_base",
            ),
        ]
    )

    validation = validate_imported_case(case)

    assert case.retrieval_context is not None
    assert case.retrieval_context.returned_document_ids == ["doc-refund-policy"]
    assert validation.fidelity.eligibility == "eligible"


def test_langfuse_rows_without_trace_id_become_held_cases() -> None:
    row = _root_row(
        trace_id="trace-will-be-removed",
        case_id="case-missing-trace",
        available_tools=[],
    )
    row.pop("traceId")

    [case] = cases_from_langfuse_observations([row])
    validation = validate_imported_case(case)

    assert case.case_id == "case-missing-trace"
    assert case.source_ref.source_id == ""
    assert case.raw_source_payload["source_import_reasons"] == ["missing_trace_id"]
    assert validation.fidelity.eligibility == "ineligible"
    assert "missing_trace_id" in validation.fidelity.reasons
    assert validation.candidate_execution_allowed is False


def test_explicit_empty_allowed_tool_set_is_not_replaced_by_defaults() -> None:
    [case] = cases_from_langfuse_observations(
        [
            _root_row(
                trace_id="trace-empty-allow-list",
                case_id="case-empty-allow-list",
                available_tools=["lookup_invoice"],
                output=_observed_output(["lookup_invoice"]),
            ),
            _tool_row(trace_id="trace-empty-allow-list", name="lookup_invoice"),
        ]
    )

    validation = validate_imported_case(case, allowed_tool_names=set())

    assert "unknown_tool:lookup_invoice" in validation.fidelity.reasons
    assert validation.candidate_execution_allowed is False
