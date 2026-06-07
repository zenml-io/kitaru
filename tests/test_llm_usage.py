"""Tests for canonical LLM usage metadata helpers."""

import math
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru._client._models import (
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionStatus,
)
from kitaru._llm_usage import (
    LLM_FLAT_ACTUAL_COST_USD_KEY,
    LLM_FLAT_DISPLAY_COST_USD_KEY,
    LLM_FLAT_ESTIMATED_COST_USD_KEY,
    LLM_FLAT_INCURRED_CALL_COUNT_KEY,
    LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY,
    LLM_FLAT_REUSED_CALL_COUNT_KEY,
    LLM_FLAT_REUSED_TOTAL_TOKENS_KEY,
    LLM_USAGE_METADATA_KEY,
    LLM_USAGE_SUMMARY_METADATA_KEY,
    aggregate_usage_records,
    build_usage_record,
    execution_metadata_from_records,
    log_usage_record_best_effort,
    parse_usage_summary,
    usage_records_from_metadata,
)


def test_aggregate_prefers_actual_cost_for_display() -> None:
    actual_record = build_usage_record(
        adapter="claude_agent_sdk",
        surface="agent_invocation",
        record_id="actual",
        input_tokens=10,
        output_tokens=5,
        actual_cost_usd=0.25,
    )
    estimated_record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="estimated",
        input_tokens=7,
        output_tokens=3,
        estimated_cost_usd=0.04,
    )

    summary = aggregate_usage_records([actual_record, estimated_record])

    assert summary["call_count"] == 2
    assert summary["incurred_call_count"] == 2
    assert summary["total_tokens"] == 25
    assert summary["actual_cost_usd"] == 0.25
    assert summary["estimated_cost_usd"] == 0.04
    assert summary["display_cost_usd"] == 0.29


def test_aggregate_preserves_explicit_zero_total_tokens() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="zero-total",
        input_tokens=10,
        output_tokens=5,
        total_tokens=0,
    )

    summary = aggregate_usage_records([record])

    assert summary["input_tokens"] == 10
    assert summary["output_tokens"] == 5
    assert summary["total_tokens"] == 0
    assert summary["incurred_total_tokens"] == 0


def test_reused_records_do_not_add_incurred_cost_or_tokens() -> None:
    record = build_usage_record(
        adapter="pydantic_ai",
        surface="model_call",
        record_id="cached",
        input_tokens=100,
        output_tokens=20,
        estimated_cost_usd=0.5,
        billing_effect="reused_not_incurred",
        cache_status="checkpoint_cache_hit",
    )

    summary = aggregate_usage_records([record])

    assert summary["call_count"] == 1
    assert summary["incurred_call_count"] == 0
    assert summary["reused_call_count"] == 1
    assert summary["total_tokens"] == 120
    assert summary["reused_total_tokens"] == 120
    assert summary["incurred_total_tokens"] == 0
    assert summary["display_cost_usd"] == 0.0


def test_retry_attempts_with_same_record_id_are_counted_separately() -> None:
    first_attempt = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="same-call",
        total_tokens=11,
    )
    second_attempt = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="same-call",
        total_tokens=13,
    )

    records = [
        *usage_records_from_metadata(
            {LLM_USAGE_METADATA_KEY: {"same-call": first_attempt}},
            source_attempt_id="attempt-1",
        ),
        *usage_records_from_metadata(
            {LLM_USAGE_METADATA_KEY: {"same-call": second_attempt}},
            source_attempt_id="attempt-2",
        ),
    ]

    summary = aggregate_usage_records(records)

    assert summary["call_count"] == 2
    assert summary["incurred_total_tokens"] == 24


def test_duplicate_record_from_same_attempt_is_counted_once() -> None:
    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="same-call",
        total_tokens=11,
    )
    records = usage_records_from_metadata(
        {LLM_USAGE_METADATA_KEY: {"first": record, "duplicate": record}},
        source_attempt_id="attempt-1",
    )

    summary = aggregate_usage_records(records)

    assert summary["call_count"] == 1
    assert summary["total_tokens"] == 11


def test_zero_token_values_are_preserved_as_present_values() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        usage={
            "input_tokens": 0,
            "prompt_tokens": 99,
            "output_tokens": 0,
            "completion_tokens": 88,
            "total_tokens": 0,
            "cached_input_tokens": 0,
            "details": {"cached_tokens": 77},
            "reasoning_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 66},
        },
    )

    assert record["usage"]["input_tokens"] == 0
    assert record["usage"]["output_tokens"] == 0
    assert record["usage"]["total_tokens"] == 0
    assert record["usage"]["cached_input_tokens"] == 0
    assert record["usage"]["reasoning_tokens"] == 0


def test_gemini_snake_case_usage_fields_are_normalized() -> None:
    record = build_usage_record(
        adapter="gemini_interactions",
        surface="gemini_interaction",
        usage={
            "prompt_token_count": 10,
            "candidates_token_count": 4,
            "total_token_count": 17,
            "cached_content_token_count": 3,
            "thoughts_token_count": 2,
            "tool_use_prompt_token_count": 1,
        },
    )

    assert record["usage"]["input_tokens"] == 10
    assert record["usage"]["output_tokens"] == 4
    assert record["usage"]["total_tokens"] == 17
    assert record["usage"]["cached_input_tokens"] == 3
    assert record["usage"]["reasoning_tokens"] == 2
    assert record["usage"]["raw"]["tool_use_prompt_token_count"] == 1


def test_gemini_camel_case_usage_fields_are_normalized() -> None:
    record = build_usage_record(
        adapter="gemini_interactions",
        surface="gemini_interaction",
        usage={
            "promptTokenCount": 12,
            "candidatesTokenCount": 5,
            "totalTokenCount": 20,
            "cachedContentTokenCount": 4,
            "thoughtsTokenCount": 3,
            "toolUsePromptTokenCount": 2,
        },
    )

    assert record["usage"]["input_tokens"] == 12
    assert record["usage"]["output_tokens"] == 5
    assert record["usage"]["total_tokens"] == 20
    assert record["usage"]["cached_input_tokens"] == 4
    assert record["usage"]["reasoning_tokens"] == 3
    assert record["usage"]["raw"]["toolUsePromptTokenCount"] == 2


def test_gemini_explicit_zero_usage_values_are_preserved() -> None:
    record = build_usage_record(
        adapter="gemini_interactions",
        surface="gemini_interaction",
        usage={
            "prompt_token_count": 0,
            "promptTokenCount": 99,
            "candidates_token_count": 0,
            "candidatesTokenCount": 88,
            "total_token_count": 0,
            "totalTokenCount": 77,
            "cached_content_token_count": 0,
            "cachedContentTokenCount": 66,
            "thoughts_token_count": 0,
            "thoughtsTokenCount": 55,
        },
    )

    assert record["usage"]["input_tokens"] == 0
    assert record["usage"]["output_tokens"] == 0
    assert record["usage"]["total_tokens"] == 0
    assert record["usage"]["cached_input_tokens"] == 0
    assert record["usage"]["reasoning_tokens"] == 0


def test_gemini_token_only_record_aggregates_without_cost() -> None:
    record = build_usage_record(
        adapter="gemini_interactions",
        surface="gemini_interaction",
        record_id="gemini-call",
        model="gemini-test",
        usage={
            "promptTokenCount": 8,
            "candidatesTokenCount": 6,
            "totalTokenCount": 14,
        },
        cost_source="none",
    )

    summary = aggregate_usage_records([record])

    assert summary["call_count"] == 1
    assert summary["incurred_call_count"] == 1
    assert summary["input_tokens"] == 8
    assert summary["output_tokens"] == 6
    assert summary["total_tokens"] == 14
    assert summary["actual_cost_usd"] == 0.0
    assert summary["estimated_cost_usd"] == 0.0
    assert summary["display_cost_usd"] == 0.0
    assert summary["records_without_cost_count"] == 1
    assert summary["adapters"] == ["gemini_interactions"]
    assert summary["models"] == ["gemini-test"]


@pytest.mark.parametrize("invalid_cost", [True, -0.01, math.nan, math.inf, "bad"])
def test_invalid_cost_values_are_omitted(invalid_cost: object) -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="bad-cost",
        total_tokens=3,
        actual_cost_usd=cast(float, invalid_cost),
        estimated_cost_usd=cast(float, invalid_cost),
    )

    assert record["cost"]["actual_cost_usd"] is None
    assert record["cost"]["estimated_cost_usd"] is None

    summary = aggregate_usage_records([record])
    assert summary["display_cost_usd"] == 0.0
    assert summary["records_without_cost_count"] == 1


def test_zero_cost_values_are_preserved() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        actual_cost_usd=0,
        estimated_cost_usd=0,
    )

    assert record["cost"]["actual_cost_usd"] == 0.0
    assert record["cost"]["estimated_cost_usd"] == 0.0


def test_malformed_records_are_ignored_during_parsing() -> None:
    records = usage_records_from_metadata(
        {
            LLM_USAGE_METADATA_KEY: {
                "bad": {"schema_version": 1, "record_id": "missing-blocks"},
                "not-a-record": "hello",
            }
        }
    )

    assert records == []


def test_aggregate_skips_malformed_records_without_counting_them() -> None:
    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="valid",
        total_tokens=5,
    )

    summary = aggregate_usage_records(
        [record, {"schema_version": 1, "record_id": "missing-blocks"}]
    )

    assert summary["call_count"] == 1
    assert "malformed_record_count" not in summary


def test_parsed_records_preserve_billing_and_cache_status() -> None:
    record = build_usage_record(
        adapter="pydantic_ai",
        surface="model_call",
        record_id="cached",
        billing_effect="reused_not_incurred",
        cache_status="checkpoint_cache_hit",
    )

    records = usage_records_from_metadata({LLM_USAGE_METADATA_KEY: {"cached": record}})

    assert records[0]["billing_effect"] == "reused_not_incurred"
    assert records[0]["cache_status"] == "checkpoint_cache_hit"


def test_log_usage_record_best_effort_suppresses_backend_errors(monkeypatch) -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="provider-call",
        total_tokens=5,
    )

    def fail_log(**metadata: object) -> None:
        raise RuntimeError("metadata backend down")

    monkeypatch.setattr("kitaru.logging.log", fail_log)

    log_usage_record_best_effort(record)


def test_execution_metadata_omits_empty_summary_by_default() -> None:
    assert execution_metadata_from_records([]) == {}
    assert LLM_USAGE_SUMMARY_METADATA_KEY in execution_metadata_from_records(
        [],
        include_empty_summary=True,
    )


def test_execution_metadata_is_deterministic_json_summary() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="run",
        total_tokens=42,
        estimated_cost_usd=0.123,
    )

    first = execution_metadata_from_records([record])
    second = execution_metadata_from_records([record])

    assert first == second
    assert isinstance(first[LLM_USAGE_SUMMARY_METADATA_KEY], str)
    assert parse_usage_summary(
        first[LLM_USAGE_SUMMARY_METADATA_KEY]
    ) == parse_usage_summary(second[LLM_USAGE_SUMMARY_METADATA_KEY])


def test_execution_metadata_uses_json_summary_and_flat_numeric_keys() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="run",
        total_tokens=42,
        estimated_cost_usd=0.123,
    )

    metadata = execution_metadata_from_records([record])
    summary = parse_usage_summary(metadata[LLM_USAGE_SUMMARY_METADATA_KEY])

    assert summary is not None
    assert summary["call_count"] == 1
    assert metadata[LLM_FLAT_INCURRED_CALL_COUNT_KEY] == 1
    assert metadata[LLM_FLAT_REUSED_CALL_COUNT_KEY] == 0
    assert metadata[LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY] == 42
    assert metadata[LLM_FLAT_REUSED_TOTAL_TOKENS_KEY] == 0
    assert metadata[LLM_FLAT_ACTUAL_COST_USD_KEY] == 0.0
    assert metadata[LLM_FLAT_ESTIMATED_COST_USD_KEY] == 0.123
    assert metadata[LLM_FLAT_DISPLAY_COST_USD_KEY] == 0.123


def test_cached_checkpoint_attempt_public_records_are_marked_reused() -> None:
    from kitaru._client._mappers import _map_checkpoint_attempt

    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="cached-call",
        total_tokens=100,
        actual_cost_usd=1.25,
        billing_effect="incurred",
        cache_status="executed",
    )
    attempt = _map_checkpoint_attempt(
        cast(
            Any,
            SimpleNamespace(
                id="attempt-cached",
                name="cached_call",
                status="cached",
                start_time=None,
                end_time=None,
                run_metadata={LLM_USAGE_METADATA_KEY: {"cached-call": record}},
                exception_info=None,
            ),
        )
    )

    records = attempt.llm_usage_records

    assert attempt.status is ExecutionStatus.COMPLETED
    assert len(records) == 1
    assert records[0]["billing_effect"] == "reused_not_incurred"
    assert records[0]["cache_status"] == "checkpoint_cache_hit"
    assert records[0]["cost"]["actual_cost_usd"] == 1.25
    assert "_source_attempt_id" not in records[0]


def test_execution_llm_usage_records_include_unique_run_level_records() -> None:
    checkpoint_record = build_usage_record(
        adapter="pydantic_ai",
        surface="model_call",
        record_id="same-record",
        event_id="same-event",
        total_tokens=11,
    )
    run_level_record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="run-only",
        event_id="run-only-event",
        total_tokens=7,
    )
    duplicate_run_record = dict(checkpoint_record)
    attempt = CheckpointAttempt(
        attempt_id="attempt-1",
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        metadata={LLM_USAGE_METADATA_KEY: {"checkpoint": checkpoint_record}},
        failure=None,
    )
    checkpoint = CheckpointCall(
        call_id="call-1",
        name="model_call",
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        metadata={},
        original_call_id=None,
        parent_call_ids=[],
        failure=None,
        attempts=[attempt],
        artifacts=[],
    )
    execution = Execution(
        exec_id="exec-1",
        flow_id=None,
        flow_name=None,
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        stack_name=None,
        metadata={
            LLM_USAGE_METADATA_KEY: {
                "duplicate": duplicate_run_record,
                "run-only": run_level_record,
            }
        },
        status_reason=None,
        failure=None,
        pending_wait=None,
        frozen_execution_spec=None,
        original_exec_id=None,
        checkpoints=[checkpoint],
        artifacts=[],
        _client=cast(Any, None),
    )

    records = execution.llm_usage_records

    assert [record["record_id"] for record in records] == [
        "run-only",
        "same-record",
    ]
    assert all("_source_attempt_id" not in record for record in records)
