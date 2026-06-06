"""Tests for canonical LLM usage metadata helpers."""

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
