"""Tests for OpenAI Agents usage normalization."""

from types import SimpleNamespace

from kitaru._llm_usage import build_usage_record
from kitaru.adapters.openai_agents._usage import normalize_usage


def test_normalize_usage_reads_standard_openai_dict_shape() -> None:
    usage = normalize_usage(
        {
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "prompt_tokens_details": {"cached_tokens": 3},
            "completion_tokens_details": {"reasoning_tokens": 2},
        },
        model_name="gpt-5-mini",
    )

    assert usage.model_name == "gpt-5-mini"
    assert usage.input_tokens == 10
    assert usage.output_tokens == 4
    assert usage.total_tokens == 14
    assert usage.cached_input_tokens == 3
    assert usage.reasoning_tokens == 2


def test_normalize_usage_reads_agents_object_shape() -> None:
    raw_usage = SimpleNamespace(
        input_tokens=12,
        output_tokens=8,
        total_tokens=20,
        input_tokens_details=SimpleNamespace(cached_tokens=5),
        output_tokens_details=SimpleNamespace(reasoning_tokens=7),
    )

    usage = normalize_usage(raw_usage)

    assert usage.input_tokens == 12
    assert usage.output_tokens == 8
    assert usage.total_tokens == 20
    assert usage.cached_input_tokens == 5
    assert usage.reasoning_tokens == 7
    assert usage.raw is not None


def test_openai_usage_summary_populates_canonical_token_totals() -> None:
    usage = normalize_usage(
        {
            "input_tokens": 21,
            "output_tokens": 9,
            "input_tokens_details": {"cached_tokens": 6},
            "output_tokens_details": {"reasoning_tokens": 4},
        }
    )

    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        usage=usage.model_dump(mode="json"),
    )

    assert record["usage"]["input_tokens"] == 21
    assert record["usage"]["output_tokens"] == 9
    assert record["usage"]["total_tokens"] == 30
    assert record["usage"]["cached_input_tokens"] == 6
    assert record["usage"]["reasoning_tokens"] == 4
