"""No-network contract tests for the OpenAI research bot example."""

import importlib

from examples.end_to_end.openai_research_bot.models import (
    SearchSummary,
    WebSearchItem,
    WebSearchPlan,
)
from examples.end_to_end.openai_research_bot.prompts import build_writer_input
from examples.end_to_end.openai_research_bot.research_bot import (
    DEFAULT_MODEL,
    SEARCH_CHECKPOINT_STRATEGY,
    clamp_max_searches,
    missing_api_key_message,
    normalize_search_plan,
    parse_args,
)
from examples.end_to_end.openai_research_bot.tools import _safe_error_message


def test_example_imports_without_openai_api_key(monkeypatch) -> None:
    """The example should be importable before a user sets credentials."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    for module_name in (
        "examples.end_to_end.openai_research_bot.models",
        "examples.end_to_end.openai_research_bot.prompts",
        "examples.end_to_end.openai_research_bot.tools",
        "examples.end_to_end.openai_research_bot.bot_agents",
        "examples.end_to_end.openai_research_bot.research_bot",
    ):
        importlib.import_module(module_name)


def test_normalize_search_plan_trims_deduplicates_and_clamps() -> None:
    plan = WebSearchPlan(
        searches=[
            WebSearchItem(query=" Durable agents ", reason="first"),
            WebSearchItem(query="durable   agents", reason="duplicate"),
            WebSearchItem(query="replay cost", reason="second"),
            WebSearchItem(query="checkpoint UI", reason="third"),
        ]
    )

    normalized = normalize_search_plan(
        plan,
        original_query="durable agents",
        max_searches=2,
    )

    assert [item.query for item in normalized.searches] == [
        "Durable agents",
        "replay cost",
    ]
    assert clamp_max_searches(0) == 1
    assert clamp_max_searches(99) == 10


def test_normalize_search_plan_adds_fallback_for_empty_plan() -> None:
    normalized = normalize_search_plan(
        WebSearchPlan(searches=[]),
        original_query="Why does replay help agents?",
        max_searches=5,
    )

    assert len(normalized.searches) == 1
    assert normalized.searches[0].query == "Why does replay help agents?"
    assert "Fallback" in normalized.searches[0].reason


def test_writer_input_includes_completed_and_failed_summaries() -> None:
    text = build_writer_input(
        "research durable agents",
        [
            SearchSummary(
                index=0,
                query="durable agents",
                reason="baseline",
                status="completed",
                summary="Checkpointing avoids duplicate calls.",
            ),
            SearchSummary(
                index=1,
                query="agent replay failure modes",
                reason="risk check",
                status="failed",
                summary="Missing evidence.",
                error_message="TimeoutError: search timed out",
            ),
        ],
    )

    assert "Original query: research durable agents" in text
    assert "Status: completed" in text
    assert "Status: failed" in text
    assert "TimeoutError: search timed out" in text


def test_search_checkpoint_uses_runner_call_strategy() -> None:
    assert SEARCH_CHECKPOINT_STRATEGY == "runner_call"


def test_safe_error_message_does_not_mangle_unset_api_key(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    message = _safe_error_message(RuntimeError("network timeout"))

    assert "network timeout" in message
    assert "[redacted]" not in message


def test_missing_api_key_message_is_actionable() -> None:
    message = missing_api_key_message("example-secret")

    assert "OPENAI_API_KEY" in message
    assert "export OPENAI_API_KEY" in message
    assert "kitaru secrets set example-secret" in message
    assert "flow parameters" in message


def test_parse_args_defaults_to_gpt_5_nano() -> None:
    args = parse_args(["Research durable agents", "--max-searches", "2"])

    assert args.model == DEFAULT_MODEL == "gpt-5-nano"
    assert args.max_searches == 2
    assert args.strategy == "calls"
    assert args.fail_on_search_error is False


def test_parse_args_supports_fail_on_search_error() -> None:
    args = parse_args(["Research durable agents", "--fail-on-search-error"])

    assert args.fail_on_search_error is True
