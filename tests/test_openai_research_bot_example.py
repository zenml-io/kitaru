"""No-network contract tests for the OpenAI research bot example."""

import argparse
import importlib
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from examples.end_to_end.openai_research_bot import research_bot
from examples.end_to_end.openai_research_bot.models import (
    ReportData,
    SearchSummary,
    WebSearchItem,
    WebSearchPlan,
)
from examples.end_to_end.openai_research_bot.prompts import build_writer_input
from examples.end_to_end.openai_research_bot.research_bot import (
    DEFAULT_MODEL,
    FAIL_AFTER_SEARCHES_ENV,
    _env_flag_enabled,
    _normalize_search_plan,
    clamp_max_searches,
    missing_api_key_message,
    parse_args,
)
from examples.end_to_end.openai_research_bot.tools import _safe_error_message

from kitaru.client import KitaruClient


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

    normalized = _normalize_search_plan(
        plan,
        "durable agents",
        2,
    )

    assert [item.query for item in normalized.searches] == [
        "Durable agents",
        "replay cost",
    ]
    assert clamp_max_searches(0) == 1
    assert clamp_max_searches(99) == 10


def test_normalize_search_plan_adds_fallback_for_empty_plan() -> None:
    normalized = _normalize_search_plan(
        WebSearchPlan(searches=[]),
        "Why does replay help agents?",
        5,
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
    assert not hasattr(args, "strategy")
    assert args.fail_on_search_error is False


def test_parse_args_supports_fail_on_search_error() -> None:
    args = parse_args(["Research durable agents", "--fail-on-search-error"])

    assert args.fail_on_search_error is True


def test_research_bot_flow_disables_ordinary_cache() -> None:
    assert research_bot.openai_research_bot._decorator_config.cache is False


def test_durability_drill_env_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(FAIL_AFTER_SEARCHES_ENV, raising=False)
    assert _env_flag_enabled(FAIL_AFTER_SEARCHES_ENV) is False

    monkeypatch.setenv(FAIL_AFTER_SEARCHES_ENV, "1")
    assert _env_flag_enabled(FAIL_AFTER_SEARCHES_ENV) is True


def test_cli_run_loads_final_report_when_runner_checkpoints_are_terminal(
    monkeypatch: pytest.MonkeyPatch,
    primed_zenml: None,
) -> None:
    """The script should print final_report even with terminal runner checkpoints."""

    del primed_zenml

    class FakeRunner:
        def run_sync(self, request: Any) -> SimpleNamespace:
            stage = request.metadata["stage"]
            if stage == "planner":
                output = WebSearchPlan(
                    searches=[
                        WebSearchItem(query="durable agents", reason="baseline"),
                        WebSearchItem(query="agent replay", reason="replay"),
                    ]
                )
            elif stage == "writer":
                output = ReportData(
                    short_summary="Durability avoids repeated work.",
                    markdown_report="# Durable agents\n\nReplay saves completed work.",
                    follow_up_questions=["How should tools be checkpointed?"],
                )
            else:
                output = "Search summary"
            return SimpleNamespace(status="completed", final_output=output)

    monkeypatch.setattr(
        research_bot,
        "_new_runner",
        lambda *_args, **_kwargs: FakeRunner(),
    )

    args = argparse.Namespace(
        query=f"durable agents {uuid4().hex}",
        max_searches=2,
        model="gpt-5-nano",
        planner_model=None,
        search_model=None,
        writer_model=None,
        search_tool_model=None,
        fail_on_search_error=True,
    )

    report = research_bot._run_once(args, image_override=None)

    assert report == "# Durable agents\n\nReplay saves completed work."


def test_flow_keeps_final_report_artifact_available(
    monkeypatch: pytest.MonkeyPatch,
    primed_zenml: None,
) -> None:
    """The final report should remain a named artifact despite runner checkpoints."""

    del primed_zenml

    class FakeRunner:
        def run_sync(self, request: Any) -> SimpleNamespace:
            stage = request.metadata["stage"]
            if stage == "planner":
                output = WebSearchPlan(
                    searches=[WebSearchItem(query="durable agents", reason="baseline")]
                )
            elif stage == "writer":
                output = ReportData(
                    short_summary="Durability avoids repeated work.",
                    markdown_report="# Durable agents\n\nReplay saves completed work.",
                    follow_up_questions=["How should tools be checkpointed?"],
                )
            else:
                output = "Search summary"
            return SimpleNamespace(status="completed", final_output=output)

    monkeypatch.setattr(
        research_bot,
        "_new_runner",
        lambda *_args, **_kwargs: FakeRunner(),
    )

    handle = research_bot.openai_research_bot.run(
        query=f"durable agents {uuid4().hex}",
        max_searches=1,
        planner_model="gpt-5-nano",
        search_model="gpt-5-nano",
        writer_model="gpt-5-nano",
        search_tool_model="gpt-5-nano",
        fail_on_search_error=True,
    )

    artifacts = KitaruClient().artifacts.list(
        handle.exec_id,
        name="final_report",
        limit=1,
    )
    assert len(artifacts) == 1
    assert artifacts[0].load() == "# Durable agents\n\nReplay saves completed work."
