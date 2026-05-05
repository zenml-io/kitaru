"""No-network contract tests for the OpenAI research bot example."""

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
    SEARCH_CHECKPOINT_STRATEGY,
    _env_flag_enabled,
    clamp_max_searches,
    missing_api_key_message,
    normalize_search_plan,
    parse_args,
)
from examples.end_to_end.openai_research_bot.tools import _safe_error_message
from zenml.client import Client


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


def test_durability_drill_env_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(FAIL_AFTER_SEARCHES_ENV, raising=False)
    assert _env_flag_enabled(FAIL_AFTER_SEARCHES_ENV) is False

    monkeypatch.setenv(FAIL_AFTER_SEARCHES_ENV, "1")
    assert _env_flag_enabled(FAIL_AFTER_SEARCHES_ENV) is True


def test_flow_has_one_terminal_output_after_side_effect_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    primed_zenml: None,
) -> None:
    """Side-effect artifact checkpoints should feed into the final report step."""

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

    handle = research_bot.openai_research_bot.run(
        query=f"durable agents {uuid4().hex}",
        max_searches=2,
        planner_model="gpt-5-nano",
        search_model="gpt-5-nano",
        writer_model="gpt-5-nano",
        search_tool_model="gpt-5-nano",
        checkpoint_strategy="runner_call",
        fail_on_search_error=True,
    )

    assert handle.wait() == "# Durable agents\n\nReplay saves completed work."

    hydrated = (
        Client()
        .get_pipeline_run(
            handle.exec_id,
            allow_name_prefix_match=False,
        )
        .get_hydrated_version()
    )
    upstream_step_names: set[str] = set()
    for step_run in hydrated.steps.values():
        step_spec = getattr(step_run, "spec", None)
        if step_spec is not None:
            upstream_step_names.update(getattr(step_spec, "upstream_steps", []) or [])

    terminal_step_names = sorted(
        step_name
        for step_name in hydrated.steps
        if step_name not in upstream_step_names
    )

    assert terminal_step_names == ["publish_report"]
