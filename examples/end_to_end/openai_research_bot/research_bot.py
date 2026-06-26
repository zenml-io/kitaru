"""OpenAI research bot — planner, search agents, writer on Kitaru.

Run locally::

    cd examples/end_to_end/openai_research_bot
    uv sync --extra local --extra openai-agents
    uv run kitaru init
    export OPENAI_API_KEY=sk-...
    uv run python research_bot.py "AI agent durability" --max-searches 2
"""

import argparse
import os
import re
import sys
from typing import Annotated, Any, TypeVar

from agents import RunConfig
from pydantic import BaseModel

import kitaru
from kitaru import ImageSettings, checkpoint, flow
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest
from kitaru.client import KitaruClient
from kitaru.config import classify_stack_deployment_type
from kitaru.errors import KitaruAmbiguousFlowResultError

try:  # Package import path used by tests.
    from .bot_agents import (
        DEFAULT_MODEL,
        SMARTER_MODEL,
        new_planner_agent,
        new_search_agent,
        new_writer_agent,
    )
    from .models import ReportData, SearchSummary, WebSearchItem, WebSearchPlan
    from .prompts import build_search_input, build_writer_input
    from .tools import SEARCH_TOOL_MODEL_ENV
except ImportError:  # Direct script path used by README commands.
    from bot_agents import (
        DEFAULT_MODEL,
        SMARTER_MODEL,
        new_planner_agent,
        new_search_agent,
        new_writer_agent,
    )
    from models import ReportData, SearchSummary, WebSearchItem, WebSearchPlan
    from prompts import build_search_input, build_writer_input
    from tools import SEARCH_TOOL_MODEL_ENV

DEFAULT_MAX_SEARCHES = 5
MAX_SEARCHES_LIMIT = 10
SECRET_NAME = "openai-research-bot-keys"

_REMOTE_STACK_DEPLOYMENT_TYPES = frozenset(
    {"kubernetes", "vertex", "sagemaker", "azureml"}
)
_NON_SECRET_ENV_VARS = (
    "OPENAI_RESEARCH_BOT_MODEL",
    "OPENAI_RESEARCH_BOT_PLANNER_MODEL",
    "OPENAI_RESEARCH_BOT_SEARCH_MODEL",
    "OPENAI_RESEARCH_BOT_WRITER_MODEL",
    "OPENAI_RESEARCH_BOT_MAX_SEARCHES",
    SEARCH_TOOL_MODEL_ENV,
)
FAIL_AFTER_SEARCHES_ENV = "KITARU_RESEARCH_BOT_FAIL_AFTER_SEARCHES"
_TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}

RESEARCH_BOT_IMAGE = ImageSettings(
    requirements=[
        "openai-agents>=0.15.0,<0.16.0",
        "openai>=1.0.0,<3",
    ],
)

T = TypeVar("T", bound=BaseModel)


def clamp_max_searches(value: int) -> int:
    """Keep search fan-out inside a small, example-friendly cost range."""
    return min(max(value, 1), MAX_SEARCHES_LIMIT)


def _normalize_search_plan(
    plan: WebSearchPlan,
    original_query: str,
    max_searches: int,
) -> WebSearchPlan:
    """Trim, deduplicate, and add a fallback search if the planner returns none."""
    limit = clamp_max_searches(max_searches)
    normalized: list[WebSearchItem] = []
    seen: set[str] = set()

    for item in plan.searches:
        query = item.query.strip()
        reason = item.reason.strip() or "The planner marked this search as relevant."
        if not query:
            continue
        key = re.sub(r"\s+", " ", query.lower())
        if key in seen:
            continue
        seen.add(key)
        normalized.append(WebSearchItem(query=query, reason=reason))
        if len(normalized) >= limit:
            break

    if normalized:
        return WebSearchPlan(searches=normalized)

    return WebSearchPlan(
        searches=[
            WebSearchItem(
                query=original_query,
                reason=(
                    "Fallback search because the planner returned no usable searches."
                ),
            )
        ]
    )


@checkpoint
def normalize_search_plan(
    plan: WebSearchPlan,
    original_query: str,
    max_searches: int,
) -> Annotated[WebSearchPlan, "research_plan"]:
    """Publish the normalized research plan as a replayable artifact."""
    return _normalize_search_plan(plan, original_query, max_searches)


def missing_api_key_message(secret_name: str = SECRET_NAME) -> str:
    """Friendly setup text for users who run the example without credentials."""
    return (
        "Missing OPENAI_API_KEY.\n\n"
        "For local runs, set it in your shell and rerun:\n"
        "  export OPENAI_API_KEY='sk-...'\n\n"
        "For remote stacks, create a Kitaru secret instead:\n"
        f"  kitaru secrets set {secret_name} --OPENAI_API_KEY=sk-...\n\n"
        "The example passes only the secret name to remote runs; it does not put "
        "the key in flow parameters, logs, or artifacts."
    )


def _env_flag_enabled(name: str) -> bool:
    """Return whether an environment flag is explicitly enabled."""
    return os.getenv(name, "").strip().lower() in _TRUTHY_ENV_VALUES


def _expect_structured_output(value: Any, model_type: type[T], *, stage: str) -> T:
    """Validate structured OpenAI output against the model this example expects."""
    if isinstance(value, model_type):
        return value
    if isinstance(value, BaseModel):
        return model_type.model_validate(value.model_dump())
    if isinstance(value, dict):
        return model_type.model_validate(value)
    raise RuntimeError(
        f"The {stage} agent returned {type(value).__name__}, but this example "
        f"expects structured {model_type.__name__} output."
    )


def _completed_final_output(result: Any, *, stage: str) -> Any:
    """Return final output or raise if an OpenAI run paused unexpectedly."""
    if result.status != "completed":
        raise RuntimeError(
            f"The {stage} agent returned status={result.status!r}. "
            "This example does not use human approval tools, so the run "
            "should complete."
        )
    return result.final_output


def _load_checkpoint_output(value: Any) -> Any:
    """Materialize a checkpoint output handle when dynamic execution returns one."""
    load = getattr(value, "load", None)
    return load() if callable(load) else value


def _new_runner(agent: Any, *, name: str | None = None) -> KitaruRunner:
    """Build a Kitaru-wrapped OpenAI runner with tracing disabled for clarity."""
    return KitaruRunner(
        agent,
        name=name,
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )


@checkpoint
def publish_search_summaries(
    summaries: list[Any],
) -> Annotated[list[dict[str, Any]], "search_summaries"]:
    """Save the aggregate search results as a readable dashboard artifact.

    Submitted checkpoints can reload Pydantic objects through a different import
    path than the direct script uses. Keep the ZenML boundary permissive, then
    validate each item against this module's `SearchSummary` model inside the
    checkpoint body.
    """
    return [
        _expect_structured_output(
            summary, SearchSummary, stage="search summary"
        ).model_dump(mode="json")
        for summary in summaries
    ]


@checkpoint
def run_search_item(
    index: int,
    item: WebSearchItem,
    search_model: str,
    search_tool_model: str,
) -> SearchSummary:
    """Run one planned search as a submitted durable checkpoint."""
    try:
        searcher = _new_runner(
            new_search_agent(
                model=search_model,
                search_tool_model=search_tool_model,
            ),
            name=f"research_searcher_{index + 1:02d}",
        )
        search_result = searcher.run_sync(
            OpenAIRunRequest.start(
                build_search_input(query=item.query, reason=item.reason),
                metadata={"stage": "search", "search_index": index},
                max_turns=4,
            )
        )
        search_output = _completed_final_output(
            search_result,
            stage=f"search {index + 1}",
        )
        return SearchSummary(
            index=index,
            query=item.query,
            reason=item.reason,
            status="completed",
            summary=str(search_output),
        )
    except Exception as error:
        return SearchSummary(
            index=index,
            query=item.query,
            reason=item.reason,
            status="failed",
            summary=(
                "This search failed; the writer should treat this as missing evidence."
            ),
            error_message=f"{type(error).__name__}: {error}",
        )


@checkpoint
def durability_drill_gate(
    search_summaries_artifact: list[dict[str, Any]],
) -> Annotated[dict[str, Any], "durability_drill"]:
    """Optionally fail after searches so users can retry from saved checkpoints."""
    search_count = len(search_summaries_artifact)
    if _env_flag_enabled(FAIL_AFTER_SEARCHES_ENV):
        raise RuntimeError(
            "Intentional durability drill failure after the search stage. "
            f"Unset {FAIL_AFTER_SEARCHES_ENV} and replay this execution with "
            "`kitaru executions replay <EXECUTION_ID> --at "
            "durability_drill_gate`; Kitaru should reuse the completed "
            "planner/search outputs."
        )
    return {
        "enabled": False,
        "fail_after_searches_env": FAIL_AFTER_SEARCHES_ENV,
        "search_count": search_count,
    }


@checkpoint
def publish_report(
    report: ReportData,
    metadata: dict[str, Any],
    research_plan_artifact: WebSearchPlan,
    search_summaries_artifact: list[dict[str, Any]],
    durability_drill_artifact: dict[str, Any],
) -> Annotated[str, "final_report"]:
    """Save the final Markdown report and related metadata artifacts."""
    # These values are already saved as named artifacts by earlier checkpoints.
    # Keeping them as inputs makes `publish_report` depend on those publishing
    # checkpoints, so Kitaru has one unambiguous terminal output to return.
    _ = research_plan_artifact, search_summaries_artifact, durability_drill_artifact
    kitaru.save("research_report_metadata", metadata, type="context")
    kitaru.save("follow_up_questions", report.follow_up_questions, type="context")
    return report.markdown_report


@flow(image=RESEARCH_BOT_IMAGE, cache=False)
def openai_research_bot(
    query: str,
    max_searches: int,
    planner_model: str,
    search_model: str,
    writer_model: str,
    search_tool_model: str,
    fail_on_search_error: bool = False,
) -> str:
    """Run planner → search agents → writer, then publish a report artifact."""
    max_searches = clamp_max_searches(max_searches)

    planner = _new_runner(
        new_planner_agent(model=planner_model, max_searches=max_searches)
    )
    planner_result = planner.run_sync(
        OpenAIRunRequest.start(
            f"Query: {query}",
            metadata={"stage": "planner"},
            max_turns=3,
        )
    )
    raw_plan = _expect_structured_output(
        _completed_final_output(planner_result, stage="planner"),
        WebSearchPlan,
        stage="planner",
    )
    research_plan_artifact = normalize_search_plan(
        raw_plan,
        query,
        max_searches,
    )
    plan = research_plan_artifact.load()

    search_futures = [
        run_search_item.submit(
            index,
            item,
            search_model,
            search_tool_model,
            id=f"search_{index + 1:02d}",
        )
        for index, item in enumerate(plan.searches)
    ]
    summaries = [_load_checkpoint_output(future.result()) for future in search_futures]
    search_summaries_artifact = publish_search_summaries(summaries)
    durability_drill_artifact = durability_drill_gate(search_summaries_artifact)

    failed = [item for item in summaries if item.status == "failed"]
    if fail_on_search_error and failed:
        details = "; ".join(
            f"{item.query}: {item.error_message or item.summary}" for item in failed
        )
        raise RuntimeError(f"{len(failed)} search agent run(s) failed: {details}")

    writer = _new_runner(new_writer_agent(model=writer_model))
    writer_result = writer.run_sync(
        OpenAIRunRequest.start(
            build_writer_input(original_query=query, summaries=summaries),
            metadata={"stage": "writer"},
            max_turns=3,
        )
    )
    report = _expect_structured_output(
        _completed_final_output(writer_result, stage="writer"),
        ReportData,
        stage="writer",
    )

    metadata = {
        "query": query,
        "planner_model": planner_model,
        "search_model": search_model,
        "writer_model": writer_model,
        "search_tool_model": search_tool_model,
        "planned_search_count": len(plan.searches),
        "completed_search_count": sum(
            1 for item in summaries if item.status == "completed"
        ),
        "failed_search_count": sum(1 for item in summaries if item.status == "failed"),
        "short_summary": report.short_summary,
        "follow_up_questions": report.follow_up_questions,
    }
    return publish_report(
        report,
        metadata,
        research_plan_artifact,
        search_summaries_artifact,
        durability_drill_artifact,
    )


def _collect_non_secret_env() -> dict[str, str]:
    """Forward local non-secret config into remote images."""
    return {key: os.environ[key] for key in _NON_SECRET_ENV_VARS if os.getenv(key)}


def _active_stack_is_remote() -> bool:
    """Return whether the current stack needs remote secret injection."""
    try:
        return classify_stack_deployment_type() in _REMOTE_STACK_DEPLOYMENT_TYPES
    except Exception:
        return False


def _image_override_for_active_stack(secret_name: str) -> dict[str, Any] | None:
    """Inject OPENAI_API_KEY by secret name only for remote-stack runs."""
    if not _active_stack_is_remote():
        return None
    return {
        "requirements": list(RESEARCH_BOT_IMAGE.requirements or []),
        "environment": _collect_non_secret_env(),
        "secret_environment_from": [secret_name],
    }


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the research bot example."""
    default_model = os.getenv("OPENAI_RESEARCH_BOT_MODEL", DEFAULT_MODEL)
    parser = argparse.ArgumentParser(
        description="Run a durable OpenAI research bot on Kitaru."
    )
    parser.add_argument("query", help="Research question or topic.")
    parser.add_argument(
        "--max-searches",
        type=int,
        default=_env_int("OPENAI_RESEARCH_BOT_MAX_SEARCHES", DEFAULT_MAX_SEARCHES),
        help=f"Planner search budget, clamped to 1-{MAX_SEARCHES_LIMIT}.",
    )
    parser.add_argument(
        "--model",
        default=default_model,
        help=(
            f"Default model for all stages (default: {DEFAULT_MODEL}; "
            f"try {SMARTER_MODEL} for a smarter/pricier run)."
        ),
    )
    parser.add_argument(
        "--planner-model",
        default=os.getenv("OPENAI_RESEARCH_BOT_PLANNER_MODEL"),
        help="Planner model override. Defaults to --model.",
    )
    parser.add_argument(
        "--search-model",
        default=os.getenv("OPENAI_RESEARCH_BOT_SEARCH_MODEL"),
        help="Search summarizer model override. Defaults to --model.",
    )
    parser.add_argument(
        "--writer-model",
        default=os.getenv("OPENAI_RESEARCH_BOT_WRITER_MODEL"),
        help="Writer model override. Defaults to --model.",
    )
    parser.add_argument(
        "--search-tool-model",
        default=os.getenv(SEARCH_TOOL_MODEL_ENV),
        help=(
            "Responses API model used inside the local search_web tool. "
            "Defaults to --search-model."
        ),
    )
    parser.add_argument(
        "--fail-on-search-error",
        action="store_true",
        help="Exit non-zero if any search agent run fails.",
    )
    parser.add_argument(
        "--secret-name",
        default=SECRET_NAME,
        help="Kitaru secret name that holds OPENAI_API_KEY for remote stacks.",
    )
    return parser.parse_args(argv)


def _run_once(
    args: argparse.Namespace,
    *,
    image_override: dict[str, Any] | None,
) -> str:
    model = args.model
    planner_model = args.planner_model or model
    search_model = args.search_model or model
    writer_model = args.writer_model or model
    search_tool_model = args.search_tool_model or search_model

    run_kwargs: dict[str, Any] = {
        "query": args.query,
        "max_searches": clamp_max_searches(args.max_searches),
        "planner_model": planner_model,
        "search_model": search_model,
        "writer_model": writer_model,
        "search_tool_model": search_tool_model,
        "fail_on_search_error": args.fail_on_search_error,
    }
    if image_override is not None:
        run_kwargs["image"] = image_override

    handle = openai_research_bot.run(**run_kwargs)
    try:
        return str(handle.wait())
    except KitaruAmbiguousFlowResultError:
        artifacts = KitaruClient().artifacts.list(
            handle.exec_id,
            name="final_report",
            limit=1,
        )
        if not artifacts:
            raise
        return str(artifacts[0].load())


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    args = parse_args(argv)
    image_override = _image_override_for_active_stack(args.secret_name)
    if not os.getenv("OPENAI_API_KEY") and image_override is None:
        print(missing_api_key_message(args.secret_name), file=sys.stderr)
        return 2
    if not os.getenv("OPENAI_API_KEY") and image_override is not None:
        print(
            f"OPENAI_API_KEY is not set locally; relying on remote secret "
            f"{args.secret_name!r}.",
            file=sys.stderr,
        )

    print(
        f"Running OpenAI research bot with model={args.model!r}, "
        f"max_searches={clamp_max_searches(args.max_searches)}."
    )
    report = _run_once(args, image_override=image_override)
    print("\n=== final report ===\n")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
