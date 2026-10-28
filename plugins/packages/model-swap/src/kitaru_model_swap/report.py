#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Aggregate replay results into a report and insight cards.

The summary insight carries the full report under `REPORT_METADATA_KEY`, for
agents and scripts, and a rendered HTML section under `REPORT_SECTION_KEY`,
which `kitaru insight report` embeds without calling any model.
"""

import re
import statistics
from collections import Counter, defaultdict
from collections.abc import Callable
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    CategoryValue,
    InsightInput,
    TextInsightData,
)
from kitaru_model_swap.calls import Collection, RecordedCall, estimate_usage
from kitaru_model_swap.compare import AGREEING, AGREEMENT_LABELS
from kitaru_model_swap.decision_plan import DecisionPlan
from kitaru_model_swap.decision_report import (
    build_decision_candidate_report,
    get_decision_shapes,
)
from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.html import render_section
from kitaru_model_swap.mix import (
    get_best_mix,
    get_mix_rows,
    split_routes,
)
from kitaru_model_swap.params import (
    Candidate,
    DecisionJudge,
    ModelSwapParams,
)
from kitaru_model_swap.picks import (
    add_paired_rates,
    add_summaries,
    get_best_for,
    get_headline,
    get_primary_rate,
    is_measured,
)
from kitaru_model_swap.pricing import ResolvedPrice, call_cost
from kitaru_model_swap.runner import (
    CallResult,
    DecisionResult,
    NotRunReason,
    describe,
)
from kitaru_model_swap.stats import (
    compute_interval,
    compute_percentile,
    compute_rate,
    format_decimal,
    format_pct,
    format_usd,
    format_yes_no,
    round_rate,
)
from kitaru_model_swap.steps import (
    get_group,
    get_route_key,
    get_step_results,
    summarize_by_group,
)
from kitaru_model_swap.volume import (
    Volume,
    get_volume,
    project_cost,
)

REPORT_METADATA_KEY = "kitaru.model_swap/v1"
REPORT_SECTION_KEY = "kitaru.report_section/v1"
SUMMARY_INSIGHT_NAME = "model-swap-summary"
_EXCERPT = 600
_REVIEW_ORDER: dict[bool | None, int] = {False: 0, None: 1, True: 2}
# A judge that changes this share of its rulings when only the order of the
# answers changes is too unstable to lean on.
_UNSTABLE_JUDGE = 0.2
_MIN_JUDGE_RECHECKS = 5


def build_report(
    params: ModelSwapParams,
    collection: Collection,
    sampled: list[RecordedCall],
    results: list[CallResult],
    prices: dict[str, ResolvedPrice | None],
    *,
    spent: Decimal,
    stopped_early: NotRunReason | None,
    plans: list[tuple[RecordedCall, DecisionPlan]] | None = None,
    decision_results: list[DecisionResult] | None = None,
    judge_check: dict[str, int] | None = None,
    unmetered_attempts: int = 0,
) -> dict[str, Any]:
    """Build the JSON report for a completed run or a dry run.

    `unmetered_attempts` counts attempts charged their reservation against
    the spend cap because the provider may have billed them without
    reporting usage.
    """
    plans = plans or []
    volume = get_volume(collection, params)
    by_candidate: dict[str, list[CallResult]] = defaultdict(list)
    for result in results:
        by_candidate[result.candidate].append(result)
    candidates = [
        _candidate_report(
            candidate,
            sampled,
            by_candidate[candidate.name],
            prices.get(candidate.name),
            volume,
            collection.group_seen,
            dry_run=params.dry_run,
            excluded=params.is_excluded(candidate),
            repeats=params.repeats,
        )
        for candidate in params.candidates
    ]
    if not params.dry_run:
        add_paired_rates(candidates, by_candidate)
        add_summaries(candidates)
    disagreements = [
        _disagreement(result)
        for result in results
        if result.attempt == 0
        and result.agreement is not None
        and result.agreement not in AGREEING
    ]
    # Which disagreements are kept must not depend on the judge, whose rulings
    # text inside an answer can sway, so the cap is applied in a fixed order
    # first. Only the kept ones are then ordered for review within each group:
    # answers the judge found different first, then unsure or not judged ones,
    # then likely equivalent ones.
    disagreements.sort(key=lambda item: (item["group"], item["candidate"]))
    kept = disagreements[: params.max_disagreements]
    kept.sort(
        key=lambda item: (
            item["group"],
            _REVIEW_ORDER[item["judge_equivalent"]],
            item["candidate"],
        )
    )
    coverage = _coverage(collection, sampled)
    weight = population_weight(sampled, collection.group_seen)
    mix = (
        None
        if params.dry_run
        else get_best_mix(
            candidates,
            sampled,
            volume,
            params.min_mix_calls,
            split_routes(by_candidate, sampled, weight),
            weight,
        )
    )
    return {
        "schema": 1,
        "dry_run": params.dry_run,
        "coverage": coverage,
        "volume": {
            "sessions": volume.sessions,
            "calls_per_session": round(volume.calls_per_session, 2),
            "sessions_per_month": (
                None
                if volume.sessions_per_month is None
                else round(volume.sessions_per_month, 1)
            ),
            "source": volume.source,
        },
        "recorded": _recorded_report(sampled, volume, collection.group_seen),
        "candidates": candidates,
        "decision_shapes": get_decision_shapes(plans),
        "decision_candidates": [
            build_decision_candidate_report(
                candidate,
                plans,
                [r for r in decision_results or [] if r.candidate == candidate.name],
                prices.get(candidate.name),
                dry_run=params.dry_run,
            )
            for candidate in params.decision_candidates
        ],
        "best_for": []
        if params.dry_run
        else get_best_for(candidates) + get_mix_rows(mix),
        "best_mix": mix,
        "excluded_origins": dict(params.excluded_origins),
        "disagreements": kept,
        "disagreements_total": len(disagreements),
        "spent_usd": format_usd(spent),
        "max_spend_usd": format_usd(params.max_spend_usd),
        "stopped_early": stopped_early,
        "judge": _judge_label(params),
        "judge_check": judge_check,
        "notes": _notes(coverage)
        + _build_setup_notes(params, judge_check, {c.model for c in sampled if c.model})
        + (
            [
                "Per-session and monthly costs assume each candidate takes the "
                "same steps as the recording. A model that needs more turns, "
                "retries, or tool calls to finish a task costs more per task; "
                "a full replay of the agent measures that."
            ]
            if params.candidates and not params.dry_run
            else []
        )
        + (
            [
                f"{unmetered_attempts} requests may have been billed without "
                "reporting usage (a timeout or broken connection after the "
                "request was sent, or a reply without usage). Each was charged "
                "its full reservation against the spend cap, so the spend shown "
                "is an upper bound, and their cost is left out of per-call costs."
            ]
            if unmetered_attempts
            else []
        ),
    }


def _coverage(collection: Collection, sampled: list[RecordedCall]) -> dict[str, Any]:
    """Count what was seen, replayable, sampled and skipped."""
    inferred = collection.replayable["inferred"]
    return {
        "sessions": collection.sessions,
        "sessions_sampled": len({call.session_id for call in sampled}),
        "sessions_not_fetched": collection.sessions_not_fetched,
        "llm_calls_seen": collection.llm_calls_seen,
        "replayable": collection.replayable.total() - inferred,
        "replayable_inferred": inferred,
        "sampled": len(sampled),
        "skipped": dict(sorted(collection.skipped.items())),
        "schema_sources": dict(
            sorted(Counter(c.request.schema_source for c in sampled).items())
        ),
        "source_formats": dict(
            sorted(Counter(c.request.source_format for c in sampled).items())
        ),
        "calls_with_dropped_reasoning": sum(
            c.request.dropped_reasoning_parts > 0 for c in sampled
        ),
        "calls_without_system_prompt": sum(
            not any(m["role"] in {"system", "developer"} for m in c.request.messages)
            for c in sampled
        ),
    }


def _notes(coverage: dict[str, Any]) -> list[str]:
    """Explain anything that changes how the numbers should be read."""
    notes = []
    dropped = coverage["calls_with_dropped_reasoning"]
    if dropped:
        notes.append(
            f"{dropped} sampled calls had recorded reasoning in their history. "
            "Reasoning is not sent to candidates, so they see less context "
            "than the recorded model did."
        )
    inferred = coverage["schema_sources"].get("inferred", 0)
    if inferred:
        notes.append(
            f"{inferred} sampled calls use tool schemas inferred from recorded "
            "arguments. They are reported separately and left out of the "
            "headline agreement."
        )
    if without := coverage["calls_without_system_prompt"]:
        notes.append(
            "Sampled calls without a system or developer message: "
            f"{without}. The first user message may carry the instructions."
        )
    if no_ids := coverage["skipped"].get("missing_tool_call_id"):
        notes.append(
            f"{no_ids} calls were skipped because the recording has tool calls "
            "or results without ids, which endpoints need to pair them."
        )
    if unfetched := coverage["sessions_not_fetched"]:
        notes.append(
            f"The time budget ran out while fetching sessions; {unfetched} "
            "sessions were not read. Lower `max_calls` or raise "
            "`time_budget_seconds`."
        )
    if missing := coverage["skipped"].get("missing_tool_schema"):
        notes.append(
            f"{missing} calls were skipped "
            "because a tool they could use has no schema. Pass `tool_schemas` "
            "from the agent's code to include them."
        )
    return notes


def weighted_per_call(
    costs: list[tuple[RecordedCall, Decimal]], weights: Counter[tuple[str, str | None]]
) -> Decimal | None:
    """Mean cost per call, weighting each kind of call by how often it occurs.

    Sampling takes an even number of calls from each node name and model, so
    a plain mean would let rare, expensive calls stand in for common ones.
    """
    by_group: dict[tuple[str, str | None], list[Decimal]] = defaultdict(list)
    for call, cost in costs:
        by_group[(call.node_name, call.model)].append(cost)
    total = sum(weights[group] for group in by_group)
    if not by_group or not total:
        return None
    return sum(
        (
            sum(group_costs, Decimal(0)) / len(group_costs) * weights[group]
            for group, group_costs in by_group.items()
        ),
        Decimal(0),
    ) / Decimal(total)


def _recorded_report(
    sampled: list[RecordedCall],
    volume: Volume,
    weights: Counter[tuple[str, str | None]],
) -> dict[str, Any]:
    """Summarize the recorded model and its cost on the sampled calls."""
    per_call = weighted_per_call(
        [(c, c.cost) for c in sampled if c.cost is not None], weights
    )
    return {
        "models": dict(Counter(c.model or "unknown" for c in sampled).most_common()),
        "tokens_per_call": _mean_tokens([c.usage for c in sampled]),
        "cost": project_cost(per_call, volume),
    }


def _build_setup_notes(
    params: ModelSwapParams,
    judge_check: dict[str, int] | None,
    recorded_models: set[str],
) -> list[str]:
    """Explain how the run's setup limits what its numbers can show."""
    notes = []
    if params.candidates and not any(c.reference for c in params.candidates):
        notes.append(
            "No candidate is marked `reference`, so there is no noise floor. "
            "Today's model run again shows how much answers vary on their own, "
            "and a difference smaller than that is noise. Add the current model "
            "with `reference: true`."
        )
    if prompted := [c.name for c in params.candidates if c.system_prompt]:
        notes.append(
            f"{', '.join(prompted)} use their own system prompt instead of the "
            "recorded one, so their numbers measure the prompt and the model "
            "together."
        )
    judge = params.judge
    if isinstance(judge, Candidate):
        own = _normalize_model(judge.model)
        same = [c.name for c in params.candidates if _normalize_model(c.model) == own]
        if any(_normalize_model(m) == own for m in recorded_models):
            same.append("today's recorded model")
        if same:
            notes.append(
                f"The judge runs the same model as {', '.join(same)}. A model may "
                "favor answers like its own, so read the judge's rulings on "
                "those answers with care, or use a judge that is none of them."
            )
    if judge_check:
        checked, changed = judge_check["checked"], judge_check["changed"]
        unsure, unanswered = judge_check["unsure"], judge_check["unanswered"]
        if not checked:
            notes.append(
                f"The judge could not be asked again on any of its {unanswered} "
                "sampled rulings (the time or spend budget ran out), so how "
                "stable it is remains unknown."
            )
        else:
            note = (
                "The judge was asked again with the two answers in the other "
                f"order on {checked} of its rulings: {changed} flipped"
                + (f" and {unsure} became unsure" if unsure else "")
                + (f"; {unanswered} could not be asked" if unanswered else "")
                + "."
            )
            if checked >= _MIN_JUDGE_RECHECKS and changed / checked >= _UNSTABLE_JUDGE:
                note += " Treat its rulings as weak evidence and review the answers."
            notes.append(note)
    return notes


def _normalize_model(name: str) -> str:
    """Compare model names without a provider prefix such as `openai/`."""
    return name.rsplit("/", 1)[-1].casefold()


def _candidate_report(
    candidate: Candidate,
    sampled: list[RecordedCall],
    attempts: list[CallResult],
    price: ResolvedPrice | None,
    volume: Volume,
    weights: Counter[tuple[str, str | None]],
    *,
    dry_run: bool,
    excluded: bool = False,
    repeats: int = 1,
) -> dict[str, Any]:
    """Aggregate one candidate's results.

    Agreement comes from the first attempt at each call; every attempt
    counts toward cost, failures, latency and consistency.
    """
    report: dict[str, Any] = {
        "name": candidate.name,
        "model": candidate.model,
        "endpoint_host": urlsplit(candidate.base_url).hostname,
        "reference": candidate.reference,
        "open_weights": candidate.open_weights,
        "origin": None if candidate.origin is None else candidate.origin.model_dump(),
        "excluded": excluded,
        "price": (
            None
            if price is None
            else {
                "input_per_mtok": str(price.price.input_per_mtok),
                "output_per_mtok": str(price.price.output_per_mtok),
                "cached_input_per_mtok": format_decimal(
                    price.price.cached_input_per_mtok
                ),
                "source": price.source,
            }
        ),
    }
    if dry_run:
        max_tokens = candidate.provider_settings.max_tokens
        estimates = (
            []
            if price is None
            else [
                (
                    c,
                    call_cost(
                        price,
                        estimate_usage(
                            c, max_tokens, candidate.get_system_prompt(c.node_name)
                        ),
                    ),
                )
                for c in sampled
            ]
        )
        report["estimate"] = {
            "calls": len(sampled) * repeats,
            "spend_usd": format_usd(
                sum((cost for _, cost in estimates), Decimal(0)) * repeats
                if price is not None
                else None
            ),
            "cost": project_cost(weighted_per_call(estimates, weights), volume),
        }
        return report

    results = [r for r in attempts if r.attempt == 0]
    weight = population_weight(sampled, weights)
    sent = [r for r in results if r.reply is not None]
    answered = [r for r in sent if r.agreement is not None]
    every_sent = [r for r in attempts if r.reply is not None]
    headline = get_headline(attempts)
    inferred = [r for r in answered if r.call.request.schema_source == "inferred"]
    failed = [r.reply for r in every_sent if r.reply and r.reply.failure is not None]
    failures = Counter(reply.failure for reply in failed)
    tool_calls = [r for r in headline if r.call.request.recorded.tool_calls]
    agreeing = sum(r.agreement in AGREEING for r in headline)
    same_tool = sum(
        r.agreement in {"same_tool_same_args", "same_tool_different_args"}
        for r in tool_calls
    )
    # Failed calls carry no usage, so pricing them at zero would make a
    # failing candidate look cheap; only answered calls set the per-call cost.
    answered_costs = [
        (r.call, r.cost)
        for r in every_sent
        if r.agreement is not None and r.cost is not None
    ]
    spent = [r.cost for r in every_sent if r.cost is not None]
    replies = [r.reply for r in every_sent if r.reply]
    latencies = sorted(reply.latency_seconds for reply in replies)
    step_results = get_step_results(attempts)
    report |= {
        "coverage": {"answered": len(headline), "attempted": len(step_results)},
        "measured": is_measured(len(headline), len(step_results)),
        "calls_sent": len(every_sent),
        "calls_not_run": dict(Counter(r.not_run for r in attempts if r.not_run)),
        "consistency": _consistency(attempts, repeats),
        "answered": len(headline),
        "agreement": dict(Counter(r.agreement for r in headline).most_common()),
        "agreement_rate": compute_rate(agreeing, len(headline)),
        "agreement_interval": compute_interval(agreeing, len(headline)),
        "tool_choice_rate": compute_rate(same_tool, len(tool_calls)),
        "tool_choice_interval": compute_interval(same_tool, len(tool_calls)),
        "tool_choice_calls": len(tool_calls),
        "judged_equivalent": sum(r.judge_equivalent is True for r in headline),
        "judged_different": sum(r.judge_equivalent is False for r in headline),
        "judged_unsure": sum(
            r.judge_equivalent is None and r.judge_probability is not None
            for r in headline
        ),
        "judge_failures": dict(
            Counter(r.judge_failure for r in headline if r.judge_failure).most_common()
        ),
        "inferred_schema_agreement": dict(Counter(r.agreement for r in inferred)),
        "failures": dict(failures.most_common()),
        "failure_examples": {
            reply.failure: reply.failure_detail for reply in reversed(failed)
        },
        "failure_rate": compute_rate(sum(failures.values()), len(every_sent)),
        "by_tool": summarize_by_group(step_results, get_group, weight),
        "by_route": summarize_by_group(step_results, get_route_key(sampled), weight),
        "latency_seconds": {
            "p50": round_rate(statistics.median(latencies)) if latencies else None,
            "p95": round_rate(compute_percentile(latencies, 0.95))
            if latencies
            else None,
        },
        "tokens_per_call": _mean_tokens([reply.usage for reply in replies]),
        "retries": sum(reply.attempts - 1 for reply in replies),
        "cost": project_cost(weighted_per_call(answered_costs, weights), volume),
        # A candidate with no answered call of some kind has no cost for it,
        # so its per-call cost covers only the kinds it answered.
        "cost_complete": {(c.node_name, c.model) for c in sampled}
        <= {(call.node_name, call.model) for call, _ in answered_costs},
        "spent_usd": format_usd(sum(spent, Decimal(0)) if spent else None),
    }
    return report


def population_weight(
    sampled: list[RecordedCall], weights: Counter[tuple[str, str | None]]
) -> Callable[[RecordedCall], Decimal]:
    """Weight each sampled call by how many recorded calls it stands for.

    Sampling takes an even number of calls from each node name and model,
    so a call from a group seen 9,900 times stands for far more traffic than
    one from a group seen 100 times. A group without a recorded count stands
    for just its sampled calls.
    """
    sampled_counts = Counter((c.node_name, c.model) for c in sampled)

    def weight(call: RecordedCall) -> Decimal:
        group = (call.node_name, call.model)
        seen, count = weights.get(group), sampled_counts.get(group)
        return Decimal(seen) / count if seen and count else Decimal(1)

    return weight


def _disagreement(result: CallResult) -> dict[str, Any]:
    """Describe one disagreeing call for review."""
    assert result.reply is not None and result.agreement is not None
    return {
        "candidate": result.candidate,
        "session_id": str(result.call.session_id),
        "node_id": str(result.call.node_id),
        "node_name": result.call.node_name,
        "group": get_group(result.call),
        "agreement": result.agreement,
        "schema_source": result.call.request.schema_source,
        "recorded": describe(result.call.request.recorded)[:_EXCERPT],
        "candidate_answer": describe(result.reply)[:_EXCERPT],
        "judge_equivalent": result.judge_equivalent,
        "judge_probability": (
            None
            if result.judge_probability is None
            else round(result.judge_probability, 3)
        ),
        "judge_failure": result.judge_failure,
    }


def _mean_tokens(usages: list[Usage]) -> dict[str, float] | None:
    """Mean token counts per call."""
    if not usages:
        return None

    def mean(values: list[int]) -> float:
        return round(statistics.fmean(values), 1)

    return {
        "input_tokens": mean([u.input_tokens for u in usages]),
        "cached_input_tokens": mean([u.cached_input_tokens for u in usages]),
        "output_tokens": mean([u.output_tokens for u in usages]),
        "reasoning_tokens": mean([u.reasoning_tokens for u in usages]),
    }


def build_insights(
    report: dict[str, Any], *, dashboard_url: str | None
) -> list[InsightInput]:
    """Turn a report into insight cards; the summary carries the full report."""
    summary, *cards = _build_cards(report)
    summary.metadata[REPORT_SECTION_KEY] = {
        "html": render_section(report, dashboard_url),
        "covers": [card.name for card in cards],
    }
    return [summary, *cards]


def _build_cards(report: dict[str, Any]) -> list[InsightInput]:
    """Build the summary insight and one card per chart."""
    insights = [
        InsightInput(
            name=SUMMARY_INSIGHT_NAME,
            title=(
                "Model swap estimate" if report["dry_run"] else "Model swap results"
            ),
            description="What changes if these model calls move to other models.",
            data=TextInsightData(content=render_markdown(report)),
            metadata={REPORT_METADATA_KEY: report},
        ),
        InsightInput(
            name="model-swap-coverage",
            title="Recorded calls covered",
            data=CategoricalInsightData(
                unit="calls",
                values=[
                    CategoryValue(label="Sampled", value=report["coverage"]["sampled"]),
                    CategoryValue(
                        label="Replayable", value=report["coverage"]["replayable"]
                    ),
                    CategoryValue(
                        label="Replayable with inferred schemas",
                        value=report["coverage"]["replayable_inferred"],
                    ),
                    *(
                        CategoryValue(label=f"Skipped: {reason}", value=count)
                        for reason, count in report["coverage"]["skipped"].items()
                    ),
                ],
            ),
        ),
    ]
    shapes = report["decision_shapes"]
    if shapes["calls"]:
        insights.append(
            InsightInput(
                name="model-swap-decision-shapes",
                title="Calls a decision model could make",
                data=CategoricalInsightData(
                    unit="calls",
                    values=[
                        CategoryValue(label=label, value=shapes[shape])
                        for shape, label in (
                            ("decision", "Decision"),
                            ("partial", "Partly a decision"),
                            ("generative", "Needs an LLM"),
                        )
                    ],
                ),
            )
        )
    cost_values = _cost_values(report)
    if cost_values:
        if report["dry_run"]:
            unit = "USD for the sampled calls"
        elif report["volume"]["sessions_per_month"] is not None:
            unit = "USD per month"
        else:
            unit = "USD per session"
        insights.append(
            InsightInput(
                name="model-swap-cost",
                title="Estimated spend" if report["dry_run"] else "Projected cost",
                data=CategoricalInsightData(unit=unit, values=cost_values),
            )
        )
    if report["dry_run"]:
        return insights
    slugs: set[str] = set()
    for candidate in report["candidates"]:
        slug = _slug(candidate["name"], slugs)
        if candidate["agreement"]:
            insights.append(
                InsightInput(
                    name=f"model-swap-agreement-{slug}",
                    title=f"Agreement: {candidate['name']}",
                    description=candidate.get("summary"),
                    data=CategoricalInsightData(
                        unit="calls",
                        values=[
                            CategoryValue(label=AGREEMENT_LABELS[label], value=count)
                            for label, count in candidate["agreement"].items()
                        ],
                    ),
                )
            )
    if report["candidates"]:
        insights.append(
            InsightInput(
                name="model-swap-failures",
                title="Provider failures",
                description="Failed requests, counted apart from disagreement.",
                data=CategoricalInsightData(
                    unit="calls",
                    values=[
                        CategoryValue(
                            label=f"{candidate['name']}: {category}", value=count
                        )
                        for candidate in report["candidates"]
                        for category, count in candidate["failures"].items()
                    ]
                    or [
                        CategoryValue(label=candidate["name"], value=0)
                        for candidate in report["candidates"]
                    ],
                ),
            )
        )
    return insights


def _cost_values(report: dict[str, Any]) -> list[CategoryValue]:
    """Monthly (or per-session) cost per candidate and for the recording."""
    key = (
        "per_month_usd"
        if report["volume"]["sessions_per_month"] is not None
        else "per_session_usd"
    )
    values = []
    for candidate in report["candidates"]:
        if report["dry_run"]:
            amount = candidate["estimate"]["spend_usd"]
        else:
            amount = candidate["cost"][key]
        if amount is not None:
            label = candidate["name"]
            if not report["dry_run"] and not candidate["cost_complete"]:
                label += " (partial)"
            values.append(CategoryValue(label=label, value=float(amount)))
    if report["dry_run"]:
        values += [
            CategoryValue(label=c["name"], value=float(c["estimate"]["spend_usd"]))
            for c in report["decision_candidates"]
            if c["estimate"]["spend_usd"] is not None
        ]
    recorded = report["recorded"]["cost"][key]
    if recorded is not None and not report["dry_run"]:
        label = "Recorded"
        # Category labels must be unique, and a candidate may be named so.
        while any(value.label == label for value in values):
            label += " (recording)"
        values.append(CategoryValue(label=label, value=float(recorded)))
    return values


def render_markdown(report: dict[str, Any]) -> str:
    """Render a short Markdown summary of the report."""
    coverage = report["coverage"]
    lines = [
        f"Sampled {coverage['sampled']} of {coverage['llm_calls_seen']} recorded "
        f"model calls from {coverage['sessions_sampled']} of "
        f"{coverage['sessions']} sessions. "
        f"{coverage['replayable']} were replayable with real tool schemas"
        + (
            f", {coverage['replayable_inferred']} only with inferred schemas"
            if coverage["replayable_inferred"]
            else ""
        )
        + (
            ", skipped: "
            + ", ".join(f"{k} {v}" for k, v in coverage["skipped"].items())
            if coverage["skipped"]
            else ""
        )
        + ".",
        "",
    ]
    if report["dry_run"] and report["candidates"]:
        lines.append("| Candidate | Calls | Estimated spend (USD) |")
        lines.append("|---|---|---|")
        for candidate in report["candidates"]:
            estimate = candidate["estimate"]
            lines.append(
                f"| {candidate['name']} | {estimate['calls']} | "
                f"{estimate['spend_usd'] or 'unknown price'} |"
            )
    elif report["candidates"]:
        lines.append(
            "Stage 1 of 2: this screen compares single recorded calls, so it rules "
            "candidates out but does not pick a winner. Replay the candidates worth "
            "a full replay end to end, graded on outcomes, to decide."
        )
        lines.append("")
        lines.extend(
            f"- **{choice['goal']}:** {choice['candidate'] or 'No pick'}. "
            f"{choice['why']}"
            for choice in report["best_for"]
        )
        lines += [
            "",
            "| Candidate | Matches today | Failures | Cost per call (USD) "
            "| Latency p50 | Open weights |",
            "|---|---|---|---|---|---|",
        ]
        for candidate in report["candidates"]:
            label = candidate["name"] + (" (today)" if candidate["reference"] else "")
            p50 = candidate["latency_seconds"]["p50"]
            lines.append(
                f"| {label}"
                f" | {format_pct(get_primary_rate(candidate)[1])}"
                f" | {format_pct(candidate['failure_rate'])}"
                f" | {candidate['cost']['per_call_usd'] or 'unknown'}"
                f" | {'n/a' if p50 is None else f'{p50:.2f}s'}"
                f" | {format_yes_no(candidate['open_weights'])} |"
            )
        lines.append("")
        lines.extend(f"- **{c['name']}**: {c['summary']}" for c in report["candidates"])
    mix = report.get("best_mix")
    if mix:
        lines += ["", "Best mix:"]
        for item in mix["groups"]:
            use = item["candidate"] or "today's model"
            lines.append(
                f"- {item['group']} ({format_pct(item['share'])} of calls): {use}"
            )
    shapes = report["decision_shapes"]
    if shapes["calls"]:
        lines += [
            "",
            f"Decision-model fit: {shapes['decision']} decision, "
            f"{shapes['partial']} partial, {shapes['generative']} generative "
            f"of {shapes['calls']} sampled calls.",
        ]
    for candidate in report["decision_candidates"]:
        if "estimate" in candidate:
            spend = candidate["estimate"]["spend_usd"] or "unknown price"
            lines.append(
                f"- **{candidate['name']}** would be asked about "
                f"{candidate['estimate']['calls']} calls, estimated ${spend}."
            )
        else:
            lines.append(
                f"- **{candidate['name']}** picked the recorded tool "
                f"{format_pct(candidate['tool_choice_rate'])} of the time and matched "
                "every decision "
                f"{format_pct(candidate['decisions_match_rate'])} of the time, at "
                f"${candidate['cost_per_call_usd'] or 'unknown'} per call."
            )
    if report["stopped_early"]:
        lines += ["", f"Stopped early: {report['stopped_early']}."]
    if report["notes"]:
        lines += ["", *(f"- {note}" for note in report["notes"])]
    return "\n".join(lines)


def _consistency(attempts: list[CallResult], repeats: int) -> dict[str, Any] | None:
    """Report how often any and all attempts at a call matched the recording.

    With one attempt per call there is nothing to compare.
    """
    if repeats < 2:
        return None
    by_call: dict[UUID, list[CallResult]] = defaultdict(list)
    for result in attempts:
        if result.call.request.schema_source != "inferred":
            by_call[result.call.node_id].append(result)
    complete = [
        group
        for group in by_call.values()
        if len(group) == repeats and all(r.agreement is not None for r in group)
    ]
    return {
        "repeats": repeats,
        "calls": len(complete),
        "any_match_rate": compute_rate(
            sum(any(r.agreement in AGREEING for r in g) for g in complete),
            len(complete),
        ),
        "all_match_rate": compute_rate(
            sum(all(r.agreement in AGREEING for r in g) for g in complete),
            len(complete),
        ),
    }


def _judge_label(params: ModelSwapParams) -> str | None:
    """Name the judge in the report."""
    judge = params.judge
    if isinstance(judge, DecisionJudge):
        return f"{judge.kind} ({judge.model})"
    return None if judge is None else judge.name


def _slug(name: str, used: set[str]) -> str:
    """Make a unique insight-name suffix from a candidate name."""
    base = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "candidate"
    slug, index = base, 2
    while slug in used:
        slug, index = f"{base}-{index}", index + 1
    used.add(slug)
    return slug
