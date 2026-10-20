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
from dataclasses import dataclass
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
from kitaru_model_swap.compare import AGREEING, AGREEMENT_LABELS, field_matches
from kitaru_model_swap.decision_plan import TOOL_QUESTION, DecisionPlan, build_state
from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.html import render_section
from kitaru_model_swap.params import (
    Candidate,
    DecisionCandidate,
    DecisionJudge,
    ModelSwapParams,
)
from kitaru_model_swap.pricing import ResolvedPrice, call_cost
from kitaru_model_swap.runner import (
    CallResult,
    DecisionResult,
    NotRunReason,
    decision_tokens,
    describe,
)

REPORT_METADATA_KEY = "kitaru.model_swap/v1"
REPORT_SECTION_KEY = "kitaru.report_section/v1"
SUMMARY_INSIGHT_NAME = "model-swap-summary"
_EXCERPT = 600
_REVIEW_ORDER: dict[bool | None, int] = {False: 0, None: 1, True: 2}
_TEXT_GROUP = "text answer"
_SAME_TOOL = frozenset({"same_tool_same_args", "same_tool_different_args"})
# Names come from imported traces, so lists of them are bounded.
_MAX_LISTED = 50
# A candidate failing more often than this is a setup problem, not a choice.
_MAX_FAILURE_RATE = 0.2
# Candidates this close to the best match count as behaving the same.
_COMPARABLE_POINTS = 0.1
_NOISY_REFERENCE = 0.5
_DAYS_PER_MONTH = 30
# Spans shorter than this are too short to extrapolate traffic from.
_MIN_OBSERVED_DAYS = 1


@dataclass(frozen=True)
class Volume:
    """Observed traffic used to project costs."""

    sessions: int
    calls_per_session: float
    sessions_per_month: float | None
    source: str | None


def get_volume(collection: Collection, params: ModelSwapParams) -> Volume:
    """Derive calls per session and monthly sessions from the import."""
    calls_per_session = collection.llm_calls_seen / max(collection.sessions, 1)
    if params.sessions_per_month is not None:
        return Volume(
            collection.sessions, calls_per_session, params.sessions_per_month, "params"
        )
    if collection.first_started and collection.last_started:
        days = (
            collection.last_started - collection.first_started
        ).total_seconds() / 86400
        if days >= _MIN_OBSERVED_DAYS:
            return Volume(
                collection.sessions,
                calls_per_session,
                collection.sessions / days * _DAYS_PER_MONTH,
                "observed",
            )
    return Volume(collection.sessions, calls_per_session, None, None)


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
) -> dict[str, Any]:
    """Build the JSON report for a completed run or a dry run."""
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
        _add_summaries(candidates)
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
    mix = None if params.dry_run else _best_mix(candidates, sampled, volume)
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
        "decision_shapes": _decision_shapes(plans),
        "decision_candidates": [
            _decision_candidate_report(
                candidate,
                plans,
                [r for r in decision_results or [] if r.candidate == candidate.name],
                prices.get(candidate.name),
                dry_run=params.dry_run,
            )
            for candidate in params.decision_candidates
        ],
        "best_for": [] if params.dry_run else _best_for(candidates) + _mix_row(mix),
        "best_mix": mix,
        "excluded_origins": dict(params.excluded_origins),
        "disagreements": kept,
        "disagreements_total": len(disagreements),
        "spent_usd": _usd(spent),
        "max_spend_usd": _usd(params.max_spend_usd),
        "stopped_early": stopped_early,
        "judge": _judge_label(params),
        "notes": _notes(coverage)
        + (
            [
                "Per-session and monthly costs assume each candidate takes the "
                "same steps as the recording. A model that needs more turns, "
                "retries, or tool calls to finish a task costs more per task; "
                "a full replay of the agent measures that."
            ]
            if params.candidates and not params.dry_run
            else []
        ),
    }


def _coverage(collection: Collection, sampled: list[RecordedCall]) -> dict[str, Any]:
    """Count what was seen, replayable, sampled and skipped."""
    inferred = collection.replayable["inferred"]
    return {
        "sessions": collection.sessions,
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
            f"{without} sampled calls have no system prompt in their recording. "
            "If the agent uses one, the trace source or importer did not keep "
            "it, and candidates answer without it."
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
        "cost": _projection(per_call, volume),
    }


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
                "cached_input_per_mtok": _str(price.price.cached_input_per_mtok),
                "source": price.source,
            }
        ),
    }
    if dry_run:
        max_tokens = candidate.provider_settings.max_tokens
        estimates = (
            []
            if price is None
            else [(c, call_cost(price, estimate_usage(c, max_tokens))) for c in sampled]
        )
        report["estimate"] = {
            "calls": len(sampled) * repeats,
            "spend_usd": _usd(
                sum((cost for _, cost in estimates), Decimal(0)) * repeats
                if price is not None
                else None
            ),
            "cost": _projection(weighted_per_call(estimates, weights), volume),
        }
        return report

    results = [r for r in attempts if r.attempt == 0]
    sent = [r for r in results if r.reply is not None]
    answered = [r for r in sent if r.agreement is not None]
    every_sent = [r for r in attempts if r.reply is not None]
    headline = [r for r in answered if r.call.request.schema_source != "inferred"]
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
    report |= {
        "calls_sent": len(every_sent),
        "calls_not_run": dict(Counter(r.not_run for r in attempts if r.not_run)),
        "consistency": _consistency(attempts, repeats),
        "answered": len(headline),
        "agreement": dict(Counter(r.agreement for r in headline).most_common()),
        "agreement_rate": _rate(agreeing, len(headline)),
        "tool_choice_rate": _rate(same_tool, len(tool_calls)),
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
        "failure_rate": _rate(sum(failures.values()), len(every_sent)),
        "by_tool": _by_tool(headline),
        "latency_seconds": {
            "p50": _round(statistics.median(latencies)) if latencies else None,
            "p95": _round(_percentile(latencies, 0.95)) if latencies else None,
        },
        "tokens_per_call": _mean_tokens([reply.usage for reply in replies]),
        "retries": sum(reply.attempts - 1 for reply in replies),
        "cost": _projection(weighted_per_call(answered_costs, weights), volume),
        # A candidate with no answered call of some kind has no cost for it,
        # so its per-call cost covers only the kinds it answered.
        "cost_complete": {(c.node_name, c.model) for c in sampled}
        <= {(call.node_name, call.model) for call, _ in answered_costs},
        "spent_usd": _usd(sum(spent, Decimal(0)) if spent else None),
    }
    return report


def _by_tool(results: list[CallResult]) -> dict[str, Any]:
    """Agreement and per-argument agreement grouped by recorded tool."""
    groups: dict[str, dict[str, Any]] = {}
    for result in results:
        assert result.reply is not None
        group = groups.setdefault(
            _group(result.call),
            {
                "calls": 0,
                "agreeing": 0,
                "same_tool": 0,
                "costs": [],
                "fields": defaultdict(lambda: [0, 0]),
            },
        )
        group["calls"] += 1
        group["agreeing"] += result.agreement in AGREEING
        group["same_tool"] += result.agreement in _SAME_TOOL
        if result.cost is not None:
            group["costs"].append(result.cost)
        for tool, fields in field_matches(
            result.call.request.recorded, result.reply
        ).items():
            for name, matched in fields.items():
                counts = group["fields"][f"{tool}.{name}"]
                counts[0] += matched
                counts[1] += 1
    return {
        name: {
            "calls": group["calls"],
            "agreement_rate": _rate(group["agreeing"], group["calls"]),
            "tool_choice_rate": (
                None
                if name == _TEXT_GROUP
                else _rate(group["same_tool"], group["calls"])
            ),
            "cost_per_call_usd": _mean_usd(group["costs"]),
            "fields": {
                field: _rate(matched, total)
                for field, (matched, total) in sorted(group["fields"].items())
            },
        }
        for name, group in sorted(groups.items())
    }


def _group(call: RecordedCall) -> str:
    """Group a call by the tools it recorded, or as a text answer."""
    names = sorted({c.name for c in call.request.recorded.tool_calls})
    return " + ".join(names) if names else _TEXT_GROUP


def _primary_rate(candidate: dict[str, Any]) -> tuple[str, float | None]:
    """Pick the comparison that is meaningful for this candidate's calls.

    Tool arguments often hold free text that never matches exactly, so calls
    that use tools are judged on tool choice; text-only calls on answers.
    """
    if candidate["tool_choice_rate"] is not None:
        return "picks the same tool", candidate["tool_choice_rate"]
    return "gives the same answer", candidate["agreement_rate"]


def _add_summaries(candidates: list[dict[str, Any]]) -> None:
    """Attach a plain-language, factual summary to each candidate."""
    reference = next((c for c in candidates if c["reference"]), None)
    noise = _primary_rate(reference)[1] if reference else None
    for candidate in candidates:
        what, rate = _primary_rate(candidate)
        if candidate["failure_rate"] is not None and candidate["failure_rate"] > (
            _MAX_FAILURE_RATE
        ):
            top = next(iter(candidate["failures"]), "unknown")
            text = (
                f"Fix the provider setup first: {candidate['failure_rate']:.0%} "
                f"of calls failed, mostly {top}. Failures are not counted as "
                "disagreement."
            )
        elif rate is None:
            text = "No answered calls to compare."
        elif candidate["reference"]:
            text = (
                f"Your current model, run again: {what} as its own recording "
                f"{rate:.0%} of the time. This is how much answers vary from run "
                "to run."
            )
        else:
            text = f"{what.capitalize()} as your current model {rate:.0%} of the time."
            if noise is not None and noise < _NOISY_REFERENCE:
                text += (
                    f" Your current model matches itself only {noise:.0%} of the "
                    "time, so read per-argument agreement and your verdicts "
                    "rather than this rate."
                )
        candidate["summary"] = text


def _best_for(candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Name the best candidate for each goal, by simple, stated rules.

    Quality here is closeness to the current model's recorded answers; the
    user's verdicts on the disagreements decide which answers are right.
    """
    usable = [
        c
        for c in candidates
        if not c["excluded"]
        and _primary_rate(c)[1] is not None
        and (c["failure_rate"] is None or c["failure_rate"] <= _MAX_FAILURE_RATE)
    ]
    if not usable:
        return []
    closest = max(usable, key=lambda c: _primary_rate(c)[1] or 0)
    floor = (_primary_rate(closest)[1] or 0) - _COMPARABLE_POINTS
    comparable = [c for c in usable if (_primary_rate(c)[1] or 0) >= floor]
    choices = [
        {
            "goal": "Closest to today",
            "candidate": closest["name"],
            "why": f"{_primary_rate(closest)[0].capitalize()} as your current model "
            f"{_pct(_primary_rate(closest)[1])} of the time.",
        }
    ]
    priced = [
        c
        for c in comparable
        if c["cost"]["per_call_usd"] is not None and c["cost_complete"]
    ]
    if priced:
        cheapest = min(priced, key=lambda c: Decimal(c["cost"]["per_call_usd"]))
        choices.append(
            {
                "goal": "Cheapest",
                "candidate": cheapest["name"],
                "why": f"${cheapest['cost']['per_call_usd']} per call, within "
                f"{_COMPARABLE_POINTS * 100:.0f} points of the closest match.",
            }
        )
    timed = [c for c in comparable if c["latency_seconds"]["p50"] is not None]
    if timed:
        fastest = min(timed, key=lambda c: c["latency_seconds"]["p50"])
        choices.append(
            {
                "goal": "Fastest",
                "candidate": fastest["name"],
                "why": f"{fastest['latency_seconds']['p50']:.2f}s median latency, "
                f"within {_COMPARABLE_POINTS * 100:.0f} points of the closest match.",
            }
        )
    open_models = [c for c in usable if c["open_weights"]]
    if open_models:
        best_open = max(open_models, key=lambda c: _primary_rate(c)[1] or 0)
        choices.append(
            {
                "goal": "Best open model",
                "candidate": best_open["name"],
                "why": f"{_primary_rate(best_open)[0].capitalize()} as your current "
                "model "
                f"{_pct(_primary_rate(best_open)[1])} of the time, and you can "
                "run it anywhere.",
            }
        )
    return choices


def _disagreement(result: CallResult) -> dict[str, Any]:
    """Describe one disagreeing call for review."""
    assert result.reply is not None and result.agreement is not None
    return {
        "candidate": result.candidate,
        "session_id": str(result.call.session_id),
        "node_id": str(result.call.node_id),
        "node_name": result.call.node_name,
        "group": _group(result.call),
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


def _projection(per_call: Decimal | None, volume: Volume) -> dict[str, str | None]:
    """Project a per-call cost to sessions and months."""
    if per_call is None:
        return {"per_call_usd": None, "per_session_usd": None, "per_month_usd": None}
    per_session = per_call * Decimal(str(volume.calls_per_session))
    per_month = (
        None
        if volume.sessions_per_month is None
        else per_session * Decimal(str(volume.sessions_per_month))
    )
    return {
        "per_call_usd": _usd(per_call),
        "per_session_usd": _usd(per_session),
        "per_month_usd": _usd(per_month),
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
        f"model calls from {coverage['sessions']} sessions. "
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
        lines.extend(
            f"- **{choice['goal']}:** {choice['candidate']}. {choice['why']}"
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
                f" | {_pct(_primary_rate(candidate)[1])}"
                f" | {_pct(candidate['failure_rate'])}"
                f" | {candidate['cost']['per_call_usd'] or 'unknown'}"
                f" | {'n/a' if p50 is None else f'{p50:.2f}s'}"
                f" | {_yes_no(candidate['open_weights'])} |"
            )
        lines.append("")
        lines.extend(f"- **{c['name']}**: {c['summary']}" for c in report["candidates"])
    mix = report.get("best_mix")
    if mix:
        lines += ["", "Best mix:"]
        for item in mix["groups"]:
            use = item["candidate"] or "today's model"
            lines.append(f"- {item['group']} ({_pct(item['share'])} of calls): {use}")
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
                f"{_pct(candidate['tool_choice_rate'])} of the time and matched "
                f"every decision {_pct(candidate['decisions_match_rate'])} of the "
                f"time, at ${candidate['cost_per_call_usd'] or 'unknown'} per call."
            )
    if report["stopped_early"]:
        lines += ["", f"Stopped early: {report['stopped_early']}."]
    if report["notes"]:
        lines += ["", *(f"- {note}" for note in report["notes"])]
    return "\n".join(lines)


def _decision_shapes(plans: list[tuple[RecordedCall, DecisionPlan]]) -> dict[str, Any]:
    """Count which sampled calls a decision model could make, with their cost."""
    shapes = Counter(plan.shape for _, plan in plans)
    costs: dict[str, Decimal] = defaultdict(Decimal)
    for call, plan in plans:
        costs[plan.shape] += call.cost or Decimal(0)
    total = sum(costs.values(), Decimal(0))
    by_group: dict[str, Counter[str]] = defaultdict(Counter)
    for call, plan in plans:
        by_group[call.node_name][plan.shape] += 1
    return {
        "calls": len(plans),
        "decision": shapes["decision"],
        "partial": shapes["partial"],
        "generative": shapes["generative"],
        "recorded_cost_share": {
            shape: _rate_decimal(costs[shape], total)
            for shape in ("decision", "partial", "generative")
        },
        "reasons": dict(
            Counter(plan.reason for _, plan in plans if plan.reason).most_common(10)
        ),
        "by_node": {
            name[:80]: dict(counts)
            for name, counts in sorted(by_group.items())[:_MAX_LISTED]
        },
    }


def _decision_candidate_report(
    candidate: DecisionCandidate,
    plans: list[tuple[RecordedCall, DecisionPlan]],
    results: list[DecisionResult],
    price: ResolvedPrice | None,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    """Aggregate one decision candidate's answers against the recording."""
    report: dict[str, Any] = {
        "name": candidate.name,
        "kind": candidate.kind,
        "model": candidate.model,
        "price": None if price is None else str(price.price.input_per_mtok),
    }
    asked = [(call, plan) for call, plan in plans if plan.questions]
    if dry_run:
        tokens = sum(
            decision_tokens(candidate, build_state(call.request), plan.questions)
            for call, plan in asked
        )
        report["estimate"] = {
            "calls": len(asked),
            "spend_usd": None
            if price is None
            else _usd(call_cost(price, Usage(input_tokens=tokens))),
        }
        return report
    answered = [r for r in results if r.answers is not None]
    failures = Counter(r.failure for r in results if r.failure)
    tool_right = sum(
        r.answers.get(TOOL_QUESTION) == r.plan.expected.get(TOOL_QUESTION)
        for r in answered
        if r.answers is not None and r.plan.expected
    )
    fields: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    all_right = 0
    for r in answered:
        assert r.answers is not None and r.plan.expected is not None
        matches = {
            key: r.answers.get(key) == value for key, value in r.plan.expected.items()
        }
        all_right += all(matches.values())
        for key, matched in matches.items():
            if key != TOOL_QUESTION:
                name = (r.plan.arguments or {}).get(key, key)
                fields[name][0] += matched
                fields[name][1] += 1
    decision_calls = [r for r in answered if r.plan.shape == "decision"]
    costs = [r.cost for r in answered if r.cost is not None]
    # Partial calls still need an LLM for the rest, so savings are only
    # compared on calls that are decisions throughout.
    decision_costs = [r.cost for r in decision_calls if r.cost is not None]
    recorded_decision = [r.call.cost for r in decision_calls if r.call.cost is not None]
    latencies = sorted(
        r.latency_seconds for r in answered if r.latency_seconds is not None
    )
    report |= {
        "calls_asked": len(results) - sum(r.not_run is not None for r in results),
        "calls_not_run": dict(Counter(r.not_run for r in results if r.not_run)),
        "answered": len(answered),
        "failures": dict(failures.most_common()),
        "tool_choice_rate": _rate(tool_right, len(answered)),
        "argument_rates": {
            name: _rate(matched, total)
            for name, (matched, total) in sorted(fields.items())[:_MAX_LISTED]
        },
        "decisions_match_rate": _rate(all_right, len(answered)),
        "decision_calls": len(decision_calls),
        "decision_calls_match_rate": _rate(
            sum(
                all(
                    r.answers is not None and r.answers.get(k) == v
                    for k, v in (r.plan.expected or {}).items()
                )
                for r in decision_calls
            ),
            len(decision_calls),
        ),
        "cost_per_call_usd": _usd(sum(costs, Decimal(0)) / len(costs))
        if costs
        else None,
        "decision_call_cost_usd": _mean_usd(decision_costs),
        "recorded_decision_call_cost_usd": _mean_usd(recorded_decision),
        "latency_p50_seconds": _round(statistics.median(latencies))
        if latencies
        else None,
    }
    return report


def _mean_usd(costs: list[Decimal]) -> str | None:
    return _usd(sum(costs, Decimal(0)) / len(costs)) if costs else None


def _rate_decimal(part: Decimal, whole: Decimal) -> float | None:
    return round(float(part / whole), 4) if whole else None


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
        "any_match_rate": _rate(
            sum(any(r.agreement in AGREEING for r in g) for g in complete),
            len(complete),
        ),
        "all_match_rate": _rate(
            sum(all(r.agreement in AGREEING for r in g) for g in complete),
            len(complete),
        ),
    }


def _best_mix(
    candidates: list[dict[str, Any]],
    sampled: list[RecordedCall],
    volume: Volume,
) -> dict[str, Any] | None:
    """Pick, per kind of call, the cheapest candidate that behaves well enough.

    A kind of call moves to a candidate only when its rate there is within
    10 points of the reference's rate there (or of the best rate when there
    is no reference); otherwise it stays on today's model. Excluded, failing
    and unpriced candidates are never picked.
    """
    shares = Counter(_group(call) for call in sampled)
    recorded_costs: dict[str, list[Decimal]] = defaultdict(list)
    for call in sampled:
        if call.cost is not None:
            recorded_costs[_group(call)].append(call.cost)
    # The reference is today's model run again, so it sets the bar but is not
    # somewhere to move calls to.
    usable = [
        c
        for c in candidates
        if not c["excluded"]
        and not c["reference"]
        and (c["failure_rate"] is None or c["failure_rate"] <= _MAX_FAILURE_RATE)
    ]
    reference = next((c for c in candidates if c["reference"]), None)
    rows: list[
        tuple[str, float, str | None, float | None, Decimal | None, Decimal | None]
    ] = []
    for group, count in sorted(shares.items()):
        today = _mean_decimal(recorded_costs[group])
        rates: dict[str, float] = {}
        costs: dict[str, Decimal] = {}
        for c in usable:
            rate = _group_rate(c, group)
            cost = c["by_tool"].get(group, {}).get("cost_per_call_usd")
            if rate is not None and cost is not None:
                rates[c["name"]], costs[c["name"]] = rate, Decimal(cost)
        bar = _group_rate(reference, group)
        if bar is None and rates:
            bar = max(rates.values())
        fits = (
            [name for name, rate in rates.items() if rate >= bar - _COMPARABLE_POINTS]
            if bar is not None
            else []
        )
        choice = min(fits, key=lambda name: costs[name], default=None)
        # Keep today's model when it is cheaper than every candidate that fits.
        if choice is not None and today is not None and costs[choice] > today:
            choice = None
        rows.append(
            (
                group,
                count / len(sampled),
                choice,
                None if choice is None else rates[choice],
                costs[choice] if choice else today,
                today,
            )
        )
    if not any(choice for _, _, choice, _, _, _ in rows):
        return None
    groups = [
        {
            "group": group,
            "share": round(share, 4),
            "candidate": choice,
            "rate": rate,
            "cost_per_call_usd": _usd(cost),
        }
        for group, share, choice, rate, cost, _ in rows
    ]
    if any(cost is None or today is None for _, _, _, _, cost, today in rows):
        return {"groups": groups, "cost": None, "recorded_cost": None}
    mixed = sum(
        (
            (cost or Decimal(0)) * Decimal(str(share))
            for _, share, _, _, cost, _ in rows
        ),
        Decimal(0),
    )
    recorded = sum(
        (
            (today or Decimal(0)) * Decimal(str(share))
            for _, share, _, _, _, today in rows
        ),
        Decimal(0),
    )
    return {
        "groups": groups,
        "cost": _projection(mixed, volume),
        "recorded_cost": _projection(recorded, volume),
    }


def _mix_row(mix: dict[str, Any] | None) -> list[dict[str, str]]:
    """Summarize the best mix as one "best for" row."""
    if mix is None:
        return []
    moved = [item for item in mix["groups"] if item["candidate"]]
    targets = sorted({item["candidate"] for item in moved})
    why = (
        f"Moves {len(moved)} of {len(mix['groups'])} kinds of call "
        f"({_pct(sum(item['share'] for item in moved))} of calls) to "
        f"{', '.join(targets)} and keeps the rest on today's model"
    )
    if mix["cost"] is not None and mix["recorded_cost"] is not None:
        why += (
            f": ${mix['cost']['per_call_usd']} per call against "
            f"${mix['recorded_cost']['per_call_usd']} today"
        )
    return [{"goal": "Best mix", "candidate": " + ".join(targets), "why": why + "."}]


def _group_rate(candidate: dict[str, Any] | None, group: str) -> float | None:
    """The meaningful rate for one kind of call: tool choice, or the answer."""
    if candidate is None:
        return None
    values = candidate["by_tool"].get(group)
    if values is None:
        return None
    rate = values["tool_choice_rate"]
    return values["agreement_rate"] if rate is None else rate


def _mean_decimal(costs: list[Decimal]) -> Decimal | None:
    return sum(costs, Decimal(0)) / len(costs) if costs else None


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


def _rate(part: int, whole: int) -> float | None:
    return round(part / whole, 4) if whole else None


def _yes_no(value: bool | None) -> str:
    return "unknown" if value is None else ("yes" if value else "no")


def _pct(rate: float | None) -> str:
    return "n/a" if rate is None else f"{rate:.0%}"


def _usd(value: Decimal | None) -> str | None:
    return None if value is None else format(value.quantize(Decimal("1e-8")), "f")


def _str(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _round(value: float) -> float:
    return round(value, 3)


def _percentile(values: list[float], fraction: float) -> float:
    return values[min(len(values) - 1, int(fraction * len(values)))]
