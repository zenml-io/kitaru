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

import hashlib
import math
import re
import statistics
from collections import Counter, defaultdict
from collections.abc import Callable
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
_TEXT_REPLY = frozenset({"same_text", "different_text"})
# Names come from imported traces, so lists of them are bounded.
_MAX_LISTED = 50
# A candidate failing more often than this is a setup problem, not a choice.
_MAX_FAILURE_RATE = 0.2
# Candidates this close to the best match count as behaving the same.
_COMPARABLE_POINTS = 0.1
# A candidate must answer this share of its calls to be ranked.
_MIN_COVERAGE = 0.8
_NOISY_REFERENCE = 0.5
# A candidate matching today's model less than this share as often as the
# closest candidate does is a clear failure, not worth a full replay.
_CLEAR_FAILURE_RATIO = 0.5
# A judge that changes this share of its rulings when only the order of the
# answers changes is too unstable to lean on.
_UNSTABLE_JUDGE = 0.2
_MIN_JUDGE_RECHECKS = 5
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
        _add_paired_rates(candidates, by_candidate)
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
    weight = population_weight(sampled, collection.group_seen)
    mix = (
        None
        if params.dry_run
        else _best_mix(
            candidates,
            sampled,
            volume,
            params.min_mix_calls,
            _split_routes(by_candidate, sampled, weight),
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
        "cost": _projection(per_call, volume),
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
            "spend_usd": _usd(
                sum((cost for _, cost in estimates), Decimal(0)) * repeats
                if price is not None
                else None
            ),
            "cost": _projection(weighted_per_call(estimates, weights), volume),
        }
        return report

    results = [r for r in attempts if r.attempt == 0]
    weight = population_weight(sampled, weights)
    sent = [r for r in results if r.reply is not None]
    answered = [r for r in sent if r.agreement is not None]
    every_sent = [r for r in attempts if r.reply is not None]
    headline = _get_headline(attempts)
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
    step_results = _get_step_results(attempts)
    report |= {
        "coverage": {"answered": len(headline), "attempted": len(step_results)},
        "measured": _is_measured(len(headline), len(step_results)),
        "calls_sent": len(every_sent),
        "calls_not_run": dict(Counter(r.not_run for r in attempts if r.not_run)),
        "consistency": _consistency(attempts, repeats),
        "answered": len(headline),
        "agreement": dict(Counter(r.agreement for r in headline).most_common()),
        "agreement_rate": _rate(agreeing, len(headline)),
        "agreement_interval": _interval(agreeing, len(headline)),
        "tool_choice_rate": _rate(same_tool, len(tool_calls)),
        "tool_choice_interval": _interval(same_tool, len(tool_calls)),
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
        "failure_rate": _rate(sum(failures.values()), len(every_sent)),
        "by_tool": _by_group(step_results, _group, weight),
        "by_route": _by_group(step_results, _route_key(sampled), weight),
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


def _get_step_results(attempts: list[CallResult]) -> list[CallResult]:
    """First attempts on calls with known tool schemas, answered or not."""
    return [
        r
        for r in attempts
        if r.attempt == 0 and r.call.request.schema_source != "inferred"
    ]


def _by_group(
    results: list[CallResult],
    key: Callable[[RecordedCall], str],
    weight: Callable[[RecordedCall], Decimal] | None = None,
) -> dict[str, Any]:
    """Agreement, reliability and per-argument agreement grouped by `key`.

    `calls` counts answered calls, which the rates are computed on;
    `attempted` counts every call, of which `failed` got a provider failure
    and `not_run` was never sent because a budget ran out. `failure_rate` is
    failures among the calls that were sent.

    `match_rate` asks whether the candidate took the same kind of action: the
    same tools, or a text answer where the recording answered in text. Exact
    wording almost never matches, so it would make every text step look
    unmovable, or, against a reference that does not match itself, movable to
    anything. It is not semantic agreement: any two text answers match.

    `cost_per_call_usd` weights each call by `weight`, the recorded traffic
    it stands for, so a group's cost is not skewed by balanced sampling.
    """
    groups: dict[str, dict[str, Any]] = {}
    for result in results:
        group = groups.setdefault(
            key(result.call),
            {
                "attempted": 0,
                "failed": 0,
                "not_run": 0,
                "calls": 0,
                "agreeing": 0,
                "same_tool": 0,
                "matching": 0,
                "costs": [],
                "fields": defaultdict(lambda: [0, 0]),
            },
        )
        group["attempted"] += 1
        if result.reply is None:
            group["not_run"] += 1
            continue
        if result.agreement is None:
            group["failed"] += result.reply.failure is not None
            continue
        group["calls"] += 1
        group["agreeing"] += result.agreement in AGREEING
        group["same_tool"] += result.agreement in _SAME_TOOL
        group["matching"] += result.agreement in (
            _SAME_TOOL if result.call.request.recorded.tool_calls else _TEXT_REPLY
        )
        if result.cost is not None:
            group["costs"].append(
                (result.cost, weight(result.call) if weight else Decimal(1))
            )
        for tool, fields in field_matches(
            result.call.request.recorded, result.reply
        ).items():
            for name, matched in fields.items():
                counts = group["fields"][f"{tool}.{name}"]
                counts[0] += matched
                counts[1] += 1
    return {
        name: {
            "attempted": group["attempted"],
            "calls": group["calls"],
            "failed": group["failed"],
            "not_run": group["not_run"],
            "failure_rate": _rate(group["failed"], group["calls"] + group["failed"]),
            "agreement_rate": _rate(group["agreeing"], group["calls"]),
            "tool_choice_rate": (
                None
                if name == _TEXT_GROUP
                else _rate(group["same_tool"], group["calls"])
            ),
            "match_rate": _rate(group["matching"], group["calls"]),
            "cost_per_call_usd": _usd(_weighted_mean(group["costs"])),
            "fields": {
                field: _rate(matched, total)
                for field, (matched, total) in sorted(group["fields"].items())
            },
        }
        for name, group in sorted(groups.items())
    }


def _weighted_mean(values: list[tuple[Decimal, Decimal]]) -> Decimal | None:
    """Mean of `(value, weight)` pairs, or None without any weight."""
    total = sum((w for _, w in values), Decimal(0))
    if not total:
        return None
    return sum((v * w for v, w in values), Decimal(0)) / total


def _group(call: RecordedCall) -> str:
    """Group a call by the tools it recorded, or as a text answer."""
    names = sorted({c.name for c in call.request.recorded.tool_calls})
    return " + ".join(names) if names else _TEXT_GROUP


def _route_key(sampled: list[RecordedCall]) -> Callable[[RecordedCall], str]:
    """Key calls by the step they answer, named by what a router sees first.

    The node name is added when the sample holds more than one.
    """
    several = len({call.node_name for call in sampled}) > 1
    return lambda call: (f"{call.node_name}: " if several else "") + _route(call)


def _route(call: RecordedCall) -> str:
    """Name a call by what came just before it.

    The tool the recorded model ends up calling is only known after the call,
    so it cannot route one; the request's last messages are known before it.
    """
    messages = [
        m for m in call.request.messages if m.get("role") not in {"system", "developer"}
    ]
    if not any(m.get("role") == "assistant" for m in messages):
        return "first turn"
    last = messages[-1]
    if last.get("role") == "tool":
        for message in reversed(messages):
            calls = (
                message.get("tool_calls")
                if message.get("role") == "assistant"
                else None
            )
            if calls:
                names = sorted(
                    {str(_as_dict(c.get("function")).get("name")) for c in calls}
                )
                return "after " + " + ".join(names)
    if last.get("role") == "user":
        return "after a user message"
    return "after an assistant message"


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _primary_rate(candidate: dict[str, Any]) -> tuple[str, float | None]:
    """Pick the comparison that is meaningful for this candidate's calls.

    Tool arguments often hold free text that never matches exactly, so calls
    that use tools are judged on tool choice; text-only calls on answers.
    """
    if candidate["tool_choice_rate"] is not None:
        return "picks the same tool", candidate["tool_choice_rate"]
    return "gives the same answer", candidate["agreement_rate"]


def _is_measured(answered: int, attempted: int) -> bool:
    """Tell whether a candidate answered enough of its calls to be ranked.

    A candidate the budget stopped early answered only the first calls, so a
    rate over them neither covers the sample nor can be compared with a
    candidate that answered all of it.
    """
    return attempted > 0 and answered >= math.ceil(_MIN_COVERAGE * attempted)


def _hits(result: CallResult, tool_choice: bool) -> bool:
    """Whether one answer counts as a match under the candidate's headline rate."""
    if tool_choice:
        return result.agreement in _SAME_TOOL
    return result.agreement in AGREEING


def _add_paired_rates(
    candidates: list[dict[str, Any]], by_candidate: dict[str, list[CallResult]]
) -> None:
    """Compare each candidate with the reference on the calls both answered.

    Rates over different calls are not comparable when one candidate answered
    only some of them, so equivalence is judged on the common calls.
    """
    reference = next((c for c in candidates if c["reference"]), None)
    if reference is None:
        return
    ref_answers = {
        r.call.node_id: r for r in _get_headline(by_candidate[reference["name"]])
    }
    for candidate in candidates:
        if candidate["reference"]:
            continue
        tool_choice = candidate["tool_choice_rate"] is not None
        pairs = [
            (r, ref_answers[r.call.node_id])
            for r in _get_headline(by_candidate[candidate["name"]])
            if r.call.node_id in ref_answers
            and (not tool_choice or r.call.request.recorded.tool_calls)
        ]
        candidate["paired"] = {
            "calls": len(pairs),
            "rate": _rate(sum(_hits(r, tool_choice) for r, _ in pairs), len(pairs)),
            "reference_rate": _rate(
                sum(_hits(ref, tool_choice) for _, ref in pairs), len(pairs)
            ),
        }


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
    user's verdicts on the disagreements decide which answers are right. The
    current model run again sets the bar: a candidate counts as comparable
    within a few points of how often today's model matches itself, measured
    on the calls both answered. Only candidates that answered most of their
    calls are ranked; the rest are named as needing more measurement.
    """
    usable = [
        c
        for c in candidates
        if not c["excluded"]
        and _primary_rate(c)[1] is not None
        and (c["failure_rate"] is None or c["failure_rate"] <= _MAX_FAILURE_RATE)
    ]
    reference = next((c for c in usable if c["reference"]), None)
    others = [c for c in usable if not c["reference"] and c["measured"]]
    # Candidates failing too often get "fix the provider setup" instead.
    unmeasured = [
        c
        for c in candidates
        if not c["excluded"]
        and not c["reference"]
        and not c["measured"]
        and (c["failure_rate"] is None or c["failure_rate"] <= _MAX_FAILURE_RATE)
    ]
    choices = _unmeasured_row(unmeasured)
    if not others:
        return choices

    def rate(candidate: dict[str, Any]) -> float:
        return _primary_rate(candidate)[1] or 0

    closest = max(others, key=rate)
    bar = rate(reference) if reference is not None else rate(closest)

    def is_comparable(candidate: dict[str, Any]) -> bool:
        paired = candidate.get("paired")
        if reference is None or not paired or paired["rate"] is None:
            return rate(candidate) >= bar - _COMPARABLE_POINTS
        return paired["rate"] >= paired["reference_rate"] - _COMPARABLE_POINTS

    # When today's model does not take the same action as its own recording,
    # agreement cannot tell an equivalent candidate from a different one; and
    # when it answered too few calls, it sets no bar at all.
    unbarred = reference is not None and not reference["measured"]
    noisy = reference is not None and (unbarred or bar < _NOISY_REFERENCE)
    comparable = [] if noisy else [c for c in others if is_comparable(c)]
    choices = [
        {
            "goal": "Closest to today",
            "candidate": closest["name"],
            "why": f"{_primary_rate(closest)[0].capitalize()} as your current model "
            f"{_pct(rate(closest))} of the time{_answered(closest)}"
            + (
                f"; your current model run again does {_pct(bar)}."
                if reference is not None
                else "."
            ),
        },
        *choices,
    ]
    priced = [
        c
        for c in comparable
        if c["cost"]["per_call_usd"] is not None and c["cost_complete"]
    ]
    if priced:
        cheapest = min(priced, key=lambda c: Decimal(c["cost"]["per_call_usd"]))
        today = (reference or {}).get("cost", {}).get("per_call_usd")
        if (
            reference is not None
            and today is not None
            and Decimal(today) <= Decimal(cheapest["cost"]["per_call_usd"])
        ):
            choices.append(
                {
                    "goal": "Cheapest",
                    "candidate": reference["name"],
                    "why": f"${today} per call: no comparable candidate costs less "
                    "than your current model.",
                }
            )
        else:
            choices.append(
                {
                    "goal": "Cheapest",
                    "candidate": cheapest["name"],
                    "why": f"${cheapest['cost']['per_call_usd']} per call, within "
                    f"{_COMPARABLE_POINTS * 100:.0f} points of how often your "
                    f"current model matches itself{_answered(cheapest)}.",
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
                f"within {_COMPARABLE_POINTS * 100:.0f} points of how often your "
                f"current model matches itself{_answered(fastest)}.",
            }
        )
    open_models = [c for c in others if c["open_weights"]]
    if open_models:
        best_open = max(open_models, key=rate)
        choices.append(
            {
                "goal": "Best open model",
                "candidate": best_open["name"],
                "why": f"{_primary_rate(best_open)[0].capitalize()} as your current "
                f"model {_pct(rate(best_open))} of the time{_answered(best_open)}, "
                "and you can run it anywhere.",
            }
        )
    # Step agreement ranks a cheap model low when it reaches the same outcome by
    # a different route, so only clear failures are dropped and the cheapest
    # candidate is always kept.
    cutoff = rate(closest) * _CLEAR_FAILURE_RATIO
    finalists = sorted((c for c in others if rate(c) >= cutoff), key=rate, reverse=True)
    priced_others = [
        c
        for c in others
        if c["cost"]["per_call_usd"] is not None and c["cost_complete"]
    ]
    cheapest_overall = min(
        priced_others,
        key=lambda c: Decimal(c["cost"]["per_call_usd"]),
        default=None,
    )
    if cheapest_overall is not None and cheapest_overall not in finalists:
        finalists.append(cheapest_overall)
    dropped = [c["name"] for c in others if c not in finalists]
    if unbarred:
        reason = (
            "Your current model run again answered too few of its calls to "
            "set a bar, so step agreement cannot show which candidate is "
            "equivalent."
        )
    elif noisy:
        reason = (
            f"Your current model matches its own recording only {_pct(bar)} of "
            "the time, so step agreement cannot show which candidate is "
            "equivalent."
        )
    elif not comparable:
        reason = (
            f"No candidate comes within {_COMPARABLE_POINTS * 100:.0f} points of "
            f"how often your current model matches itself ({_pct(bar)}), so step "
            "agreement cannot name an equivalent."
        )
    else:
        reason = "Step agreement ranks candidates but does not show outcomes."
    left_out = (
        f" Left out as clear failures, matching today's model less than half as "
        f"often as the closest candidate: {', '.join(dropped)}."
        if dropped
        else ""
    )
    choices.append(
        {
            "goal": "Worth a full replay",
            "candidate": " + ".join(c["name"] for c in finalists),
            "why": f"{reason} Replay these end to end with your evaluator before "
            "switching: every candidate except clear failures, plus the "
            "cheapest, which step agreement often ranks low even when it keeps "
            f"outcomes.{left_out}",
        }
    )
    return choices


def _answered(candidate: dict[str, Any]) -> str:
    """Say how many of its calls a candidate answered."""
    coverage = candidate["coverage"]
    return f" (answered {coverage['answered']} of {coverage['attempted']} calls)"


def _unmeasured_row(unmeasured: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Name candidates that answered too few calls to be ranked or ruled out."""
    if not unmeasured:
        return []
    return [
        {
            "goal": "Needs more measurement",
            "candidate": " + ".join(c["name"] for c in unmeasured),
            "why": "Answered too few of their calls to be ranked or ruled out: "
            + ", ".join(
                f"{c['name']} {c['coverage']['answered']} of "
                f"{c['coverage']['attempted']}"
                for c in unmeasured
            )
            + ". Raise the spend cap or time budget and run again before "
            "keeping or dropping them.",
        }
    ]


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
        lines.append(
            "Stage 1 of 2: this screen compares single recorded calls, so it rules "
            "candidates out but does not pick a winner. Replay the candidates worth "
            "a full replay end to end, graded on outcomes, to decide."
        )
        lines.append("")
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


def _get_headline(attempts: list[CallResult]) -> list[CallResult]:
    """First attempts that were answered, on calls with known tool schemas."""
    return [
        r
        for r in attempts
        if r.attempt == 0
        and r.agreement is not None
        and r.call.request.schema_source != "inferred"
    ]


def _is_learning_session(call: RecordedCall) -> bool:
    """Put about half of the sessions in the learning set, by session id."""
    return int(hashlib.sha256(str(call.session_id).encode()).hexdigest(), 16) % 2 == 0


def _split_routes(
    by_candidate: dict[str, list[CallResult]],
    sampled: list[RecordedCall],
    weight: Callable[[RecordedCall], Decimal],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Per-step results of each candidate on the learning and held-out sessions.

    Sessions, not calls, are split, so calls from one conversation never land
    on both sides.
    """
    key = _route_key(sampled)
    learning: dict[str, dict[str, Any]] = {}
    held_out: dict[str, dict[str, Any]] = {}
    for name, results in by_candidate.items():
        halves: tuple[list[CallResult], list[CallResult]] = ([], [])
        for r in _get_step_results(results):
            halves[0 if _is_learning_session(r.call) else 1].append(r)
        learning[name] = _by_group(halves[0], key, weight)
        held_out[name] = _by_group(halves[1], key, weight)
    return learning, held_out


def _is_unreliable(values: dict[str, Any] | None) -> bool:
    """Tell whether a candidate failed too often on one step.

    Agreement among the replies that came back says nothing about how often
    the provider failed to reply, so a step is judged on its own failures,
    not on the candidate's overall rate, which other steps can dilute.
    """
    rate = None if values is None else values["failure_rate"]
    return rate is not None and rate > _MAX_FAILURE_RATE


def _best_mix(
    candidates: list[dict[str, Any]],
    sampled: list[RecordedCall],
    volume: Volume,
    min_calls: int,
    split: tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]],
    weight: Callable[[RecordedCall], Decimal],
) -> dict[str, Any] | None:
    """Pick, per step, the cheapest candidate that behaves well enough.

    Steps are named by what comes just before a call (`_route`), which a
    router can see. Picking the best of many candidates on many steps finds
    some by luck, so each step is chosen on the learning half of the sessions
    and checked on the held-out half. A step moves to a candidate only when
    the candidate answered at least `min_calls` of the step's calls, failed on
    no more than a fifth of the step's calls it was sent, and its rate on the
    learning half is within 10 points of the reference's rate there (or of the
    best rate when there is no reference). A pick whose held-out rate, on
    enough held-out calls, falls more than 10 points below the reference's
    held-out rate (or the learning bar), or that fails too often on the
    held-out calls, is dropped (`held_out: failed`); with too few held-out
    calls it stays but is marked `too_few`. Otherwise the step stays on
    today's model, as does a step where the reference itself takes the
    recorded action less than half the time. "Rate" is whether the candidate
    takes the same kind of action (the same tools, or a text answer), not
    exact wording or meaning. Today's cost is the reference's replay cost
    where the reference answered the step, since recorded calls often hit a
    warm prompt cache that replayed single calls do not, and the recorded
    cost elsewhere; each step says which (`today_source`). Shares and costs
    weight each sampled call by the recorded traffic it stands for, so a
    mix that picks one candidate everywhere costs what that candidate costs.
    Excluded, failing and unpriced candidates are never picked.
    """
    key = _route_key(sampled)
    population: dict[str, Decimal] = defaultdict(Decimal)
    counts = Counter(key(call) for call in sampled)
    recorded_costs: dict[str, list[tuple[Decimal, Decimal]]] = defaultdict(list)
    for call in sampled:
        population[key(call)] += weight(call)
        if call.cost is not None:
            recorded_costs[key(call)].append((call.cost, weight(call)))
    total_population = sum(population.values(), Decimal(0))
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
    groups: list[dict[str, Any]] = []
    # (share, cost per call with the mix, cost per call today) per step
    prices: list[tuple[Decimal, Decimal | None, Decimal | None]] = []
    learning, held_out = split
    sessions = {
        "learning": len({c.session_id for c in sampled if _is_learning_session(c)}),
        "held_out": len({c.session_id for c in sampled if not _is_learning_session(c)}),
    }
    for group, count in sorted(counts.items()):
        ref_all = reference["by_route"].get(group) if reference else None
        # A step seen in only one half of the sessions cannot be split; it is
        # judged on all its calls and its pick marked as not checked.
        ref_values = (
            learning.get(reference["name"], {}).get(group) or ref_all
            if reference
            else None
        )
        ref_held = held_out.get(reference["name"], {}).get(group) if reference else None
        if ref_all and ref_all["cost_per_call_usd"] is not None:
            today: Decimal | None = Decimal(ref_all["cost_per_call_usd"])
            today_source: str | None = "reference replay"
        else:
            today = _weighted_mean(recorded_costs[group])
            today_source = None if today is None else "recording"
        rates: dict[str, float] = {}
        costs: dict[str, Decimal] = {}
        unsplit: set[str] = set()
        unreliable: list[str] = []
        too_few = False
        for c in usable:
            values = learning.get(c["name"], {}).get(group)
            overall = c["by_route"].get(group)
            if _is_unreliable(overall):
                unreliable.append(c["name"])
                continue
            if not overall or overall["cost_per_call_usd"] is None:
                continue
            if overall["calls"] < min_calls:
                too_few = True
                continue
            if not values or not values["calls"]:
                unsplit.add(c["name"])
                values = overall
            rates[c["name"]] = values["match_rate"]
            costs[c["name"]] = Decimal(overall["cost_per_call_usd"])
        bar = ref_values["match_rate"] if ref_values else None
        if bar is None and rates:
            bar = max(rates.values())
        # When today's model does not take the same action as its own
        # recording, agreement cannot tell candidates apart on this step.
        noisy = bar is not None and bar < _NOISY_REFERENCE
        if noisy:
            bar = None
        fits = (
            [name for name, rate in rates.items() if rate >= bar - _COMPARABLE_POINTS]
            if bar is not None
            else []
        )
        choice = min(fits, key=lambda name: costs[name], default=None)
        # Keep today's model when it is cheaper than every candidate that fits.
        if choice is not None and today is not None and costs[choice] > today:
            choice = None
        held_rate, held_calls, held_status = None, 0, None
        if choice is not None and bar is not None:
            held = held_out.get(choice, {}).get(group)
            held_bar = ref_held["match_rate"] if ref_held else bar
            held_calls = held["calls"] if held else 0
            held_rate = held["match_rate"] if held else None
            if choice in unsplit:
                held_status = "no_split"
            elif _is_unreliable(held):
                choice, held_status = None, "failed"
            elif held_rate is None or held_calls < max(1, min_calls // 4):
                held_status = "too_few"
            elif held_rate < held_bar - _COMPARABLE_POINTS:
                choice, held_status = None, "failed"
            else:
                held_status = "held"
        cost = costs[choice] if choice else today
        share = population[group] / total_population if total_population else None
        groups.append(
            {
                "group": group,
                "share": None if share is None else round(float(share), 4),
                "calls": count,
                "candidate": choice,
                "rate": None if choice is None else rates[choice],
                "cost_per_call_usd": _usd(cost),
                "too_few_calls": choice is None and too_few and not rates,
                "noisy_reference": noisy,
                "unreliable": sorted(unreliable),
                "held_out": held_status,
                "held_out_rate": held_rate,
                "held_out_calls": held_calls,
                "today_source": today_source,
            }
        )
        prices.append((share or Decimal(0), cost, today))
    # A pick that failed on held-out sessions is a result worth showing.
    if not any(item["candidate"] or item["held_out"] == "failed" for item in groups):
        return None
    summary = {
        "groups": groups,
        "min_calls": min_calls,
        "today_source": _describe_sources(groups),
        "sessions": sessions,
        "cost": None,
        "recorded_cost": None,
    }
    if any(cost is None or today is None for _, cost, today in prices):
        return summary
    mixed = sum((share * (cost or 0) for share, cost, _ in prices), Decimal(0))
    recorded = sum((share * (today or 0) for share, _, today in prices), Decimal(0))
    return summary | {
        "cost": _projection(mixed, volume),
        "recorded_cost": _projection(recorded, volume),
    }


def _describe_sources(groups: list[dict[str, Any]]) -> str:
    """Say where today's cost comes from, naming a mix of sources."""
    sources = Counter(item["today_source"] for item in groups if item["today_source"])
    if len(sources) <= 1:
        return next(iter(sources), "recording")
    replayed = sources["reference replay"]
    return (
        f"reference replay on {replayed} of {sources.total()} steps and the "
        "recording on the rest, which is not like for like"
    )


def _mix_row(mix: dict[str, Any] | None) -> list[dict[str, str]]:
    """Summarize the best mix as one "best for" row."""
    if mix is None:
        return []
    moved = [item for item in mix["groups"] if item["candidate"]]
    if not moved:
        return []
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


def _interval(part: int, whole: int, z: float = 1.96) -> list[float] | None:
    """Wilson 95% interval for a rate, so small samples read as uncertain."""
    if not whole:
        return None
    p = part / whole
    center = (p + z * z / (2 * whole)) / (1 + z * z / whole)
    half = (z * math.sqrt(p * (1 - p) / whole + z * z / (4 * whole * whole))) / (
        1 + z * z / whole
    )
    return [round(max(0.0, center - half), 4), round(min(1.0, center + half), 4)]


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
