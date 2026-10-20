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

The full report is stored under `REPORT_METADATA_KEY` on the summary insight
so the HTML report command can render it without calling any model.
"""

import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit

from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    CategoryValue,
    InsightInput,
    TextInsightData,
)
from kitaru_model_swap.calls import Collection, RecordedCall, estimate_usage
from kitaru_model_swap.compare import AGREEING, Agreement, field_matches
from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.params import Candidate, ModelSwapParams
from kitaru_model_swap.pricing import ResolvedPrice, call_cost
from kitaru_model_swap.runner import CallResult, NotRunReason, describe

REPORT_METADATA_KEY = "kitaru.model_swap/v1"
SUMMARY_INSIGHT_NAME = "model-swap-summary"

AGREEMENT_LABELS: dict[Agreement, str] = {
    "same_tool_same_args": "Same tool and arguments",
    "same_tool_different_args": "Same tool, different arguments",
    "different_tool": "Different tool",
    "text_instead_of_tool": "Text instead of a tool call",
    "same_text": "Same answer",
    "different_text": "Different answer",
    "tool_instead_of_text": "Tool call instead of an answer",
}
_EXCERPT = 600
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
) -> dict[str, Any]:
    """Build the JSON report for a completed run or a dry run."""
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
            dry_run=params.dry_run,
        )
        for candidate in params.candidates
    ]
    _add_recommendations(candidates)
    disagreements = [
        _disagreement(result)
        for result in results
        if result.agreement is not None and result.agreement not in AGREEING
    ]
    disagreements.sort(key=lambda item: (item["group"], item["candidate"]))
    return {
        "schema": 1,
        "dry_run": params.dry_run,
        "coverage": _coverage(collection, sampled),
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
        "recorded": _recorded_report(sampled, volume),
        "candidates": candidates,
        "disagreements": disagreements[: params.max_disagreements],
        "disagreements_total": len(disagreements),
        "spent_usd": _usd(spent),
        "max_spend_usd": _usd(params.max_spend_usd),
        "stopped_early": stopped_early,
        "judge": params.judge.name if params.judge else None,
        "notes": _notes(collection, sampled),
    }


def _coverage(collection: Collection, sampled: list[RecordedCall]) -> dict[str, Any]:
    """Count what was seen, replayable, sampled and skipped."""
    replayable = collection.calls
    return {
        "sessions": collection.sessions,
        "llm_calls_seen": collection.llm_calls_seen,
        "replayable": sum(c.request.schema_source != "inferred" for c in replayable),
        "replayable_inferred": sum(
            c.request.schema_source == "inferred" for c in replayable
        ),
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
    }


def _notes(collection: Collection, sampled: list[RecordedCall]) -> list[str]:
    """Explain anything that changes how the numbers should be read."""
    notes = []
    dropped = sum(c.request.dropped_reasoning_parts > 0 for c in sampled)
    if dropped:
        notes.append(
            f"{dropped} sampled calls had recorded reasoning in their history. "
            "Reasoning is not sent to candidates, so they see less context "
            "than the recorded model did."
        )
    inferred = sum(c.request.schema_source == "inferred" for c in sampled)
    if inferred:
        notes.append(
            f"{inferred} sampled calls use tool schemas inferred from recorded "
            "arguments. They are reported separately and left out of the "
            "headline agreement."
        )
    if collection.skipped.get("missing_tool_schema"):
        notes.append(
            f"{collection.skipped['missing_tool_schema']} calls were skipped "
            "because a tool they could use has no schema. Pass `tool_schemas` "
            "from the agent's code to include them."
        )
    return notes


def _recorded_report(sampled: list[RecordedCall], volume: Volume) -> dict[str, Any]:
    """Summarize the recorded model and its cost on the sampled calls."""
    costs = [c.cost for c in sampled if c.cost is not None]
    per_call = sum(costs, Decimal(0)) / len(costs) if costs else None
    return {
        "models": dict(Counter(c.model or "unknown" for c in sampled).most_common()),
        "tokens_per_call": _mean_tokens([c.usage for c in sampled]),
        "cost": _projection(per_call, volume),
    }


def _candidate_report(
    candidate: Candidate,
    sampled: list[RecordedCall],
    results: list[CallResult],
    price: ResolvedPrice | None,
    volume: Volume,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    """Aggregate one candidate's results."""
    report: dict[str, Any] = {
        "name": candidate.name,
        "model": candidate.model,
        "endpoint_host": urlsplit(candidate.base_url).netloc,
        "reference": candidate.reference,
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
        estimates = [
            call_cost(
                price.price,
                estimate_usage(call, candidate.provider_settings.max_tokens),
            )
            for call in sampled
            if price is not None
        ]
        total = sum(estimates, Decimal(0)) if price is not None else None
        report["estimate"] = {
            "calls": len(sampled),
            "spend_usd": _usd(total),
            "cost": _projection(
                None if total is None or not sampled else total / len(sampled),
                volume,
            ),
        }
        return report

    sent = [r for r in results if r.reply is not None]
    answered = [r for r in sent if r.agreement is not None]
    headline = [r for r in answered if r.call.request.schema_source != "inferred"]
    inferred = [r for r in answered if r.call.request.schema_source == "inferred"]
    failures = Counter(
        r.reply.failure for r in sent if r.reply and r.reply.failure is not None
    )
    tool_calls = [r for r in headline if r.call.request.recorded.tool_calls]
    agreeing = sum(r.agreement in AGREEING for r in headline)
    same_tool = sum(
        r.agreement in {"same_tool_same_args", "same_tool_different_args"}
        for r in tool_calls
    )
    costs = [r.cost for r in sent if r.cost is not None]
    latencies = sorted(r.reply.latency_seconds for r in sent if r.reply)
    report |= {
        "calls_sent": len(sent),
        "calls_not_run": dict(Counter(r.not_run for r in results if r.not_run)),
        "answered": len(headline),
        "agreement": dict(Counter(r.agreement for r in headline).most_common()),
        "agreement_rate": _rate(agreeing, len(headline)),
        "tool_choice_rate": _rate(same_tool, len(tool_calls)),
        "judged_equivalent": sum(r.judge_equivalent is True for r in headline),
        "inferred_schema_agreement": dict(Counter(r.agreement for r in inferred)),
        "failures": dict(failures.most_common()),
        "failure_rate": _rate(sum(failures.values()), len(sent)),
        "by_tool": _by_tool(headline),
        "latency_seconds": {
            "p50": _round(statistics.median(latencies)) if latencies else None,
            "p95": _round(_percentile(latencies, 0.95)) if latencies else None,
        },
        "tokens_per_call": _mean_tokens([r.reply.usage for r in sent if r.reply]),
        "retries": sum(r.reply.attempts - 1 for r in sent if r.reply),
        "cost": _projection(
            sum(costs, Decimal(0)) / len(costs) if costs else None, volume
        ),
        "spent_usd": _usd(sum(costs, Decimal(0)) if costs else None),
    }
    return report


def _by_tool(results: list[CallResult]) -> dict[str, Any]:
    """Agreement and per-argument agreement grouped by recorded tool."""
    groups: dict[str, dict[str, Any]] = {}
    for result in results:
        assert result.reply is not None
        group = groups.setdefault(
            _group(result.call),
            {"calls": 0, "agreeing": 0, "fields": defaultdict(lambda: [0, 0])},
        )
        group["calls"] += 1
        group["agreeing"] += result.agreement in AGREEING
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
    return " + ".join(names) if names else "text answer"


def _primary_rate(candidate: dict[str, Any]) -> tuple[str, float | None]:
    """Pick the comparison that is meaningful for this candidate's calls.

    Tool arguments often hold free text that never matches exactly, so calls
    that use tools are judged on tool choice; text-only calls on answers.
    """
    if candidate["tool_choice_rate"] is not None:
        return "picks the same tool", candidate["tool_choice_rate"]
    return "gives the same answer", candidate["agreement_rate"]


def _add_recommendations(candidates: list[dict[str, Any]]) -> None:
    """Attach a plain-language, rule-based recommendation to each candidate."""
    reference = next((c for c in candidates if c["reference"]), None)
    for candidate in candidates:
        if "estimate" in candidate:
            continue
        what, rate = _primary_rate(candidate)
        noise = _primary_rate(reference)[1] if reference else None
        if candidate["reference"]:
            text = (
                f"Reference: {what} as its own recording {_pct(rate)} of the "
                "time. Read other candidates against this number."
            )
        elif candidate["failure_rate"] is not None and candidate["failure_rate"] > 0.2:
            top = next(iter(candidate["failures"]), "unknown")
            text = (
                f"Fix the provider setup first: {candidate['failure_rate']:.0%} "
                f"of calls failed, mostly {top}. Failures are not counted as "
                "disagreement."
            )
        elif rate is None:
            text = "No answered calls to judge."
        elif noise is not None and rate >= noise - 0.05:
            text = (
                f"{what.capitalize()} as the recording {rate:.0%} of the time, "
                f"within 5 points of the reference's {noise:.0%}. Review its "
                "disagreements before switching."
            )
        else:
            text = (
                f"{what.capitalize()} as the recording {rate:.0%} of the time"
                + (f", below the reference's {noise:.0%}" if noise is not None else "")
                + ". Review the disagreements to decide which calls could move."
            )
        if noise is not None and noise < 0.5 and not candidate["reference"]:
            text += (
                f" The reference itself agrees only {noise:.0%} of the time, so "
                "these calls vary from run to run; use per-argument agreement "
                "or a judge rather than this rate."
            )
        candidate["recommendation"] = text


def _disagreement(result: CallResult) -> dict[str, Any]:
    """Describe one disagreeing call for review."""
    assert result.reply is not None and result.agreement is not None
    recorded = result.call.request.recorded
    return {
        "candidate": result.candidate,
        "session_id": str(result.call.session_id),
        "node_id": str(result.call.node_id),
        "node_name": result.call.node_name,
        "group": _group(result.call),
        "agreement": result.agreement,
        "schema_source": result.call.request.schema_source,
        "recorded": describe(recorded.tool_calls, recorded.text)[:_EXCERPT],
        "candidate_answer": describe(result.reply.tool_calls, result.reply.text)[
            :_EXCERPT
        ],
        "judge_equivalent": result.judge_equivalent,
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


def build_insights(report: dict[str, Any]) -> list[InsightInput]:
    """Turn a report into insight cards; the summary carries the full report."""
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
    cost_values = _cost_values(report)
    if cost_values:
        unit = (
            "USD per month"
            if report["volume"]["sessions_per_month"] is not None
            else "USD per session"
        )
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
                    description=candidate.get("recommendation"),
                    data=CategoricalInsightData(
                        unit="calls",
                        values=[
                            CategoryValue(label=AGREEMENT_LABELS[label], value=count)
                            for label, count in candidate["agreement"].items()
                        ],
                    ),
                )
            )
    insights.append(
        InsightInput(
            name="model-swap-failures",
            title="Provider failures",
            description="Failed requests, counted apart from disagreement.",
            data=CategoricalInsightData(
                unit="calls",
                values=[
                    CategoryValue(label=f"{candidate['name']}: {category}", value=count)
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
    recorded = report["recorded"]["cost"][key]
    if recorded is not None and not report["dry_run"]:
        values.append(CategoryValue(label="Recorded", value=float(recorded)))
    for candidate in report["candidates"]:
        if report["dry_run"]:
            amount = candidate["estimate"]["spend_usd"]
        else:
            amount = candidate["cost"][key]
        if amount is not None:
            values.append(CategoryValue(label=candidate["name"], value=float(amount)))
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
    if report["dry_run"]:
        lines.append("| Candidate | Calls | Estimated spend (USD) |")
        lines.append("|---|---|---|")
        for candidate in report["candidates"]:
            estimate = candidate["estimate"]
            lines.append(
                f"| {candidate['name']} | {estimate['calls']} | "
                f"{estimate['spend_usd'] or 'unknown price'} |"
            )
    else:
        lines.append(
            "| Candidate | Agreement | Tool choice | Failures | Cost per call (USD) |"
        )
        lines.append("|---|---|---|---|---|")
        for candidate in report["candidates"]:
            label = candidate["name"] + (
                " (reference)" if candidate["reference"] else ""
            )
            lines.append(
                f"| {label}"
                f" | {_pct(candidate['agreement_rate'])}"
                f" | {_pct(candidate['tool_choice_rate'])}"
                f" | {_pct(candidate['failure_rate'])}"
                f" | {candidate['cost']['per_call_usd'] or 'unknown'} |"
            )
        lines.append("")
        lines.extend(
            f"- **{c['name']}**: {c['recommendation']}" for c in report["candidates"]
        )
    if report["stopped_early"]:
        lines += ["", f"Stopped early: {report['stopped_early']}."]
    if report["notes"]:
        lines += ["", *(f"- {note}" for note in report["notes"])]
    return "\n".join(lines)


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
