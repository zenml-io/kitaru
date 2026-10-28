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
"""Report sections for decision-shaped calls and decision-model candidates."""

import statistics
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any

from kitaru_model_swap.calls import RecordedCall
from kitaru_model_swap.decision_plan import TOOL_QUESTION, DecisionPlan, build_state
from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.params import DecisionCandidate
from kitaru_model_swap.pricing import ResolvedPrice, call_cost
from kitaru_model_swap.runner import (
    DecisionResult,
    decision_tokens,
)
from kitaru_model_swap.stats import (
    compute_rate,
    format_usd,
    round_rate,
)

# Names come from imported traces, so lists of them are bounded.
_MAX_LISTED = 50


def get_decision_shapes(
    plans: list[tuple[RecordedCall, DecisionPlan]],
) -> dict[str, Any]:
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


def build_decision_candidate_report(
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
            else format_usd(call_cost(price, Usage(input_tokens=tokens))),
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
        "tool_choice_rate": compute_rate(tool_right, len(answered)),
        "argument_rates": {
            name: compute_rate(matched, total)
            for name, (matched, total) in sorted(fields.items())[:_MAX_LISTED]
        },
        "decisions_match_rate": compute_rate(all_right, len(answered)),
        "decision_calls": len(decision_calls),
        "decision_calls_match_rate": compute_rate(
            sum(
                all(
                    r.answers is not None and r.answers.get(k) == v
                    for k, v in (r.plan.expected or {}).items()
                )
                for r in decision_calls
            ),
            len(decision_calls),
        ),
        "cost_per_call_usd": format_usd(sum(costs, Decimal(0)) / len(costs))
        if costs
        else None,
        "decision_call_cost_usd": _mean_usd(decision_costs),
        "recorded_decision_call_cost_usd": _mean_usd(recorded_decision),
        "latency_p50_seconds": round_rate(statistics.median(latencies))
        if latencies
        else None,
    }
    return report


def _mean_usd(costs: list[Decimal]) -> str | None:
    return format_usd(sum(costs, Decimal(0)) / len(costs)) if costs else None


def _rate_decimal(part: Decimal, whole: Decimal) -> float | None:
    return round(float(part / whole), 4) if whole else None
