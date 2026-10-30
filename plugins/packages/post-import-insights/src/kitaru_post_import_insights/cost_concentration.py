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
"""Find how much of the recorded cost the costliest sessions carry."""

import uuid
from decimal import ROUND_HALF_UP, Decimal

from kitaru.api_models.v1.insight import CategoricalInsightData, CategoryValue
from kitaru_post_import_insights.findings import (
    CandidateCoverage,
    CandidateFinding,
    ContributionSelector,
    DeterministicFact,
    calculate_percent,
    format_briefing,
    pluralize,
)
from kitaru_post_import_insights.profiling_state import Observation, ObservationStore
from kitaru_post_import_insights.resource_inputs import (
    FindingLimits,
    SessionView,
    build_references,
    get_locators,
    select_evidence,
)

# Below 20 sessions the costliest tenth is one session, which says more about
# that session than about how cost is spread.
MIN_COST_SESSIONS = 20
# Ten percent of sessions is the slice an investigation can read in full.
COSTLIEST_SESSION_DIVISOR = 10
# Three times an even share: below that, cost is not concentrated enough to
# make the costliest sessions a better starting point than any other cohort.
MIN_COSTLIEST_SHARE_PERCENT = 30
# Sub-nanounit precision cannot change a share, and integers keep sums exact.
_COST_EXPONENT = 9
# Costs below a billion units keep their nanounits inside the store's range.
_MAX_COST_MAGNITUDE = 8
_NAMESPACE = "session-cost"
SIGNAL = "costliest-session"


def _get_cost_units(cost: Decimal | None) -> int | None:
    """Convert a recorded cost to exact integer billionths, or None if unusable."""
    if cost is None or not cost.is_finite() or cost < 0:
        return None
    if cost and cost.adjusted() > _MAX_COST_MAGNITUDE:
        return None
    return int(cost.scaleb(_COST_EXPONENT).to_integral_value(rounding=ROUND_HALF_UP))


def _ceil_divide(numerator: int, denominator: int) -> int:
    """Divide positive integers, rounding up."""
    return -(-numerator // denominator)


class CostConcentration:
    """Accumulate recorded session costs and find the costliest tenth's share."""

    def __init__(self, store: ObservationStore, limits: FindingLimits) -> None:
        """Initialize an empty accumulator backed by a shared observation store."""
        self._store = store
        self._limits = limits
        self._sessions = 0
        self._total = 0

    def consume(self, view: SessionView) -> None:
        """Record one session's cost and its costliest recorded node."""
        self._sessions += 1
        cost = _get_cost_units(view.session.cost)
        if cost is None:
            return
        self._total += cost
        priced = [
            (node_cost, node.id)
            for node in view.nodes
            if (node_cost := _get_cost_units(node.cost)) is not None
        ]
        costliest = min(priced, key=lambda item: (-item[0], item[1]), default=None)
        self._store.add(
            _NAMESPACE,
            Observation(
                value=cost,
                session=str(view.session.id),
                node="" if costliest is None else str(costliest[1]),
            ),
        )

    def build(self, select: ContributionSelector) -> list[CandidateFinding]:
        """Return the cost-concentration candidate when cost is concentrated."""
        priced_sessions = self._store.count(_NAMESPACE)
        if priced_sessions < MIN_COST_SESSIONS or self._total <= 0:
            return []
        costliest = _ceil_divide(priced_sessions, COSTLIEST_SESSION_DIVISOR)
        upper_half = _ceil_divide(priced_sessions, 2)
        costliest_total = 0
        next_total = 0
        ranked: list[Observation] = []
        for position, item in enumerate(self._store.iter_highest(_NAMESPACE)):
            if position >= upper_half:
                break
            if position < costliest:
                costliest_total += item.value
                if position < self._limits.contributors:
                    ranked.append(item)
            else:
                next_total += item.value
        if costliest_total * 100 < MIN_COSTLIEST_SHARE_PERCENT * self._total:
            return []

        share = calculate_percent(costliest_total, self._total)
        even_share = calculate_percent(costliest, priced_sessions)
        unpriced = self._sessions - priced_sessions
        contributions, evidence = select_evidence(
            select,
            build_references(
                (uuid.UUID(item.session) for item in ranked),
                available=costliest,
                limit=self._limits.contributors,
            ),
            get_locators(ranked[: self._limits.evidence], SIGNAL),
        )
        groups = [
            (f"Costliest {costliest} sessions", costliest_total),
            (f"Next {upper_half - costliest} sessions", next_total),
            (
                f"Remaining {priced_sessions - upper_half} sessions",
                self._total - costliest_total - next_total,
            ),
        ]
        caveat = (
            "Cost is each session's recorded total, in the currency and pricing the "
            "source recorded."
        )
        if unpriced:
            caveat += (
                " "
                + pluralize(
                    unpriced,
                    "analyzed session without a recorded cost was",
                    "analyzed sessions without a recorded cost were",
                )
                + " left out, for example because a model call had no price."
            )
        return [
            CandidateFinding(
                id="cost-concentration",
                family="cost",
                rank=65,
                eyebrow="COST CONCENTRATION",
                title=(
                    f"The costliest {even_share}% of sessions ({costliest} of "
                    f"{priced_sessions}) account for {share}% of recorded cost"
                ),
                fallback_description=(
                    "A small group of sessions carries a large share of the recorded "
                    "cost. Compare the costliest sessions with the rest to choose a "
                    "starting point."
                ),
                caveat=caveat,
                data=CategoricalInsightData(
                    unit="percent",
                    values=[
                        CategoryValue(
                            label=label, value=calculate_percent(value, self._total)
                        )
                        for label, value in groups
                    ],
                ),
                facts=[
                    DeterministicFact(name="sessions_with_cost", value=priced_sessions),
                    DeterministicFact(name="costliest_sessions", value=costliest),
                    DeterministicFact(name="costliest_cost_share_percent", value=share),
                    DeterministicFact(name="sessions_without_cost", value=unpriced),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=priced_sessions,
                    affected_sessions=costliest,
                    occurrences=costliest,
                    evidence_available=costliest,
                    evidence_retained=len(evidence),
                    contributing_sessions_available=costliest,
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=evidence,
                investigation_prompt=_build_briefing(
                    costliest=costliest,
                    priced_sessions=priced_sessions,
                    share=share,
                    even_share=even_share,
                    unpriced=unpriced,
                ),
            )
        ]


def _build_briefing(
    *,
    costliest: int,
    priced_sessions: int,
    share: int | float,
    even_share: int | float,
    unpriced: int,
) -> str:
    """Build the briefing for the cost-concentration finding."""
    odd = (
        f"The {costliest} costliest of the {priced_sessions} sessions with a recorded "
        f"cost ({even_share}% of them) account for {share}% of the total recorded "
        f"cost. If cost were spread evenly they would carry about {even_share}%."
    )
    if unpriced:
        odd += (
            " "
            + pluralize(
                unpriced,
                "analyzed session has no recorded cost and is",
                "analyzed sessions have no recorded cost and are",
            )
            + " not counted."
        )
    return format_briefing(
        odd=odd,
        look_first=(
            "Open the sessions in evidence_locators, costliest first; each locator "
            "points at the costliest recorded node in its session. Compare their "
            "model-call count, input token counts, and models with a few sessions "
            "from the cheaper half of the import."
        ),
        cohort=(
            f"the {costliest} costliest sessions (listed in "
            "contributing_session_ids), versus sessions below the median recorded "
            "cost."
        ),
        hypothesis=(
            "Check whether the costliest sessions cost more because they make more "
            "model calls or because each call carries more input. If one of the two "
            "accounts for most of the gap, change that one factor, such as a step "
            "limit or a smaller context per call, and replay the cohort."
        ),
        held=(
            "the replayed cohort's cost per session falls, the costliest tenth's "
            f"share of cost drops below {share}%, and the failed-session rate does "
            "not rise."
        ),
    )
