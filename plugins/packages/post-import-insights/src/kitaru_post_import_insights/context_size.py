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
"""Find model input that grows within sessions or far exceeds the usual call."""

import uuid

from kitaru.api_models.v1.insight import Bin, BinnedInsightData
from kitaru_post_import_insights.findings import (
    CandidateCoverage,
    CandidateFinding,
    ContributionSelector,
    DeterministicFact,
    build_binned,
    calculate_percent,
    format_briefing,
    pluralize,
)
from kitaru_post_import_insights.models import EvidenceLocator
from kitaru_post_import_insights.profiling_state import (
    HighestValueNodes,
    Histogram,
    Observation,
    ObservationStore,
    SessionReferences,
)
from kitaru_post_import_insights.resource_inputs import (
    FindingLimits,
    SessionView,
    build_references,
    format_count,
    get_locators,
    select_evidence,
)

# Tripling the input between the first and last call is growth an agent
# designer did not intend for a steady task.
MIN_GROWTH_RATIO = 3
# Growth below this size costs little and rarely approaches context limits.
MIN_GROWN_INPUT_TOKENS = 4_000
# A handful of sessions, and a tenth of eligible ones, make growth a pattern.
MIN_GROWING_SESSIONS = 3
MIN_GROWING_SHARE_PERCENT = 10
GROWTH_RATIO_BOUNDS = (1.5, 2, 3, 5, 10)
# A median over fewer calls moves with any single call.
MIN_SIZED_CALLS = 20
# Five times the median is far outside the usual call, and the token floor
# keeps small agents from flagging calls that are large only in relative terms.
OUTLIER_MEDIAN_MULTIPLE = 5
MIN_OUTLIER_TOKENS = 10_000
_INPUT_CAVEAT = (
    "Input counts cache reads too when a call reports more cache reads than input "
    "tokens, because that source counts them separately."
)
_SIZE_NAMESPACE = "model-input-size"
_FIRST_NAMESPACE = "growth-first-input"
_LAST_NAMESPACE = "growth-last-input"
GROWTH_SIGNAL = "context-growth"
OUTLIER_SIGNAL = "context-outlier-call"


class ContextSize:
    """Accumulate model input sizes for growth and outlier findings."""

    def __init__(self, store: ObservationStore, limits: FindingLimits) -> None:
        """Initialize empty accumulators backed by a shared observation store."""
        self._store = store
        self._limits = limits
        # One growth ratio per session with at least two sized model calls.
        self._ratios = Histogram(GROWTH_RATIO_BOUNDS)
        self._growing = SessionReferences(limits.contributors)
        self._highest_growth = HighestValueNodes(limits.evidence)
        self._sessions_with_model_calls = 0

    def consume(self, view: SessionView) -> None:
        """Record each model call's input size and the session's growth."""
        session_id = view.session.id
        sized = [
            (call.node.id, tokens)
            for call in view.model_calls
            if (tokens := call.prompt_tokens) is not None
        ]
        if view.model_calls:
            self._sessions_with_model_calls += 1
        for node_id, tokens in sized:
            self._store.add(
                _SIZE_NAMESPACE,
                Observation(value=tokens, session=str(session_id), node=str(node_id)),
            )
        if len(sized) < 2 or sized[0][1] <= 0:
            return
        first, (last_node_id, last) = sized[0][1], sized[-1]
        ratio = last / first
        self._ratios.add(ratio)
        if ratio < MIN_GROWTH_RATIO or last < MIN_GROWN_INPUT_TOKENS:
            return
        self._growing.add(session_id)
        self._highest_growth.add(session_id, last_node_id, ratio)
        for namespace, value in ((_FIRST_NAMESPACE, first), (_LAST_NAMESPACE, last)):
            self._store.add(
                namespace, Observation(value=value, session=str(session_id))
            )

    def build(self, select: ContributionSelector) -> list[CandidateFinding]:
        """Return the growth and outlier candidates that meet their thresholds."""
        return [
            candidate
            for candidate in (self._build_growth(select), self._build_outliers(select))
            if candidate is not None
        ]

    def _build_growth(self, select: ContributionSelector) -> CandidateFinding | None:
        """Build the context-growth candidate."""
        growing = len(self._growing)
        eligible = len(self._ratios)
        if (
            growing < MIN_GROWING_SESSIONS
            or growing * 100 < MIN_GROWING_SHARE_PERCENT * eligible
        ):
            return None
        median_first = self._store.get_median(_FIRST_NAMESPACE)
        median_last = self._store.get_median(_LAST_NAMESPACE)
        share = calculate_percent(growing, eligible)
        evidence = [
            EvidenceLocator(
                session_id=session_id, node_id=node_id, signal=GROWTH_SIGNAL
            )
            for session_id, node_id in self._highest_growth.get_nodes()
        ]
        contributions, evidence = select_evidence(select, self._growing, evidence)
        excluded = self._sessions_with_model_calls - eligible
        caveat = (
            "Growth compares the first and last model call with an input token count "
            f"in each session and counts only sessions whose last call has at least "
            f"{format_count(MIN_GROWN_INPUT_TOKENS)} tokens. {_INPUT_CAVEAT}"
        )
        if excluded:
            caveat += (
                " "
                + pluralize(
                    excluded,
                    "session with model calls had fewer than two calls with input "
                    "token counts and was",
                    "sessions with model calls had fewer than two calls with input "
                    "token counts and were",
                )
                + " left out."
            )
        return CandidateFinding(
            id="context-growth",
            family="context",
            rank=75,
            eyebrow="CONTEXT GROWTH",
            title=(
                f"In {growing} sessions, model input grew at least {MIN_GROWTH_RATIO} "
                "times from the first call to the last (median "
                f"{format_count(median_first)} to {format_count(median_last)} tokens)"
            ),
            fallback_description=(
                "Model input keeps growing within these sessions. Check what each "
                "call adds to the context, such as full tool results or repeated "
                "history."
            ),
            caveat=caveat,
            data=build_binned(self._ratios, unit="times"),
            facts=[
                DeterministicFact(name="growing_sessions", value=growing),
                DeterministicFact(
                    name="sessions_with_repeated_model_calls", value=eligible
                ),
                DeterministicFact(name="growing_share_percent", value=share),
                DeterministicFact(name="median_first_call_tokens", value=median_first),
                DeterministicFact(name="median_last_call_tokens", value=median_last),
                DeterministicFact(name="maximum", value=round(self._ratios.maximum, 2)),
            ],
            coverage=CandidateCoverage(
                sessions_analyzed=eligible,
                affected_sessions=growing,
                occurrences=growing,
                evidence_available=growing,
                evidence_retained=len(evidence),
                contributing_sessions_available=growing,
                contributing_sessions_retained=len(contributions),
            ),
            contributing_session_ids=contributions,
            evidence=evidence,
            investigation_prompt=format_briefing(
                odd=(
                    f"In {growing} of the {eligible} sessions with at least two sized "
                    f"model calls ({share}%), the last call's input is at least "
                    f"{MIN_GROWTH_RATIO} times the first call's and at least "
                    f"{format_count(MIN_GROWN_INPUT_TOKENS)} tokens. Among them the "
                    f"median first call has {format_count(median_first)} input "
                    f"tokens and the median last call {format_count(median_last)}."
                ),
                look_first=(
                    "Open the sessions in evidence_locators, ordered by growth; each "
                    "locator points at the session's last model call. Read the inputs "
                    "of a few calls in order and note what each turn adds: tool "
                    "results, retrieved documents, or repeated conversation history."
                ),
                cohort=(
                    f"the {growing} sessions whose input grew at least "
                    f"{MIN_GROWTH_RATIO} times, versus sessions with several model "
                    "calls and flatter input."
                ),
                hypothesis=(
                    "Check whether one kind of content, such as raw tool output, "
                    "makes up most of the growth. If it does, trimming or summarizing "
                    "that content before the next call is the single change to "
                    "replay."
                ),
                held=(
                    "the replayed cohort's median last-call input falls below "
                    f"{format_count(median_last)} tokens and most sessions grow less "
                    f"than {MIN_GROWTH_RATIO} times, while the failed-session rate "
                    "does not rise."
                ),
            ),
        )

    def _build_outliers(self, select: ContributionSelector) -> CandidateFinding | None:
        """Build the oversized-input candidate."""
        sized_calls = self._store.count(_SIZE_NAMESPACE)
        if sized_calls < MIN_SIZED_CALLS:
            return None
        median = self._store.get_median(_SIZE_NAMESPACE)
        threshold = max(OUTLIER_MEDIAN_MULTIPLE * median, MIN_OUTLIER_TOKENS)
        outliers = self._store.count(_SIZE_NAMESPACE, minimum=threshold)
        if not outliers:
            return None
        affected = self._store.count_sessions(_SIZE_NAMESPACE, minimum=threshold)
        at_least_median = self._store.count(_SIZE_NAMESPACE, minimum=median)
        rows: list[Observation] = []
        session_ids: dict[uuid.UUID, None] = {}
        # Each row adds at most one session and evidence never outnumbers
        # contributors, so a full contributor list also fills the evidence.
        for item in self._store.iter_highest(_SIZE_NAMESPACE, minimum=threshold):
            if len(rows) < self._limits.evidence:
                rows.append(item)
            session_ids.setdefault(uuid.UUID(item.session))
            if len(session_ids) == self._limits.contributors:
                break
        largest = rows[0].value
        contributions, evidence = select_evidence(
            select,
            build_references(
                session_ids, available=affected, limit=self._limits.contributors
            ),
            get_locators(rows, OUTLIER_SIGNAL),
        )
        calls = pluralize(outliers, "model call")
        return CandidateFinding(
            id="context-outliers",
            family="context",
            rank=85,
            eyebrow="OVERSIZED MODEL INPUTS",
            title=(
                f"{calls} had at least {format_count(threshold)} input tokens, "
                f"against a median of {format_count(median)}"
            ),
            fallback_description=(
                "A few model calls carry far more input than this agent's typical "
                "call. Open them to see what filled the context."
            ),
            caveat=(
                "An oversized call has at least "
                f"{OUTLIER_MEDIAN_MULTIPLE} times the median input token count of "
                "this agent's model calls, and at least "
                f"{format_count(MIN_OUTLIER_TOKENS)} tokens. {_INPUT_CAVEAT}"
            ),
            data=BinnedInsightData(
                unit="tokens",
                bins=[
                    Bin(upper_bound=median, count=sized_calls - at_least_median),
                    Bin(
                        lower_bound=median,
                        upper_bound=threshold,
                        count=at_least_median - outliers,
                    ),
                    Bin(lower_bound=threshold, count=outliers),
                ],
            ),
            facts=[
                DeterministicFact(name="oversized_calls", value=outliers),
                DeterministicFact(
                    name="model_calls_with_input_tokens", value=sized_calls
                ),
                DeterministicFact(name="median_input_tokens", value=median),
                DeterministicFact(name="oversized_threshold_tokens", value=threshold),
                DeterministicFact(name="largest_input_tokens", value=largest),
                DeterministicFact(name="sessions_with_oversized_calls", value=affected),
            ],
            coverage=CandidateCoverage(
                sessions_analyzed=self._store.count_sessions(_SIZE_NAMESPACE),
                affected_sessions=affected,
                occurrences=outliers,
                evidence_available=outliers,
                evidence_retained=len(evidence),
                contributing_sessions_available=affected,
                contributing_sessions_retained=len(contributions),
            ),
            contributing_session_ids=contributions,
            evidence=evidence,
            investigation_prompt=format_briefing(
                odd=(
                    f"{calls} across {pluralize(affected, 'session')} had at least "
                    f"{format_count(threshold)} input tokens. The median of the "
                    f"{format_count(sized_calls)} model calls with an input count is "
                    f"{format_count(median)}, and the largest call has "
                    f"{format_count(largest)}."
                ),
                look_first=(
                    "Open the nodes in evidence_locators, largest first, and read "
                    "their inputs. Note which part is large: a tool result, a "
                    "retrieved document, an attached file, or a long history."
                ),
                cohort=(
                    f"the {affected} sessions containing at least one call with "
                    f"{format_count(threshold)} or more input tokens."
                ),
                hypothesis=(
                    "Check whether the oversized inputs share one source, such as one "
                    "tool whose raw output goes into the next prompt. If they do, "
                    "truncating or summarizing that source is the single change to "
                    "replay."
                ),
                held=(
                    f"far fewer replayed calls reach {format_count(threshold)} input "
                    "tokens, while the failed-session rate does not rise."
                ),
            ),
        )
