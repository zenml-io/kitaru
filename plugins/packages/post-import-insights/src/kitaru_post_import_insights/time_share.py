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
"""Find which tool or model covers most of the recorded session time."""

import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import islice

from kitaru.api_models.v1.insight import CategoricalInsightData, CategoryValue
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru_post_import_insights.findings import (
    CandidateCoverage,
    CandidateFinding,
    ContributionSelector,
    DeterministicFact,
    calculate_percent,
    format_briefing,
    pluralize,
)
from kitaru_post_import_insights.labels import UNAVAILABLE_TOOL
from kitaru_post_import_insights.profiling_state import (
    CountsStore,
    LabelCounts,
    Observation,
    ObservationStore,
)
from kitaru_post_import_insights.resource_inputs import (
    FindingLimits,
    ModelCallTokens,
    SessionView,
    build_references,
    get_locators,
    get_model_label,
    get_tool_label,
    select_evidence,
)

# Fewer timed sessions make one long session decide the result.
MIN_TIMED_SESSIONS = 5
# A component covering a quarter of all recorded time is the largest single
# lever on session duration in most agents; below that, time is spread out.
MIN_LEADING_SHARE_PERCENT = 25
_MICROSECOND = timedelta(microseconds=1)
# Eight components show where time goes and keep long tool names from pushing
# the copied prompt past its bound; the rest are combined.
CHARTED_COMPONENTS = 8
_PARTS_PER_MILLION = 1_000_000
_MODEL_SUFFIX = " (model)"
_UNAVAILABLE_MODEL = "Unavailable model"
_NAMESPACE = "component-time-share"
SIGNAL = "component-time-share"


@dataclass(frozen=True)
class _Interval:
    """One node's recorded interval, in microseconds from the session start."""

    start: int
    end: int
    node_id: uuid.UUID


def _is_aware(value: datetime) -> bool:
    """Return whether a timestamp carries a usable UTC offset."""
    return value.tzinfo is not None and value.utcoffset() is not None


def _get_session_bounds(view: SessionView) -> tuple[datetime, datetime] | None:
    """Return the session's start and end when they bound a positive duration."""
    started_at = view.session.started_at
    ended_at = view.session.ended_at
    if started_at is None or ended_at is None:
        return None
    if _is_aware(started_at) != _is_aware(ended_at) or ended_at <= started_at:
        return None
    return started_at, ended_at


def _get_interval(
    node: SessionNodeResponse, bounds: tuple[datetime, datetime]
) -> _Interval | None:
    """Clip a node's recorded interval to the session, or None without timestamps."""
    started_at, ended_at = node.started_at, node.ended_at
    if started_at is None or ended_at is None or ended_at < started_at:
        return None
    session_start, session_end = bounds
    # Aware and naive timestamps cannot be compared, so mixed ones are untimed.
    aware = _is_aware(session_start)
    if _is_aware(started_at) != aware or _is_aware(ended_at) != aware:
        return None
    start = max(started_at, session_start)
    end = min(ended_at, session_end)
    return _Interval(
        start=(start - session_start) // _MICROSECOND,
        end=max((end - session_start) // _MICROSECOND, 0),
        node_id=node.id,
    )


def _get_union_length(intervals: list[_Interval]) -> int:
    """Return the length covered by intervals, counting overlaps once."""
    covered = 0
    current_start: int | None = None
    current_end = 0
    for interval in sorted(intervals, key=lambda item: (item.start, item.end)):
        if interval.end <= interval.start:
            continue
        if current_start is None or interval.start > current_end:
            if current_start is not None:
                covered += current_end - current_start
            current_start, current_end = interval.start, interval.end
        else:
            current_end = max(current_end, interval.end)
    if current_start is not None:
        covered += current_end - current_start
    return covered


# The name already passed sanitize_label and the suffix is fixed text, so a
# component label holds nothing that check would withhold.
def _get_tool_component(node: SessionNodeResponse) -> str:
    """Name a tool component so it never shares a label with a model."""
    name = get_tool_label(node)
    return name if name == UNAVAILABLE_TOOL else f"{name} (tool)"


def _get_model_component(call: ModelCallTokens) -> str:
    """Name a model component so it never shares a label with a tool."""
    name = get_model_label(call)
    return _UNAVAILABLE_MODEL if name is None else f"{name}{_MODEL_SUFFIX}"


def _is_model_component(label: str) -> bool:
    """Return whether a component label names a model rather than a tool."""
    return label.endswith(_MODEL_SUFFIX) or label == _UNAVAILABLE_MODEL


def _to_seconds(microseconds: int) -> float:
    """Convert exact microseconds to seconds with millisecond precision."""
    return round(microseconds / 1_000_000, 3)


class ComponentTimeShare:
    """Accumulate per-component recorded time and find the dominant component."""

    def __init__(
        self, counts: CountsStore, store: ObservationStore, limits: FindingLimits
    ) -> None:
        """Initialize empty totals backed by shared count and observation stores."""
        self._covered = LabelCounts(counts, "time-covered")
        self._present = LabelCounts(counts, "time-sessions")
        self._present_duration = LabelCounts(counts, "time-present-duration")
        self._store = store
        self._limits = limits
        self._sessions = 0
        self._timed_sessions = 0
        self._total_duration = 0
        self._untimed_nodes = 0

    def consume(self, view: SessionView) -> None:
        """Add the union of each component's node intervals in one session."""
        self._sessions += 1
        bounds = _get_session_bounds(view)
        if bounds is None:
            return
        components = [(_get_tool_component(node), node) for node in view.tool_calls]
        components += [
            (_get_model_component(call), call.node) for call in view.model_calls
        ]
        intervals: defaultdict[str, list[_Interval]] = defaultdict(list)
        untimed = 0
        for component, node in components:
            interval = _get_interval(node, bounds)
            if interval is None:
                untimed += 1
            else:
                intervals[component].append(interval)
        if not intervals:
            return
        self._untimed_nodes += untimed
        duration = (bounds[1] - bounds[0]) // _MICROSECOND
        self._timed_sessions += 1
        self._total_duration += duration
        for component, items in sorted(intervals.items()):
            covered = _get_union_length(items)
            if not covered:
                continue
            longest = min(items, key=lambda item: (item.start - item.end, item.node_id))
            self._covered.add(component, covered)
            self._present.add(component)
            self._present_duration.add(component, duration)
            self._store.add(
                _NAMESPACE,
                Observation(
                    value=covered * _PARTS_PER_MILLION // duration,
                    session=str(view.session.id),
                    node=str(longest.node_id),
                    key=component,
                ),
            )

    def build(self, select: ContributionSelector) -> list[CandidateFinding]:
        """Return the time-share candidate when one component dominates."""
        if self._timed_sessions < MIN_TIMED_SESSIONS or not self._covered:
            return []
        ranked = self._covered.top(CHARTED_COMPONENTS)
        leader, leader_time = ranked[0]
        if leader_time * 100 < MIN_LEADING_SHARE_PERCENT * self._total_duration:
            return []
        present = self._present.get(leader)
        share = calculate_percent(leader_time, self._present_duration.get(leader))
        overall_share = calculate_percent(leader_time, self._total_duration)
        rows = list(
            islice(
                self._store.iter_highest(_NAMESPACE, key=leader),
                self._limits.contributors,
            )
        )
        evidence = get_locators(rows[: self._limits.evidence], SIGNAL)
        contributions, evidence = select_evidence(
            select,
            build_references(
                (uuid.UUID(item.session) for item in rows),
                available=present,
                limit=self._limits.contributors,
            ),
            evidence,
        )
        left_out = self._sessions - self._timed_sessions
        caveat = (
            "Each component's time is the union of its recorded node intervals "
            "within the session's start and end, so parallel calls are not counted "
            "twice; different components can overlap each other, and time outside "
            "any tool or model node is not attributed."
        )
        if len(self._covered) > CHARTED_COMPONENTS:
            caveat += (
                f" The chart shows the {CHARTED_COMPONENTS} components with the most "
                "recorded time and combines the rest."
            )
        if self._untimed_nodes:
            caveat += (
                " "
                + pluralize(
                    self._untimed_nodes,
                    "tool or model node without usable start and end times was",
                    "tool or model nodes without usable start and end times were",
                )
                + " left out."
            )
        if left_out:
            caveat += (
                " "
                + pluralize(
                    left_out,
                    "session without a valid duration or any timed node was",
                    "sessions without a valid duration or any timed node were",
                )
                + " left out."
            )
        return [
            CandidateFinding(
                id="component-time-share",
                family="time_share",
                rank=70,
                eyebrow="WHERE TIME GOES",
                title=(
                    f"{leader} covers {share}% of recorded session time in {present} "
                    f"of {self._timed_sessions} sessions"
                ),
                fallback_description=(
                    "Recorded tool and model time concentrates in one component. "
                    "Check whether its calls are slow, repeated, or run one after "
                    "another when they could overlap."
                ),
                caveat=caveat,
                data=CategoricalInsightData(
                    unit="seconds",
                    values=[
                        CategoryValue(label=label, value=_to_seconds(value))
                        for label, value in ranked
                    ],
                ),
                facts=[
                    DeterministicFact(
                        name="timed_sessions", value=self._timed_sessions
                    ),
                    DeterministicFact(
                        name="sessions_with_leading_component", value=present
                    ),
                    DeterministicFact(
                        name="leading_component_share_percent", value=share
                    ),
                    DeterministicFact(
                        name="leading_component_overall_share_percent",
                        value=overall_share,
                    ),
                    DeterministicFact(
                        name="nodes_without_timestamps", value=self._untimed_nodes
                    ),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=self._timed_sessions,
                    affected_sessions=present,
                    occurrences=present,
                    evidence_available=present,
                    evidence_retained=len(evidence),
                    contributing_sessions_available=present,
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=evidence,
                investigation_prompt=_build_briefing(
                    leader=leader,
                    share=share,
                    overall_share=overall_share,
                    present=present,
                    timed_sessions=self._timed_sessions,
                ),
            )
        ]


def _build_briefing(
    *,
    leader: str,
    share: int | float,
    overall_share: int | float,
    present: int,
    timed_sessions: int,
) -> str:
    """Build the briefing for the time-share finding."""
    if _is_model_component(leader):
        hypothesis = (
            "Check whether this model's calls are long because of large inputs or "
            "long outputs. If they are, a faster model or a smaller context for "
            "those calls is the single change to try, with one model override on "
            "replay."
        )
    else:
        hypothesis = (
            "Check whether this tool's calls repeat the same request, wait on a slow "
            "backend, or run one after another when they could overlap. If one of "
            "those holds, speeding up, caching, or parallelizing that one tool is "
            "the single change to try before comparing recorded session time."
        )
    return format_briefing(
        odd=(
            f"{leader} covers {share}% of the recorded time of the "
            f"{pluralize(present, 'session')} that use it, and {overall_share}% of "
            f"the recorded time across all {timed_sessions} timed sessions."
        ),
        look_first=(
            "Open the sessions in evidence_locators, ordered by the share of their "
            "recorded time this component covers; each locator points at the "
            "component's longest node in that session. Check whether the time comes "
            "from a few long calls or many short ones."
        ),
        cohort=(
            f"the {present} sessions that use this component, starting with the "
            "listed sessions where it covers the largest share of recorded time."
        ),
        hypothesis=hypothesis,
        held=(
            f"the replayed cohort's share of time in this component falls below "
            f"{share}% and its recorded duration drops, while the failed-session "
            "rate does not rise."
        ),
    )
