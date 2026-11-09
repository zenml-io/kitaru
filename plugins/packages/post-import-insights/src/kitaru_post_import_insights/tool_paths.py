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
"""Find the common and one-off tool-call paths sessions take."""

import hashlib
import uuid
from itertools import groupby

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
from kitaru_post_import_insights.labels import MAX_LABEL_LENGTH, sanitize_label
from kitaru_post_import_insights.models import EvidenceLocator
from kitaru_post_import_insights.profiling_state import (
    CountsStore,
    LabelCounts,
    Observation,
    ObservationStore,
    SessionReferences,
)
from kitaru_post_import_insights.resource_inputs import (
    FindingLimits,
    SessionView,
    build_references,
    get_tool_label,
    select_evidence,
)

# Fewer sessions cannot show which paths are common.
MIN_PATH_SESSIONS = 10
# The chart names the most common paths and combines the rest.
CHARTED_PATHS = 5
_PATH_SEPARATOR = " -> "
_NAMESPACE = "tool-path"
SIGNAL = "unique-tool-path"


def get_tool_path(view: SessionView) -> str | None:
    """Label a session's tool-name sequence, collapsing consecutive repeats."""
    names = [name for name, _ in groupby(map(get_tool_label, view.tool_calls))]
    if not names:
        return None
    label = sanitize_label(_PATH_SEPARATOR.join(names))
    if label is not None:
        return label
    # Long paths keep their ends and a digest, so distinct paths stay distinct.
    digest = hashlib.sha256("\x1f".join(names).encode("utf-8")).hexdigest()[:12]
    compact = (
        f"{names[0]}{_PATH_SEPARATOR}...{_PATH_SEPARATOR}{names[-1]} "
        f"({len(names)} steps, path {digest})"
    )
    if len(compact) <= MAX_LABEL_LENGTH and sanitize_label(compact) is not None:
        return compact
    return f"{len(names)}-step tool path {digest}"


class ToolPaths:
    """Count each session's collapsed tool path."""

    def __init__(
        self, counts: CountsStore, store: ObservationStore, limits: FindingLimits
    ) -> None:
        """Initialize empty path counts backed by shared stores."""
        self._paths = LabelCounts(counts, _NAMESPACE)
        self._store = store
        self._limits = limits
        self._sessions = SessionReferences(limits.contributors)

    def consume(self, view: SessionView) -> None:
        """Record the session's tool path when it has tool calls."""
        path = get_tool_path(view)
        if path is None:
            return
        self._paths.add(path)
        self._sessions.add(view.session.id)
        self._store.add(
            _NAMESPACE, Observation(value=0, session=str(view.session.id), key=path)
        )

    def build(self, select: ContributionSelector) -> list[CandidateFinding]:
        """Return the tool-path candidate when sessions take more than one path."""
        sessions = len(self._sessions)
        if sessions < MIN_PATH_SESSIONS or len(self._paths) < 2:
            return []
        top = self._paths.top(CHARTED_PATHS)
        charted = top[:CHARTED_PATHS]
        charted_sessions = sum(count for _, count in charted)
        charted_share = calculate_percent(charted_sessions, sessions)
        unique, unique_ids = self._store.get_unique_key_sessions(
            _NAMESPACE, self._limits.contributors
        )
        unique_share = calculate_percent(unique, sessions)
        if unique:
            cohort = build_references(
                (uuid.UUID(item) for item in unique_ids),
                available=unique,
                limit=self._limits.contributors,
            )
            evidence = [
                EvidenceLocator(session_id=uuid.UUID(item), signal=SIGNAL)
                for item in unique_ids[: self._limits.evidence]
            ]
        else:
            cohort, evidence = self._sessions, []
        contributions, evidence = select_evidence(select, cohort, evidence)
        paths = len(self._paths)
        shown = (
            f"The {len(charted)} most common tool paths"
            if paths > len(charted)
            else f"All {paths} tool paths"
        )
        title = f"{shown} cover {charted_share}% of sessions"
        if unique:
            title += (
                f"; {pluralize(unique, 'session')} took a path no other session took"
            )
        return [
            CandidateFinding(
                id="tool-paths",
                family="tool_paths",
                rank=115,
                eyebrow="TOOL PATHS",
                title=title,
                fallback_description=(
                    "Each path is the order of tool calls in a session with repeats "
                    "collapsed. Compare the common paths with the one-off ones to "
                    "choose a starting point."
                ),
                caveat=(
                    "Paths use recorded tool nodes in position order, collapse "
                    "consecutive calls to the same tool, and ignore inputs and "
                    "results. Sessions without tool nodes are left out."
                    + (
                        " The chart combines the less common paths."
                        if paths > len(charted)
                        else ""
                    )
                ),
                data=CategoricalInsightData(
                    unit="sessions",
                    values=[
                        CategoryValue(label=label, value=count) for label, count in top
                    ],
                ),
                facts=[
                    DeterministicFact(name="sessions_with_tool_calls", value=sessions),
                    DeterministicFact(name="distinct_tool_paths", value=paths),
                    DeterministicFact(name="charted_paths", value=len(charted)),
                    DeterministicFact(
                        name="charted_paths_share_percent", value=charted_share
                    ),
                    DeterministicFact(name="unique_path_sessions", value=unique),
                    DeterministicFact(
                        name="unique_path_share_percent", value=unique_share
                    ),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=sessions,
                    affected_sessions=len(cohort),
                    occurrences=len(cohort),
                    evidence_available=unique,
                    evidence_retained=len(evidence),
                    contributing_sessions_available=len(cohort),
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=evidence,
                investigation_prompt=_build_briefing(
                    sessions=sessions,
                    paths=paths,
                    leading=top[0],
                    charted=len(charted),
                    charted_share=charted_share,
                    unique=unique,
                    unique_share=unique_share,
                ),
            )
        ]


def _build_briefing(
    *,
    sessions: int,
    paths: int,
    leading: tuple[str, int],
    charted: int,
    charted_share: int | float,
    unique: int,
    unique_share: int | float,
) -> str:
    """Build the briefing for the tool-path finding."""
    odd = (
        f"The {sessions} sessions with tool calls take {paths} distinct tool paths. "
        f"The {charted} most common cover {charted_share}% of sessions, led by "
        f"{leading[0]} with {pluralize(leading[1], 'session')}."
    )
    if unique:
        odd += (
            f" {pluralize(unique, 'session')} ({unique_share}%) took a path no other "
            "session took."
        )
    look_first = (
        "Open the sessions in evidence_locators; each took a path no other session "
        "took. Compare each with a session on the most common path and find the "
        "first tool call where the two diverge."
        if unique
        else "Every path repeats in at least two sessions. Open a few sessions from "
        "the least common charted paths and compare them with the most common path."
    )
    return format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=(
            f"the {unique} sessions on one-off paths, versus sessions on the most "
            "common path."
            if unique
            else "sessions on the least common paths, versus sessions on the most "
            "common path."
        ),
        hypothesis=(
            "Check whether sessions off the most common path start from inputs it "
            "does not handle, or from a tool result that sends the agent off course. "
            "If they share one divergence point, the instruction or tool behind that "
            "step is the single factor to change and replay."
        ),
        held=(
            "replayed sessions from the cohort follow fewer distinct paths, more of "
            "them reach a common path, and the failed-session rate does not rise."
        ),
    )
