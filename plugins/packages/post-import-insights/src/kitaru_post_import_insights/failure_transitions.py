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
"""Last-good to first-wrong failure transition matrix and step strip."""

import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from statistics import median

from kitaru.api_models.v1.insight import (
    FailureMatrixArtifact,
    FailureMatrixCell,
    MatrixSessionRef,
    Severity,
    StepStripArtifact,
    StepStripRow,
    SuggestedQuestion,
)
from kitaru.api_models.v1.session_node import NodeStatus, NodeType, SessionNodeResponse
from kitaru_post_import_insights.findings import (
    CandidateCoverage,
    CandidateFinding,
    ContributionSelector,
    DeterministicFact,
    calculate_percent,
    format_briefing,
    pluralize,
)
from kitaru_post_import_insights.labels import sanitize_label
from kitaru_post_import_insights.models import EvidenceLocator
from kitaru_post_import_insights.profiling_state import SessionReferences
from kitaru_post_import_insights.resource_inputs import (
    FindingLimits,
    SessionView,
    get_tool_label,
    select_evidence,
)

MIN_TRANSITION_SESSIONS = 2
MAX_MATRIX_AXIS = 6
MAX_CELL_SESSIONS = 10
START_STATE = "Start"
SIGNAL = "first-failing-transition"


def _step_label(node: SessionNodeResponse) -> str:
    """Return a compact, safe state label for one tool or model node."""
    if node.node_type is NodeType.TOOL_CALL:
        return get_tool_label(node)
    return sanitize_label(node.name) or node.node_type.value


def _fold_axis(counts: Counter[str]) -> tuple[list[str], dict[str, str]]:
    """Keep at most 6 axis labels, folding rare states into 'Other'."""
    ordered = [
        label for label, _ in sorted(counts.items(), key=lambda i: (-i[1], i[0]))
    ]
    if len(ordered) <= MAX_MATRIX_AXIS:
        return ordered, {label: label for label in ordered}
    kept = ordered[: MAX_MATRIX_AXIS - 1]
    if "Other" in kept:
        kept = [label for label in kept if label != "Other"]
        kept = ordered[:MAX_MATRIX_AXIS]
    axis = [*kept, "Other"] if "Other" not in kept else kept
    kept_set = set(kept)
    mapping = {label: (label if label in kept_set else "Other") for label in ordered}
    return axis, mapping


@dataclass
class FailureTransitions:
    """Aggregate where recorded node failures start across sessions."""

    _limits: FindingLimits
    _sessions_analyzed: int = 0
    _failed_sessions: SessionReferences = field(init=False)
    _pair_counts: Counter[tuple[str, str]] = field(default_factory=Counter)
    _pair_sessions: dict[tuple[str, str], list[MatrixSessionRef]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _cause_good_steps: dict[str, list[int]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _cause_wrong_steps: dict[str, list[int]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _cause_sessions: dict[str, list[uuid.UUID]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _max_observed_step: int = 0
    _evidence: list[EvidenceLocator] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Initialize bounded session references."""
        self._failed_sessions = SessionReferences(self._limits.contributors)

    def consume(self, view: SessionView) -> None:
        """Find the last good step and first failing step in one session."""
        self._sessions_analyzed += 1
        steps = [
            node
            for node in view.nodes
            if node.node_type in (NodeType.TOOL_CALL, NodeType.LLM_CALL)
        ]
        if not steps:
            return
        first_fail_idx = next(
            (i for i, node in enumerate(steps) if node.status is NodeStatus.FAILED),
            None,
        )
        if first_fail_idx is None:
            return

        session_id = view.session.id
        failing_node = steps[first_fail_idx]
        to_state = _step_label(failing_node)
        last_good_idx = first_fail_idx - 1
        from_state = (
            _step_label(steps[last_good_idx]) if last_good_idx >= 0 else START_STATE
        )

        step_num = first_fail_idx + 1
        good_step_num = last_good_idx + 1 if last_good_idx >= 0 else None
        self._max_observed_step = max(self._max_observed_step, len(steps), step_num)

        self._failed_sessions.add(session_id)
        pair = (from_state, to_state)
        self._pair_counts[pair] += 1
        pair_refs = self._pair_sessions[pair]
        pair_refs.append(
            MatrixSessionRef(
                session_id=session_id,
                node_id=failing_node.id,
                step_label=f"step {step_num}",
            )
        )
        if len(pair_refs) > MAX_CELL_SESSIONS * 4:
            pair_refs.sort(key=lambda r: (str(r.session_id), str(r.node_id or "")))
            del pair_refs[MAX_CELL_SESSIONS:]
        if good_step_num is not None:
            self._cause_good_steps[to_state].append(good_step_num)
        self._cause_wrong_steps[to_state].append(step_num)
        cause_sids = self._cause_sessions[to_state]
        cause_sids.append(session_id)
        if len(cause_sids) > 200:
            cause_sids.sort(key=str)
            del cause_sids[50:]
        self._evidence.append(
            EvidenceLocator(
                session_id=session_id,
                node_id=failing_node.id,
                signal=SIGNAL,
            )
        )
        if len(self._evidence) > self._limits.evidence * 4:
            self._evidence.sort(
                key=lambda item: (
                    str(item.session_id),
                    str(item.node_id or ""),
                    item.signal,
                )
            )
            del self._evidence[self._limits.evidence :]

    def build(self, select: ContributionSelector) -> list[CandidateFinding]:
        """Build a failure matrix + step strip when >= 2 sessions fail at a node."""
        total_failed = len(self._failed_sessions)
        if total_failed < MIN_TRANSITION_SESSIONS or not self._pair_counts:
            return []

        from_totals: Counter[str] = Counter()
        to_totals: Counter[str] = Counter()
        for (src, dst), count in self._pair_counts.items():
            from_totals[src] += count
            to_totals[dst] += count

        rows, from_map = _fold_axis(from_totals)
        cols, to_map = _fold_axis(to_totals)

        folded_counts: Counter[tuple[str, str]] = Counter()
        folded_sessions: dict[tuple[str, str], list[MatrixSessionRef]] = defaultdict(
            list
        )
        for (src, dst), count in self._pair_counts.items():
            key = (from_map[src], to_map[dst])
            folded_counts[key] += count
            folded_sessions[key].extend(self._pair_sessions[(src, dst)])
        for refs in folded_sessions.values():
            refs.sort(key=lambda r: (str(r.session_id), str(r.node_id or "")))
            del refs[MAX_CELL_SESSIONS:]

        cells = [
            FailureMatrixCell(
                from_state=src,
                to_state=dst,
                count=count,
                sessions=folded_sessions[(src, dst)],
            )
            for (src, dst), count in sorted(
                folded_counts.items(), key=lambda i: (-i[1], i[0])
            )
        ]
        top_count = cells[0].count
        top_share = calculate_percent(top_count, total_failed)
        affected_share = calculate_percent(total_failed, self._sessions_analyzed)

        matrix = FailureMatrixArtifact(
            title="Last good to first failing step",
            state_by="tool",
            rows=rows,
            cols=cols,
            total_failed=total_failed,
            cells=cells,
        )

        total_steps = min(max(self._max_observed_step, 4), 20)
        strip_rows: list[StepStripRow] = []
        for cause, count in sorted(to_totals.items(), key=lambda i: (-i[1], i[0]))[:6]:
            wrong_step = min(round(median(self._cause_wrong_steps[cause])), total_steps)
            good_steps = self._cause_good_steps.get(cause)
            good_step = (
                min(round(median(good_steps)), total_steps) if good_steps else None
            )
            row_sev: Severity = (
                "high"
                if count * 2 >= total_failed
                else ("medium" if count >= 2 else "low")
            )
            strip_rows.append(
                StepStripRow(
                    label=cause[:120],
                    session_count=count,
                    last_good_step=good_step,
                    first_wrong_step=max(1, wrong_step),
                    severity=row_sev,
                    session_ids=sorted(self._cause_sessions[cause], key=str)[:50],
                )
            )
        step_strip = StepStripArtifact(
            title="Where failures start by step",
            presentation="rows",
            total_steps=total_steps,
            rows=strip_rows,
        )

        canonical_evidence = sorted(
            self._evidence,
            key=lambda item: (
                str(item.session_id),
                str(item.node_id or ""),
                item.signal,
            ),
        )[: self._limits.evidence]
        contributions, evidence = select_evidence(
            select, self._failed_sessions, canonical_evidence
        )
        severity: Severity = (
            "high" if affected_share >= 15 or total_failed >= 10 else "medium"
        )
        return [
            CandidateFinding(
                id="failure-transitions",
                family="failures",
                rank=25,
                tier="primary",
                severity=severity,
                eyebrow="WHERE FAILURES START",
                title=(
                    f"Recorded node failures affect "
                    f"{pluralize(total_failed, 'session')} ({affected_share}%)"
                ),
                fallback_description=(
                    f"{pluralize(total_failed, 'session')} ({affected_share}%) "
                    f"recorded a failed step; the top transition accounts for "
                    f"{pluralize(top_count, 'session')} ({top_share}%)."
                ),
                caveat=(
                    "Shows the last succeeded node before the first failed tool or "
                    "model node in each session; later recovery is still possible."
                ),
                data=matrix,
                extra_artifacts=[step_strip],
                facts=[
                    DeterministicFact(name="failed_step_sessions", value=total_failed),
                    DeterministicFact(
                        name="affected_share_percent", value=affected_share
                    ),
                    DeterministicFact(name="leading_transition_count", value=top_count),
                    DeterministicFact(
                        name="leading_transition_share_percent", value=top_share
                    ),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=self._sessions_analyzed,
                    affected_sessions=total_failed,
                    occurrences=total_failed,
                    evidence_available=total_failed,
                    evidence_retained=len(evidence),
                    contributing_sessions_available=total_failed,
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=evidence,
                investigation_prompt=format_briefing(
                    odd=(
                        f"{pluralize(total_failed, 'session')} ({affected_share}%) "
                        f"hit a failing node; the leading transition accounts for "
                        f"{pluralize(top_count, 'session')} ({top_share}%)."
                    ),
                    look_first=(
                        "Open the sessions for the leading cell in the failure matrix "
                        "and inspect the last succeeded node right before the first "
                        "failed step."
                    ),
                    cohort=(
                        f"the {top_count} sessions failing on the leading matrix "
                        "transition."
                    ),
                    hypothesis=(
                        "Check whether the preceding step produces a state or payload "
                        "that the failing step cannot handle without a guard or retry "
                        "rule."
                    ),
                    held=(
                        f"failures on the leading transition drop below {top_count} "
                        "sessions without shifting failures to another step."
                    ),
                ),
                suggested_questions=[
                    SuggestedQuestion(
                        label="Which failing transition affects the most sessions?"
                    ),
                    SuggestedQuestion(label="Show the earliest failing steps"),
                ],
            )
        ]
