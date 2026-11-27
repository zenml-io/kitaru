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
"""Per-tool health table combining calls, failures, retries, repeats, and cycles."""

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any

from kitaru.api_models.v1.insight import (
    Severity,
    SuggestedQuestion,
    TableArtifact,
    TableColumn,
)
from kitaru.api_models.v1.session_node import NodeStatus
from kitaru_post_import_insights.findings import (
    CandidateCoverage,
    CandidateFinding,
    ContributionSelector,
    DeterministicFact,
    calculate_percent,
    format_briefing,
    pluralize,
)
from kitaru_post_import_insights.models import EvidenceLocator
from kitaru_post_import_insights.profiling_state import SessionReferences
from kitaru_post_import_insights.resource_inputs import (
    FindingLimits,
    SessionView,
    get_tool_label,
    select_evidence,
)

MAX_TABLE_TOOLS = 12
SIGNAL = "tool-health-trouble"
_MIN_CYCLE_REPEATS = 3


def _hash_input(value: Any) -> str | None:
    """Return a compact stable digest for tool inputs, or None if unavailable."""
    if value is None:
        return None
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        )
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(encoded.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _detect_cycle_tools(tool_names: list[str], digests: list[str | None]) -> set[str]:
    """Return tools participating in an exact period-2-to-5 cycle repeated 3+ times."""
    cycled: set[str] = set()
    identities = [
        None if digest is None else (name, digest)
        for name, digest in zip(tool_names, digests, strict=True)
    ]
    total = len(identities)
    for period in range(2, 6):
        if total < period * _MIN_CYCLE_REPEATS:
            continue
        streak = 0
        for pos in range(total - period):
            if (
                identities[pos] is not None
                and identities[pos] == identities[pos + period]
            ):
                streak += 1
                if streak >= 2 * period:
                    for idx in range(pos - 2 * period + 1, pos + period + 1):
                        cycled.add(tool_names[idx])
            else:
                streak = 0
    return cycled


@dataclass
class _ToolRow:
    """Mutable per-tool counters retained during one profiling pass."""

    calls: int = 0
    sessions: int = 0
    failures: int = 0
    retries: int = 0
    repeats: int = 0
    cycle_sessions: int = 0

    @property
    def trouble_score(self) -> int:
        """Weight failures, retries, repeats, and cycles for table ordering."""
        return (
            self.failures * 3
            + self.retries * 3
            + self.repeats * 2
            + self.cycle_sessions * 4
        )


@dataclass
class ToolHealth:
    """Aggregate per-tool calls, failures, retries, repeats, and cycles."""

    _limits: FindingLimits
    _rows: dict[str, _ToolRow] = field(default_factory=dict)
    _total_sessions: int = 0
    _sessions_with_tools: int = 0
    _troubled_sessions: SessionReferences = field(init=False)
    _evidence: list[EvidenceLocator] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Initialize bounded session references."""
        self._troubled_sessions = SessionReferences(self._limits.contributors)

    def consume(self, view: SessionView) -> None:
        """Count per-tool activity and trouble signals for one session."""
        self._total_sessions += 1
        calls = view.tool_calls
        if not calls:
            return
        self._sessions_with_tools += 1
        session_id = view.session.id
        names = [get_tool_label(call) for call in calls]
        digests = [_hash_input(call.inputs) for call in calls]
        seen_in_session: set[str] = set()
        troubled = False

        for call, name in zip(calls, names, strict=True):
            row = self._rows.setdefault(name, _ToolRow())
            row.calls += 1
            if name not in seen_in_session:
                row.sessions += 1
                seen_in_session.add(name)
            if call.status is NodeStatus.FAILED:
                row.failures += 1
                troubled = True
                self._record_evidence(session_id, call.id, "tool-call-failed")

        for idx, (first, second) in enumerate(pairwise(calls)):
            if (
                names[idx] == names[idx + 1]
                and digests[idx] is not None
                and digests[idx] == digests[idx + 1]
            ):
                row = self._rows[names[idx]]
                row.repeats += 1
                troubled = True
                if first.status is NodeStatus.FAILED:
                    row.retries += 1
                    self._record_evidence(session_id, second.id, "failed-tool-retry")
                else:
                    self._record_evidence(
                        session_id, second.id, "identical-tool-repeat"
                    )

        for name in _detect_cycle_tools(names, digests):
            self._rows[name].cycle_sessions += 1
            troubled = True
            first_node = next(
                (
                    call.id
                    for call, tool in zip(calls, names, strict=True)
                    if tool == name
                ),
                None,
            )
            self._record_evidence(session_id, first_node, "repeating-tool-cycle")

        if troubled:
            self._troubled_sessions.add(session_id)

    def _record_evidence(
        self, session_id: uuid.UUID, node_id: uuid.UUID | None, signal: str
    ) -> None:
        locator = EvidenceLocator(
            session_id=session_id, node_id=node_id, signal=signal[:80]
        )
        self._evidence.append(locator)
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
        """Emit one per-tool health table whenever tools show trouble."""
        troubled_tools = [
            (name, row) for name, row in self._rows.items() if row.trouble_score > 0
        ]
        if not troubled_tools or not self._troubled_sessions:
            return []
        ordered = sorted(
            self._rows.items(),
            key=lambda item: (-item[1].trouble_score, -item[1].calls, item[0]),
        )[:MAX_TABLE_TOOLS]
        _top_name, top_row = ordered[0]
        total_calls = sum(row.calls for row in self._rows.values())
        total_failures = sum(row.failures for row in self._rows.values())
        total_retries = sum(row.retries for row in self._rows.values())
        total_repeats = sum(row.repeats for row in self._rows.values())
        top_failure_rate = calculate_percent(top_row.failures, top_row.calls)
        canonical_evidence = sorted(
            self._evidence,
            key=lambda item: (
                str(item.session_id),
                str(item.node_id or ""),
                item.signal,
            ),
        )[: self._limits.evidence]
        contributions, evidence = select_evidence(
            select, self._troubled_sessions, canonical_evidence
        )
        affected = len(self._troubled_sessions)
        share = calculate_percent(affected, self._sessions_with_tools)

        severity: Severity = (
            "high"
            if top_failure_rate >= 20 or total_failures + total_retries >= 10
            else "medium"
        )
        table = TableArtifact(
            columns=[
                TableColumn(key="tool", label="Tool", format="mono"),
                TableColumn(key="calls", label="Calls", format="number"),
                TableColumn(key="failures", label="Failures", format="number"),
                TableColumn(
                    key="failure_rate", label="Failure rate", format="percent_bar"
                ),
                TableColumn(key="retries", label="Retries", format="number"),
                TableColumn(key="repeats", label="Repeats", format="number"),
                TableColumn(key="cycles", label="Cycles", format="number"),
            ],
            rows=[
                {
                    "tool": name,
                    "calls": row.calls,
                    "failures": row.failures,
                    "failure_rate": round(row.failures / row.calls, 4)
                    if row.calls
                    else 0.0,
                    "retries": row.retries,
                    "repeats": row.repeats,
                    "cycles": row.cycle_sessions,
                }
                for name, row in ordered
            ],
        )
        trouble_parts = []
        if total_failures:
            trouble_parts.append(pluralize(total_failures, "failure"))
        if total_retries:
            trouble_parts.append(
                pluralize(total_retries, "failed retry", "failed retries")
            )
        if total_repeats:
            trouble_parts.append(pluralize(total_repeats, "identical repeat"))
        summary_counts = ", ".join(trouble_parts) or "tool trouble"
        return [
            CandidateFinding(
                id="tool-health-overview",
                family="tool_health",
                rank=15,
                tier="primary",
                severity=severity,
                eyebrow="TOOL HEALTH",
                title=(
                    f"Tool failures, retries, or loops affect "
                    f"{pluralize(affected, 'session')} ({share}%)"
                ),
                fallback_description=(
                    f"Across {pluralize(total_calls, 'tool call')} in "
                    f"{pluralize(len(self._rows), 'tool')}, the profiler recorded "
                    f"{summary_counts}; the top table row has {top_row.failures} "
                    f"failures ({top_failure_rate}%), {top_row.retries} retries, and "
                    f"{top_row.repeats} identical repeats."
                ),
                caveat=(
                    "Counts reflect recorded tool nodes. A tool failure or retry may "
                    "still recover before the session finishes."
                ),
                data=table,
                facts=[
                    DeterministicFact(name="tools_observed", value=len(self._rows)),
                    DeterministicFact(name="total_tool_calls", value=total_calls),
                    DeterministicFact(name="total_tool_failures", value=total_failures),
                    DeterministicFact(name="total_failed_retries", value=total_retries),
                    DeterministicFact(
                        name="total_identical_repeats", value=total_repeats
                    ),
                    DeterministicFact(name="affected_sessions", value=affected),
                    DeterministicFact(name="affected_share_percent", value=share),
                    DeterministicFact(
                        name="leading_tool_failure_rate_percent", value=top_failure_rate
                    ),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=self._sessions_with_tools,
                    affected_sessions=affected,
                    occurrences=total_failures + total_retries + total_repeats,
                    evidence_available=max(
                        len(self._evidence),
                        total_failures + total_retries + total_repeats,
                    ),
                    evidence_retained=len(evidence),
                    contributing_sessions_available=affected,
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=evidence,
                investigation_prompt=format_briefing(
                    odd=(
                        f"{pluralize(affected, 'session')} ({share}% of "
                        f"{self._sessions_with_tools} sessions with tool calls) "
                        f"recorded {summary_counts}."
                    ),
                    look_first=(
                        "Start with the leading row in the tool-health table and open "
                        "the sessions in evidence_locators to see what inputs preceded "
                        "each failure, retry, or loop."
                    ),
                    cohort=(
                        f"the {affected} sessions with tool failures, retries, or "
                        "loops, starting with the leading table row."
                    ),
                    hypothesis=(
                        "Check whether the leading tool fails or loops on one input "
                        "shape or returns an error message the agent cannot act on."
                    ),
                    held=(
                        f"the leading tool's failure rate drops below "
                        f"{top_failure_rate}% and retries and repeats fall without "
                        "raising failed sessions."
                    ),
                ),
                suggested_questions=[
                    SuggestedQuestion(
                        label="Why is the top tool failing or repeating?"
                    ),
                    SuggestedQuestion(
                        label="Which sessions have the worst tool loops?"
                    ),
                ],
            )
        ]
