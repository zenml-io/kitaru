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
"""Structured report assembly for verified session findings."""

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.insight import (
    AlignedStep,
    CategoricalArtifact,
    CategoryValue,
    EvidenceArtifact,
    EvidenceRow,
    InsightAction,
    InsightArtifact,
    InsightCoverage,
    InsightFinding,
    InsightHighlight,
    InsightInput,
    RateArtifact,
    SessionComparisonArtifact,
    SessionSide,
    StepStoryArtifact,
    StepStripArtifact,
    StepStripRow,
    StoryStep,
    SuggestedQuestion,
    TextArtifact,
    Tone,
)
from kitaru.api_models.v1.session import SessionStatus
from kitaru_post_import_insights.findings import calculate_percent, pluralize
from kitaru_post_import_insights.models import MAX_INVESTIGATION_PROMPT_LENGTH

SESSION_FINDINGS_METADATA_KEY = "kitaru.session-findings/v1"
MAX_FAILING_EVIDENCE_ROWS = 20
MAX_COUNTEREXAMPLE_ROWS = 2
MAX_PROMPT_SESSION_IDS = 50
MAX_RATE_LEAD_FINDINGS = 2
MAX_CATEGORY_BARS = 8
MAX_COMPARISON_STEPS = 15
MAX_STEP_STRIP_STEPS = 30

Harm = Literal["high", "medium", "low"]
StoryRole = Literal["user", "agent", "tool", "system"]
_VALID_ROLES: frozenset[str] = frozenset({"user", "agent", "tool", "system"})


@dataclass(frozen=True)
class ToolNodeRef:
    """Compact pointer to one tool node inside a session."""

    step: int
    node_id: UUID
    tool_name: str
    failed: bool


@dataclass(frozen=True)
class SessionRecord:
    """Bounded summary of one fetched session kept after releasing its full trace."""

    session_id: UUID
    session_number: int | None
    started_at: datetime | None
    status: SessionStatus
    tool_call_count: int
    tool_nodes: tuple[ToolNodeRef, ...]
    transcript: str

    @property
    def has_failed_tool(self) -> bool:
        """Return whether any recorded tool call in this session failed."""
        return any(node.failed for node in self.tool_nodes)

    @property
    def label(self) -> str | None:
        """Return a short human-readable session label when numbered."""
        return (
            f"Session #{self.session_number}"
            if self.session_number is not None
            else None
        )


@dataclass(frozen=True)
class StoryStepSpec:
    """One numbered step describing how a verified problem occurred."""

    text: str
    role: StoryRole | None = None
    tool_name: str | None = None
    is_bad_step: bool = False
    note: str | None = None


@dataclass(frozen=True)
class Finding:
    """A problem verified in at least one session."""

    title: str
    harm: Harm
    example: str
    checks: list[str]
    sessions: list[int]
    check_first: str | None = None
    mechanism_steps: list[StoryStepSpec] = field(default_factory=list)
    example_session: int | None = None
    counterexample_sessions: list[int] = field(default_factory=list)
    counterexample_note: str | None = None
    suggested_questions: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReportContext:
    """Shared context describing the analyzed cohort for report assembly."""

    agent_id: UUID | None = None
    agent_name: str | None = None
    server_url: str | None = None
    import_id: UUID | None = None
    source_provider: str | None = None
    reader_sessions_inspected: int | None = None


def _ensure_utc(value: datetime | None) -> datetime | None:
    """Normalize a datetime to timezone-aware UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _match_tool_node(
    record: SessionRecord,
    *,
    mentioned_tools: Sequence[str],
    prefer_failed: bool = True,
) -> ToolNodeRef | None:
    """Find the most relevant recorded tool node in a session."""
    if not record.tool_nodes:
        return None
    lowered = {tool.lower() for tool in mentioned_tools if tool}
    if lowered:
        matching = [
            node for node in record.tool_nodes if node.tool_name.lower() in lowered
        ]
        if matching:
            if prefer_failed:
                for node in matching:
                    if node.failed:
                        return node
            return matching[0]
    if prefer_failed:
        for node in record.tool_nodes:
            if node.failed:
                return node
    return record.tool_nodes[0]


def _extract_mentioned_tools(
    finding: Finding, records: Sequence[SessionRecord]
) -> list[str]:
    """Identify known tool names mentioned in a finding's steps or checks."""
    ordered: list[str] = []
    seen: set[str] = set()
    for step in finding.mechanism_steps:
        if step.tool_name and step.tool_name.lower() not in seen:
            seen.add(step.tool_name.lower())
            ordered.append(step.tool_name)
    known_tools = {
        node.tool_name
        for idx in finding.sessions[:20]
        if 0 <= idx < len(records)
        for node in records[idx].tool_nodes
    }
    search_blob = " ".join(finding.checks).lower()
    for tool in sorted(known_tools):
        if tool.lower() in search_blob and tool.lower() not in seen:
            seen.add(tool.lower())
            ordered.append(tool)
    return ordered


def _build_step_story(
    finding: Finding, records: Sequence[SessionRecord]
) -> StepStoryArtifact | None:
    """Build a StepStoryArtifact from mechanism steps when present."""
    valid_steps = [step for step in finding.mechanism_steps if step.text.strip()]
    if not valid_steps:
        return None
    record = _pick_example_record(finding, records)
    story_steps: list[StoryStep] = []
    for number, step in enumerate(valid_steps[:12], start=1):
        role: StoryRole | None = step.role if step.role in _VALID_ROLES else None
        tool_name = step.tool_name.strip()[:120] if step.tool_name else None
        matched_node = (
            _match_tool_node(
                record,
                mentioned_tools=[tool_name] if tool_name else (),
                prefer_failed=step.is_bad_step,
            )
            if record is not None and tool_name
            else None
        )
        tone: Tone = "danger" if step.is_bad_step else "neutral"
        story_steps.append(
            StoryStep(
                number=number,
                text=step.text.strip()[:400],
                role=role,
                tool_name=tool_name,
                node_id=matched_node.node_id if matched_node is not None else None,
                tone=tone,
                note=step.note.strip()[:240]
                if step.note and step.note.strip()
                else None,
            )
        )
    return StepStoryArtifact(
        title="How it happened",
        session_id=record.session_id if record is not None else None,
        session_label=record.label if record is not None else None,
        steps=story_steps,
    )


def _pick_example_record(
    finding: Finding, records: Sequence[SessionRecord]
) -> SessionRecord | None:
    """Return the primary failing example session record for a finding."""
    example_idx = (
        finding.example_session
        if finding.example_session is not None
        and finding.example_session in finding.sessions
        else finding.sessions[0]
    )
    return records[example_idx] if 0 <= example_idx < len(records) else None


def _build_session_comparison(
    finding: Finding, records: Sequence[SessionRecord]
) -> SessionComparisonArtifact | None:
    """Build a failing-vs-passing tool-sequence diff when the two paths differ."""
    if not finding.counterexample_sessions:
        return None
    failing = _pick_example_record(finding, records)
    counter_idx = finding.counterexample_sessions[0]
    passing = records[counter_idx] if 0 <= counter_idx < len(records) else None
    if failing is None or passing is None:
        return None
    if not failing.tool_nodes or not passing.tool_nodes:
        return None
    fail_names = [node.tool_name for node in failing.tool_nodes[:MAX_COMPARISON_STEPS]]
    pass_names = [node.tool_name for node in passing.tool_nodes[:MAX_COMPARISON_STEPS]]
    if fail_names == pass_names:
        return None

    left_steps: list[AlignedStep] = []
    right_steps: list[AlignedStep] = []
    i = j = 0
    while (i < len(fail_names) or j < len(pass_names)) and len(
        left_steps
    ) < MAX_COMPARISON_STEPS:
        if (
            i < len(fail_names)
            and j < len(pass_names)
            and fail_names[i] == pass_names[j]
        ):
            f_node = failing.tool_nodes[i]
            p_node = passing.tool_nodes[j]
            left_steps.append(
                AlignedStep(
                    label=f_node.tool_name[:80],
                    node_id=f_node.node_id,
                    status="highlight_bad" if f_node.failed else "present",
                )
            )
            right_steps.append(
                AlignedStep(
                    label=p_node.tool_name[:80],
                    node_id=p_node.node_id,
                    status="present",
                )
            )
            i += 1
            j += 1
        elif j < len(pass_names) and pass_names[j] not in fail_names[i:]:
            p_node = passing.tool_nodes[j]
            left_steps.append(AlignedStep(label=None, status="missing"))
            right_steps.append(
                AlignedStep(
                    label=p_node.tool_name[:80],
                    node_id=p_node.node_id,
                    status="highlight_good",
                )
            )
            j += 1
        elif i < len(fail_names):
            f_node = failing.tool_nodes[i]
            left_steps.append(
                AlignedStep(
                    label=f_node.tool_name[:80],
                    node_id=f_node.node_id,
                    status="highlight_bad",
                )
            )
            right_steps.append(AlignedStep(label=None, status="missing"))
            i += 1
        else:
            p_node = passing.tool_nodes[j]
            left_steps.append(AlignedStep(label=None, status="missing"))
            right_steps.append(
                AlignedStep(
                    label=p_node.tool_name[:80],
                    node_id=p_node.node_id,
                    status="highlight_good",
                )
            )
            j += 1

    return SessionComparisonArtifact(
        title="Failing session vs passing counterexample",
        left=SessionSide(
            session_id=failing.session_id,
            verdict="fail",
            steps=left_steps,
        ),
        right=SessionSide(
            session_id=passing.session_id,
            verdict="pass",
            steps=right_steps,
        ),
    )


def _build_step_strip(
    finding: Finding,
    records: Sequence[SessionRecord],
    mentioned_tools: Sequence[str],
) -> StepStripArtifact | None:
    """Build a step strip showing the median step where affected runs break."""
    matched_steps: list[int] = []
    max_step = 0
    for idx in finding.sessions:
        record = records[idx]
        if record.tool_nodes:
            max_step = max(max_step, max(node.step for node in record.tool_nodes))
        node = _match_tool_node(record, mentioned_tools=mentioned_tools)
        if node is not None and node.step >= 1:
            matched_steps.append(node.step)
    if not matched_steps:
        return None
    total_steps = min(MAX_STEP_STRIP_STEPS, max(2, max_step))
    matched_steps.sort()
    median_step = min(total_steps, matched_steps[len(matched_steps) // 2])
    last_good = median_step - 1 if median_step > 1 else None
    return StepStripArtifact(
        title="Where the failure occurs by step",
        presentation="rows",
        total_steps=total_steps,
        rows=[
            StepStripRow(
                label=finding.title[:80],
                session_count=len(finding.sessions),
                last_good_step=last_good,
                first_wrong_step=median_step,
                severity=finding.harm,
                session_ids=[records[idx].session_id for idx in finding.sessions[:50]],
            )
        ],
    )


def _build_tool_breakdown(
    finding: Finding,
    records: Sequence[SessionRecord],
    *,
    tone: Tone,
) -> CategoricalArtifact | None:
    """Build a ranked bar breakdown of tools seen across affected sessions."""
    tool_sessions: Counter[str] = Counter()
    for idx in finding.sessions:
        seen_in_session = {node.tool_name for node in records[idx].tool_nodes}
        for tool_name in seen_in_session:
            tool_sessions[tool_name] += 1
    if len(tool_sessions) < 2:
        return None
    ranked = sorted(tool_sessions.items(), key=lambda item: (-item[1], item[0]))
    if len(ranked) > MAX_CATEGORY_BARS:
        head = ranked[: MAX_CATEGORY_BARS - 1]
        other_total = sum(count for _, count in ranked[MAX_CATEGORY_BARS - 1 :])
        head.append(("Other", other_total))
        ranked = head
    return CategoricalArtifact(
        title="Tools in affected sessions",
        presentation="ranked_bars",
        unit="sessions",
        values=[
            CategoryValue(
                label=label[:120],
                value=count,
                tone=tone if idx == 0 else "neutral",
                focus=idx == 0,
            )
            for idx, (label, count) in enumerate(ranked)
        ],
    )


def _select_lead_artifact(
    position: int,
    finding: Finding,
    records: Sequence[SessionRecord],
    *,
    mentioned_tools: Sequence[str],
    rate: float,
    total: int,
    tone: Tone,
    used_lead_counts: Counter[str],
) -> InsightArtifact:
    """Pick a data-backed lead visual while limiting repeated rate share bars."""
    rate_artifact = RateArtifact(
        presentation="share_bar",
        rate=round(rate, 4),
        numerator=len(finding.sessions),
        denominator=max(total, 1),
        label=f"of {total} sessions",
        basis=f"Verified across all {total} sessions",
        tone=tone,
    )
    if position < MAX_RATE_LEAD_FINDINGS:
        used_lead_counts["rate"] += 1
        return rate_artifact

    candidates: list[tuple[str, InsightArtifact | None]] = [
        ("session_comparison", _build_session_comparison(finding, records)),
        ("step_strip", _build_step_strip(finding, records, mentioned_tools)),
        ("categorical", _build_tool_breakdown(finding, records, tone=tone)),
    ]
    available = [
        (kind, artifact) for kind, artifact in candidates if artifact is not None
    ]
    if available:
        available.sort(key=lambda item: used_lead_counts[item[0]])
        chosen_kind, chosen = available[0]
        used_lead_counts[chosen_kind] += 1
        return chosen

    used_lead_counts["rate"] += 1
    return rate_artifact


def _build_evidence_artifact(
    finding: Finding,
    records: Sequence[SessionRecord],
    mentioned_tools: Sequence[str],
) -> EvidenceArtifact:
    """Build a table of failing sessions plus up to 2 passing counterexamples."""
    rows: list[EvidenceRow] = []
    fallback_signal = finding.checks[0][:120] if finding.checks else finding.title[:120]
    for idx in finding.sessions[:MAX_FAILING_EVIDENCE_ROWS]:
        record = records[idx]
        node = _match_tool_node(record, mentioned_tools=mentioned_tools)
        rows.append(
            EvidenceRow(
                session_id=record.session_id,
                session_label=record.label,
                node_id=node.node_id if node is not None else None,
                step_label=f"step {node.step}" if node is not None else None,
                signal=(node.tool_name if node is not None else fallback_signal)[:120],
                verdict="fail",
            )
        )
    counter_note = (
        finding.counterexample_note.strip()[:160]
        if finding.counterexample_note and finding.counterexample_note.strip()
        else "Verified check did not hold in this session."
    )
    for idx in finding.counterexample_sessions[:MAX_COUNTEREXAMPLE_ROWS]:
        record = records[idx]
        node = _match_tool_node(
            record, mentioned_tools=mentioned_tools, prefer_failed=False
        )
        rows.append(
            EvidenceRow(
                session_id=record.session_id,
                session_label=record.label,
                node_id=node.node_id if node is not None else None,
                step_label=f"step {node.step}" if node is not None else None,
                signal=(node.tool_name if node is not None else "passing session")[
                    :120
                ],
                verdict="pass",
                is_counterexample=True,
                counterexample_note=counter_note,
            )
        )
    return EvidenceArtifact(
        title="Evidence",
        presentation="table",
        total_sessions=len(finding.sessions),
        rows=rows,
    )


def _build_action_label(title: str) -> str:
    """Build a short CTA button label within the 60-character limit."""
    clean = " ".join(title.split()).strip()
    if not clean:
        return "Investigate this finding"
    prefix = "Investigate: "
    budget = 60 - len(prefix)
    if len(clean) <= budget:
        return f"{prefix}{clean}"
    trimmed = clean[: budget - 1].rsplit(" ", 1)[0].rstrip(".,;:-")
    if not trimmed:
        return "Investigate this finding"
    return f"{prefix}{trimmed}…"


def _build_investigation_prompt(
    finding: Finding,
    records: Sequence[SessionRecord],
    *,
    context: ReportContext,
    check_first: str,
) -> str:
    """Build a self-contained investigation handoff briefing for a finding."""
    total = len(records)
    matching_ids = [str(records[idx].session_id) for idx in finding.sessions]
    counter_ids = [
        str(records[idx].session_id) for idx in finding.counterexample_sessions
    ]
    share = calculate_percent(len(matching_ids), total)
    lines = [
        "Investigate this verified Kitaru session finding.",
        "",
        "Setup:",
        "1. Ensure the `kitaru` CLI is installed (`uv tool install kitaru`).",
    ]
    if context.server_url:
        lines.append(
            f"2. Authenticate against the recorded server: "
            f"`kitaru login --server {context.server_url}`."
        )
    else:
        lines.append("2. Authenticate with `kitaru login` against your Kitaru server.")
    lines.append(
        "3. Run `kitaru setup` if the `kitaru-investigation` skill is not installed, "
        "then follow that skill."
    )
    lines.append("")
    lines.append("Context:")
    if context.agent_name and context.agent_id:
        lines.append(f"- Agent: {context.agent_name} (id {context.agent_id})")
    elif context.agent_id:
        lines.append(f"- Agent id: {context.agent_id}")
    if context.import_id:
        provider_suffix = (
            f" ({context.source_provider})"
            if context.source_provider
            and context.source_provider not in ("recorded", "mixed")
            else ""
        )
        lines.append(f"- Import id: {context.import_id}{provider_suffix}")
    lines.extend(
        [
            f"- Finding: {finding.title}",
            (
                f"- Verified rate: {len(matching_ids)} of {total} sessions "
                f"({share}%). Harm: {finding.harm}."
            ),
            f"- Check first: {check_first}",
            f"- Concrete example: {finding.example}",
            "",
            "Verified checks:",
            *(f"- {check}" for check in finding.checks),
            "",
            (
                "Affected session IDs "
                f"(showing {min(len(matching_ids), MAX_PROMPT_SESSION_IDS)} "
                f"of {len(matching_ids)}):"
            ),
            ", ".join(matching_ids[:MAX_PROMPT_SESSION_IDS]),
        ]
    )
    if counter_ids:
        lines.extend(
            [
                "",
                "Passing counterexample session IDs (for side-by-side comparison):",
                ", ".join(counter_ids[:MAX_COUNTEREXAMPLE_ROWS]),
            ]
        )
    prompt = "\n".join(lines)
    return prompt[:MAX_INVESTIGATION_PROMPT_LENGTH]


def _build_finding_questions(finding: Finding) -> list[SuggestedQuestion]:
    """Build up to 3 follow-up chat questions for one finding."""
    questions: list[SuggestedQuestion] = []
    seen: set[str] = set()
    for raw in finding.suggested_questions:
        cleaned = " ".join(raw.split()).strip()[:80]
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            questions.append(SuggestedQuestion(label=cleaned))
            if len(questions) == 3:
                return questions
    defaults = [
        f"Show sessions affected by: {finding.title}"[:80],
        "Compare a failing session with a passing counterexample",
        "Draft a replay test for a fix to this issue",
    ]
    for fallback in defaults:
        if fallback.lower() not in seen:
            seen.add(fallback.lower())
            questions.append(SuggestedQuestion(label=fallback))
            if len(questions) == 3:
                break
    return questions


def _collect_report_questions(
    findings: Sequence[InsightFinding],
) -> list[SuggestedQuestion]:
    """Collect up to 5 unique report-level sidebar chat questions."""
    questions: list[SuggestedQuestion] = []
    seen: set[str] = set()
    for finding in findings:
        for question in finding.suggested_questions:
            key = question.label.lower()
            if key in seen:
                continue
            seen.add(key)
            questions.append(question)
            if len(questions) == 5:
                return questions
    return questions


def build_session_findings_report(
    findings: Sequence[Finding],
    records: Sequence[SessionRecord],
    *,
    context: ReportContext,
) -> InsightInput:
    """Assemble one structured multi-finding InsightInput report from findings.

    Args:
        findings: Verified findings ordered most harmful first.
        records: Session records in the same index order used by ``findings``.
        context: Cohort context (agent, server, provider, reader sample count).

    Returns:
        One structured ``InsightInput`` report containing all verified findings.
    """
    if not findings:
        raise ValueError("Cannot build a report with zero verified findings.")
    total = len(records)
    started_times = [
        ts for record in records if (ts := _ensure_utc(record.started_at)) is not None
    ]
    started_after = min(started_times) if started_times else None
    started_before = max(started_times) if started_times else None

    caveats: list[str] = []
    if (
        context.reader_sessions_inspected is not None
        and context.reader_sessions_inspected < total
    ):
        caveats.append(
            f"Reader proposed checks from a targeted + random sample of "
            f"{context.reader_sessions_inspected} of {total} sessions; "
            f"every check was then verified across all {total} sessions."
        )

    insight_findings: list[InsightFinding] = []
    finding_metadata: dict[str, JsonValue] = {}
    used_lead_counts: Counter[str] = Counter()

    for position, finding in enumerate(findings):
        finding_id = f"session-finding-{position + 1}"
        matching_ids = [records[idx].session_id for idx in finding.sessions]
        rate = len(matching_ids) / total if total else 0.0
        share = calculate_percent(len(matching_ids), total)
        summary = (
            f"Verified in {len(matching_ids)} of {total} sessions "
            f"({share}%). Harm: {finding.harm}."
        )[:320]
        check_first = (
            finding.check_first.strip()[:1_000]
            if finding.check_first and finding.check_first.strip()
            else f"Verify this check in an affected session: {finding.checks[0]}"[
                :1_000
            ]
        )
        mentioned_tools = _extract_mentioned_tools(finding, records)
        tone: Tone = (
            "danger"
            if finding.harm == "high"
            else ("risk" if finding.harm == "medium" else "warn")
        )
        lead_artifact = _select_lead_artifact(
            position,
            finding,
            records,
            mentioned_tools=mentioned_tools,
            rate=rate,
            total=total,
            tone=tone,
            used_lead_counts=used_lead_counts,
        )
        artifacts: list[InsightArtifact] = [lead_artifact]
        step_story = _build_step_story(finding, records)
        if step_story is not None:
            artifacts.append(step_story)
        elif isinstance(lead_artifact, RateArtifact):
            artifacts.append(TextArtifact(style="body", content=finding.example))
        artifacts.append(_build_evidence_artifact(finding, records, mentioned_tools))

        action = InsightAction(
            label=_build_action_label(finding.title),
            prompt=_build_investigation_prompt(
                finding, records, context=context, check_first=check_first
            ),
            summary=check_first[:300],
        )
        insight_findings.append(
            InsightFinding(
                id=finding_id,
                title=finding.title[:255],
                summary=summary,
                body=finding.example,
                tier="primary",
                kind="finding",
                severity=finding.harm,
                artifacts=artifacts,
                contributing_session_ids=matching_ids[:1_000],
                check_first=check_first,
                action=action,
                suggested_questions=_build_finding_questions(finding),
            )
        )
        finding_metadata[finding_id] = {
            "harm": finding.harm,
            "checks": list(finding.checks),
            "contributing_session_ids": [str(sid) for sid in matching_ids],
            "counterexample_session_ids": [
                str(records[idx].session_id) for idx in finding.counterexample_sessions
            ],
        }

    top_finding = insight_findings[0]
    top_count = len(findings[0].sessions)
    report_summary = (
        f"{pluralize(len(insight_findings), 'verified session finding')} across "
        f"{pluralize(total, 'session')}. Top issue ({findings[0].harm} harm): "
        f"{findings[0].title} ({top_count} of {total})."
    )[:320]
    next_step = (
        InsightAction(
            label=top_finding.action.label,
            summary=(top_finding.check_first or top_finding.summary or "")[:300],
            prompt=top_finding.action.prompt,
        )
        if top_finding.action is not None
        else None
    )
    metadata: dict[str, JsonValue] = {
        "schema_version": "2",
        "total_sessions": total,
        "reader_sessions_inspected": (
            context.reader_sessions_inspected
            if context.reader_sessions_inspected is not None
            else total
        ),
        "findings": finding_metadata,
    }
    return InsightInput(
        name="openai-session-findings",
        title=top_finding.title,
        summary=report_summary,
        source_provider=context.source_provider,
        coverage=InsightCoverage(
            sessions_analyzed=total,
            sessions_available=total,
            started_after=started_after,
            started_before=started_before,
            caveats=caveats[:10],
        ),
        highlight=InsightHighlight(finding_id=top_finding.id, artifact_index=0),
        next_step=next_step,
        findings=insight_findings,
        suggested_questions=_collect_report_questions(insight_findings),
        metadata={SESSION_FINDINGS_METADATA_KEY: metadata},
    )
