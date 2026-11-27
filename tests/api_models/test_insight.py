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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for structured insight API models and artifact chart limits."""

import uuid

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.insight import (
    AlignedStep,
    Bin,
    BinnedArtifact,
    CategoricalArtifact,
    CategoryValue,
    ComparisonArtifact,
    ComparisonRow,
    CycleStep,
    EvidenceArtifact,
    EvidenceRow,
    FailureMatrixArtifact,
    FailureMatrixCell,
    FlowSankeyArtifact,
    InsightAction,
    InsightCoverage,
    InsightFinding,
    InsightHighlight,
    InsightInput,
    RateArtifact,
    SankeyLink,
    SankeyNode,
    SankeyStage,
    SessionComparisonArtifact,
    SessionDot,
    SessionDotsArtifact,
    SessionSide,
    SessionTimelineArtifact,
    StatGridArtifact,
    StatItem,
    StepStoryArtifact,
    StepStripArtifact,
    StepStripRow,
    StoryStep,
    SuggestedQuestion,
    TableArtifact,
    TableColumn,
    TextArtifact,
    TimelineSpan,
    TimeSeriesArtifact,
    TimeSeriesLine,
    ToolCycleArtifact,
)

SESSION_A = str(uuid.uuid4())
SESSION_B = str(uuid.uuid4())


ALL_ARTIFACT_PAYLOADS: list[dict[str, object]] = [
    {
        "type": "stat_grid",
        "items": [{"label": "New", "value": 1, "tone": "warn"}],
    },
    {
        "type": "rate",
        "presentation": "range",
        "rate": 0.14,
        "ci_low": 0.10,
        "ci_high": 0.20,
        "numerator": 31,
        "denominator": 214,
        "basis": "Every session read",
    },
    {
        "type": "categorical",
        "presentation": "ranked_bars",
        "unit": "sessions",
        "values": [
            {"label": "Policy check skipped", "value": 24, "tone": "danger"},
            {"label": "Other", "value": 5, "tone": "neutral"},
        ],
    },
    {
        "type": "comparison",
        "presentation": "dot_plot",
        "before_label": "Order edited",
        "after_label": "No order edit",
        "unit": "percent",
        "rows": [
            {"label": "Failed", "before": 66, "after": 12},
            {"label": "Escalated", "before": 38, "after": 8},
        ],
    },
    {
        "type": "table",
        "columns": [
            {"key": "tool", "label": "Tool", "format": "mono"},
            {"key": "calls", "label": "Calls", "format": "number"},
            {"key": "rate", "label": "Failure rate", "format": "percent_bar"},
        ],
        "rows": [{"tool": "search", "calls": 1840, "rate": 6.1}],
    },
    {
        "type": "binned",
        "presentation": "columns",
        "unit": "seconds",
        "bins": [
            {"lower_bound": None, "upper_bound": 5, "count": 40},
            {"lower_bound": 5, "upper_bound": None, "count": 12},
        ],
    },
    {
        "type": "session_dots",
        "unit": "seconds",
        "points": [
            {"session_id": SESSION_A, "value": 12.4, "highlight": True},
            {"session_id": SESSION_B, "value": 2.1},
        ],
    },
    {
        "type": "time_series",
        "presentation": "line",
        "unit": "percent",
        "x_labels": ["Oct 1", "Oct 8", "Oct 15"],
        "series": [{"label": "Failure rate", "values": [8, 12, 14], "focus": True}],
    },
    {
        "type": "step_strip",
        "total_steps": 10,
        "rows": [
            {
                "label": "Policy check skipped",
                "session_count": 24,
                "last_good_step": 6,
                "first_wrong_step": 7,
                "severity": "high",
            }
        ],
    },
    {
        "type": "failure_matrix",
        "rows": ["edit_order", "lookup_order"],
        "cols": ["issue_refund", "search"],
        "total_failed": 33,
        "cells": [
            {"from_state": "edit_order", "to_state": "issue_refund", "count": 24},
            {"from_state": "lookup_order", "to_state": "search", "count": 9},
        ],
    },
    {
        "type": "flow_sankey",
        "stages": [
            {"label": "Start", "nodes": [{"id": "s1", "label": "lookup", "count": 10}]},
            {"label": "End", "nodes": [{"id": "e1", "label": "refund", "count": 10}]},
        ],
        "links": [{"source": "s1", "target": "e1", "count": 10}],
    },
    {
        "type": "step_story",
        "steps": [
            {"number": 5, "text": "Edited the order", "tool_name": "edit_order"},
            {
                "number": 6,
                "text": "Issued refund without policy check",
                "tool_name": "issue_refund",
                "tone": "danger",
            },
        ],
    },
    {
        "type": "tool_cycle",
        "steps": [{"name": "search"}, {"name": "fetch_page"}],
        "repeat_count": 14,
        "session_count": 23,
    },
    {
        "type": "session_comparison",
        "left": {
            "session_id": SESSION_A,
            "session_label": "ses_fail",
            "verdict": "fail",
            "steps": [
                {"label": "edit_order", "status": "present"},
                {"label": "issue_refund", "status": "highlight_bad"},
            ],
        },
        "right": {
            "session_id": SESSION_B,
            "session_label": "ses_pass",
            "verdict": "pass",
            "steps": [
                {"label": "edit_order", "status": "present"},
                {"label": "check_refund_policy", "status": "highlight_good"},
            ],
        },
    },
    {
        "type": "session_timeline",
        "session_id": SESSION_A,
        "total_duration_ms": 12000,
        "spans": [
            {"name": "search", "start_ms": 0, "duration_ms": 4200, "tone": "warn"}
        ],
    },
    {
        "type": "evidence",
        "presentation": "table",
        "total_sessions": 31,
        "rows": [
            {
                "session_id": SESSION_A,
                "step_label": "step 7",
                "signal": "issue_refund",
                "verdict": "fail",
            },
            {
                "session_id": SESSION_B,
                "step_label": "step 6",
                "signal": "check_refund_policy",
                "verdict": "pass",
                "is_counterexample": True,
                "counterexample_note": "Counterexample, 1 of 16",
            },
        ],
    },
    {
        "type": "text",
        "style": "body",
        "content": "After an order edit, the agent skipped check_refund_policy.",
    },
]


@pytest.mark.parametrize(
    "artifact",
    ALL_ARTIFACT_PAYLOADS,
    ids=[str(item["type"]) for item in ALL_ARTIFACT_PAYLOADS],
)
def test_every_artifact_round_trips_through_insight_input(
    artifact: dict[str, object],
) -> None:
    """Round-trip all 17 InsightArtifact types inside InsightInput."""
    insight = InsightInput.model_validate(
        {
            "name": "report",
            "title": "Report",
            "findings": [
                {
                    "id": "finding-1",
                    "title": "Finding",
                    "artifacts": [artifact],
                }
            ],
        }
    )
    restored = InsightInput.model_validate(insight.model_dump())
    assert restored == insight


def test_full_report_round_trip() -> None:
    """Round-trip a report with coverage, highlight, next step, and questions."""
    insight = InsightInput(
        name="meridian-2026-10-19",
        title="Refunds skip the policy check when the order was edited",
        summary="31 of 47 edited-order sessions skipped check_refund_policy.",
        source_provider="Langfuse",
        coverage=InsightCoverage(
            sessions_analyzed=214,
            sessions_available=250,
            skipped=33,
            failed=3,
            caveats=["stopped at the 250-trace limit"],
        ),
        highlight=InsightHighlight(finding_id="policy-skip", artifact_index=0),
        next_step=InsightAction(
            label="Investigate this insight",
            prompt="Inspect the 31 sessions where check_refund_policy was skipped.",
            summary="Run check_refund_policy again after every order edit",
        ),
        findings=[
            InsightFinding(
                id="policy-skip",
                title="Refunds skip the policy check",
                severity="high",
                change_status="new",
                artifacts=[
                    RateArtifact(
                        presentation="range",
                        rate=0.14,
                        ci_low=0.10,
                        ci_high=0.20,
                        numerator=31,
                        denominator=214,
                    ),
                    EvidenceArtifact(
                        total_sessions=31,
                        rows=[
                            EvidenceRow(
                                session_id=uuid.UUID(SESSION_A),
                                step_label="step 7",
                                signal="issue_refund",
                                verdict="fail",
                            )
                        ],
                    ),
                ],
                contributing_session_ids=[uuid.UUID(SESSION_A)],
            )
        ],
        suggested_questions=[
            SuggestedQuestion(
                label="Why is the range this wide?",
                prompt="Explain the confidence interval on the 14% refund skip rate.",
            )
        ],
    )
    restored = InsightInput.model_validate(insight.model_dump(mode="json"))
    assert restored == insight


def test_insight_input_rejects_duplicate_finding_ids() -> None:
    """Reject two findings in the same report that share an id."""
    with pytest.raises(ValidationError, match="Duplicate finding id"):
        InsightInput(
            name="dup",
            title="Dup",
            findings=[
                InsightFinding(
                    id="same",
                    title="First",
                    artifacts=[TextArtifact(content="a")],
                ),
                InsightFinding(
                    id="same",
                    title="Second",
                    artifacts=[TextArtifact(content="b")],
                ),
            ],
        )


def test_insight_input_rejects_invalid_highlight_pointers() -> None:
    """Reject a highlight pointing to a missing finding or out-of-range artifact."""
    with pytest.raises(ValidationError, match="does not match any finding"):
        InsightInput(
            name="bad-highlight",
            title="Bad",
            highlight=InsightHighlight(finding_id="missing", artifact_index=0),
            findings=[
                InsightFinding(
                    id="f1", title="First", artifacts=[TextArtifact(content="a")]
                )
            ],
        )
    with pytest.raises(ValidationError, match="out of range"):
        InsightInput(
            name="bad-index",
            title="Bad",
            highlight=InsightHighlight(finding_id="f1", artifact_index=1),
            findings=[
                InsightFinding(
                    id="f1", title="First", artifacts=[TextArtifact(content="a")]
                )
            ],
        )


def test_categorical_limits_enforced() -> None:
    """Enforce duplicate labels, row cap, donut cap, and split-bar 2% floor."""
    with pytest.raises(ValidationError, match="Category labels must be unique"):
        CategoricalArtifact(
            unit="sessions",
            values=[
                CategoryValue(label="a", value=1),
                CategoryValue(label="a", value=2),
            ],
        )
    with pytest.raises(ValidationError, match="at most 8 rows"):
        CategoricalArtifact(
            presentation="ranked_bars",
            unit="sessions",
            values=[CategoryValue(label=f"c{i}", value=10 - i) for i in range(9)],
        )
    with pytest.raises(ValidationError, match="Donut requires 3 to 4 parts"):
        CategoricalArtifact(
            presentation="donut",
            unit="sessions",
            values=[CategoryValue(label=f"c{i}", value=10) for i in range(5)],
        )
    with pytest.raises(ValidationError, match="at least 2%"):
        CategoricalArtifact(
            presentation="split_bar",
            unit="sessions",
            values=[
                CategoryValue(label="Resolved", value=99),
                CategoryValue(label="Tiny", value=1),
            ],
        )
    with pytest.raises(ValidationError, match="cannot exceed its outer total"):
        CategoricalArtifact(
            presentation="focus_bars",
            unit="sessions",
            values=[
                CategoryValue(label="0-10", value=6, total=5),
                CategoryValue(label="10-20", value=2, total=10),
            ],
        )


def test_rate_limits_enforced() -> None:
    """Enforce rate bounds, paired CI bounds, and numerator <= denominator."""
    with pytest.raises(ValidationError, match="both ci_low and ci_high"):
        RateArtifact(presentation="range", rate=0.5, ci_low=0.4)
    with pytest.raises(ValidationError, match="ci_low <= rate <= ci_high"):
        RateArtifact(presentation="range", rate=0.5, ci_low=0.6, ci_high=0.8)
    with pytest.raises(ValidationError, match="numerator cannot exceed denominator"):
        RateArtifact(presentation="share_bar", rate=0.5, numerator=12, denominator=10)


def test_comparison_limits_enforced() -> None:
    """Enforce 8-row cap on dot plots/slopegraphs and 3-row cap on side_by_side."""
    with pytest.raises(ValidationError, match="at most 8 items"):
        ComparisonArtifact(
            presentation="dot_plot",
            before_label="A",
            after_label="B",
            unit="percent",
            rows=[ComparisonRow(label=f"r{i}", before=10, after=5) for i in range(9)],
        )
    with pytest.raises(ValidationError, match="at most 3 items"):
        ComparisonArtifact(
            presentation="side_by_side",
            before_label="Before",
            after_label="After",
            unit="percent",
            rows=[ComparisonRow(label=f"r{i}", before=10, after=5) for i in range(4)],
        )


def test_binned_contiguity_and_column_limit_enforced() -> None:
    """Enforce contiguous bins, bound ordering, and 12-bin cap."""
    with pytest.raises(ValidationError, match="equal the previous bin's upper bound"):
        BinnedArtifact(
            unit="ms",
            bins=[
                Bin(lower_bound=0, upper_bound=1, count=1),
                Bin(lower_bound=2, upper_bound=3, count=1),
            ],
        )
    with pytest.raises(ValidationError, match="Only the first bin"):
        BinnedArtifact(
            unit="ms",
            bins=[
                Bin(lower_bound=None, upper_bound=1, count=1),
                Bin(lower_bound=None, upper_bound=2, count=1),
            ],
        )
    with pytest.raises(ValidationError, match="Only the last bin"):
        BinnedArtifact(
            unit="ms",
            bins=[
                Bin(lower_bound=0, upper_bound=None, count=1),
                Bin(lower_bound=0, upper_bound=2, count=1),
            ],
        )
    with pytest.raises(ValidationError, match="less than its upper bound"):
        BinnedArtifact(
            unit="ms",
            bins=[
                Bin(lower_bound=1, upper_bound=1, count=1),
                Bin(lower_bound=1, upper_bound=2, count=1),
            ],
        )
    with pytest.raises(ValidationError):
        BinnedArtifact(
            unit="ms",
            bins=[Bin(lower_bound=i, upper_bound=i + 1, count=1) for i in range(13)],
        )


def test_session_dots_100_dot_limit() -> None:
    """Reject more than 100 dots on SessionDotsArtifact."""
    with pytest.raises(ValidationError):
        SessionDotsArtifact(
            unit="seconds",
            points=[
                SessionDot(session_id=uuid.uuid4(), value=float(i)) for i in range(101)
            ],
        )


def test_time_series_limits_enforced() -> None:
    """Enforce 3+ points, 5-line, 3-segment stacked, 6-panel, and 12-column caps."""
    with pytest.raises(ValidationError):
        TimeSeriesArtifact(
            presentation="line",
            unit="percent",
            x_labels=["a", "b"],
            series=[TimeSeriesLine(label="s1", values=[1, 2])],
        )
    with pytest.raises(ValidationError, match="at most 5 series"):
        TimeSeriesArtifact(
            presentation="line",
            unit="percent",
            x_labels=["d1", "d2", "d3"],
            series=[TimeSeriesLine(label=f"s{i}", values=[1, 2, 3]) for i in range(6)],
        )
    with pytest.raises(ValidationError, match="highlight_line requires exactly one"):
        TimeSeriesArtifact(
            presentation="highlight_line",
            unit="percent",
            x_labels=["d1", "d2", "d3"],
            series=[TimeSeriesLine(label=f"s{i}", values=[1, 2, 3]) for i in range(6)],
        )
    with pytest.raises(ValidationError, match="at most 3 segments"):
        TimeSeriesArtifact(
            presentation="stacked_columns",
            unit="sessions",
            x_labels=["d1", "d2", "d3"],
            series=[TimeSeriesLine(label=f"s{i}", values=[1, 2, 3]) for i in range(4)],
        )
    with pytest.raises(ValidationError, match="at most 12 time or version buckets"):
        TimeSeriesArtifact(
            presentation="ordered_columns",
            unit="sessions",
            x_labels=[f"v{i}" for i in range(13)],
            series=[TimeSeriesLine(label="s", values=list(range(13)))],
        )
    with pytest.raises(ValidationError, match="at most 6 panels"):
        TimeSeriesArtifact(
            presentation="small_multiples",
            unit="percent",
            x_labels=["d1", "d2", "d3"],
            series=[TimeSeriesLine(label=f"s{i}", values=[1, 2, 3]) for i in range(7)],
        )


def test_failure_matrix_6x6_and_sankey_3x8_limits() -> None:
    """Enforce 6x6 matrix cap and 3-stage/8-node Sankey cap."""
    with pytest.raises(ValidationError):
        FailureMatrixArtifact(
            rows=[f"r{i}" for i in range(7)],
            cols=["c1"],
            total_failed=1,
            cells=[FailureMatrixCell(from_state="r0", to_state="c1", count=1)],
        )
    with pytest.raises(ValidationError, match="at most 8 nodes"):
        FlowSankeyArtifact(
            stages=[
                SankeyStage(
                    label="S1",
                    nodes=[
                        SankeyNode(id=f"n{i}", label=f"n{i}", count=1) for i in range(5)
                    ],
                ),
                SankeyStage(
                    label="S2",
                    nodes=[
                        SankeyNode(id=f"m{i}", label=f"m{i}", count=1) for i in range(4)
                    ],
                ),
            ],
            links=[SankeyLink(source="n0", target="m0", count=1)],
        )


def test_step_strip_table_and_direct_model_constructors() -> None:
    """Validate step indices, unique table keys, and remaining model constructors."""
    with pytest.raises(ValidationError, match="cannot exceed total_steps"):
        StepStripArtifact(
            total_steps=5,
            rows=[StepStripRow(label="a", session_count=1, first_wrong_step=6)],
        )
    with pytest.raises(ValidationError, match="Table column keys must be unique"):
        TableArtifact(
            columns=[
                TableColumn(key="tool", label="Tool"),
                TableColumn(key="tool", label="Duplicate"),
            ],
            rows=[{"tool": "search"}],
        )
    assert (
        StepStoryArtifact(steps=[StoryStep(number=1, text="Called search")]).type
        == "step_story"
    )
    assert (
        ToolCycleArtifact(
            steps=[CycleStep(name="a"), CycleStep(name="b")],
            repeat_count=3,
            session_count=2,
        ).type
        == "tool_cycle"
    )
    assert (
        SessionComparisonArtifact(
            left=SessionSide(
                session_id=uuid.uuid4(),
                verdict="fail",
                steps=[AlignedStep(label="a", status="highlight_bad")],
            ),
            right=SessionSide(
                session_id=uuid.uuid4(),
                verdict="pass",
                steps=[AlignedStep(label="b", status="highlight_good")],
            ),
        ).type
        == "session_comparison"
    )
    assert (
        SessionTimelineArtifact(
            session_id=uuid.uuid4(),
            total_duration_ms=5000,
            spans=[TimelineSpan(name="search", start_ms=0, duration_ms=2000)],
        ).type
        == "session_timeline"
    )
    assert StatGridArtifact(items=[StatItem(label="New", value=1)]).type == "stat_grid"


def test_optional_artifact_title_supported_on_every_artifact_type() -> None:
    """Every artifact accepts an optional short block title and rejects blank/long."""
    for payload in ALL_ARTIFACT_PAYLOADS:
        titled = InsightInput(
            name="titled-check",
            title="Titled artifact check",
            findings=[
                InsightFinding(
                    id="f1",
                    title="Finding",
                    artifacts=[{**payload, "title": "Block title"}],
                )
            ],
        )
        assert titled.findings[0].artifacts[0].title == "Block title"

        untitled = InsightInput(
            name="untitled-check",
            title="Untitled artifact check",
            findings=[InsightFinding(id="f1", title="Finding", artifacts=[payload])],
        )
        assert untitled.findings[0].artifacts[0].title is None

    with pytest.raises(ValidationError):
        RateArtifact(rate=0.5, title="")
    with pytest.raises(ValidationError):
        RateArtifact(rate=0.5, title="x" * 121)
