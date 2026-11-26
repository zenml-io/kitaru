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
"""Kitchen-sink showcase report covering all 17 InsightArtifact types."""

from insight_showcase_data import SeededSession

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
    MatrixSessionRef,
    RateArtifact,
    SankeyLink,
    SankeyNode,
    SankeyStage,
    SeriesAnnotation,
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


def build_all_17_artifacts_report(sessions: list[SeededSession]) -> InsightInput:
    """Build one showcase report containing every single InsightArtifact type."""
    s1, s2, s3, s4 = sessions[:4]
    return InsightInput(
        name="all-17-artifacts-showcase",
        title="All 17 InsightArtifact types showcase",
        summary="Visual test fixture covering every typed artifact and chart layout.",
        source_provider="Langfuse",
        coverage=InsightCoverage(
            sessions_analyzed=214,
            sessions_available=250,
            skipped=33,
            failed=3,
            caveats=["Synthetic showcase fixture referencing real seeded sessions."],
        ),
        highlight=InsightHighlight(finding_id="single-numbers", artifact_index=0),
        next_step=InsightAction(
            label="Inspect chart renderers",
            summary="Verify all 17 artifact types render cleanly in R1 and R2",
            prompt="Inspect all 17 typed artifact renderers on this showcase report.",
        ),
        findings=[
            InsightFinding(
                id="single-numbers",
                title="1. Single number blocks (stat_grid + rate)",
                summary="Big number tiles, sampled rate with CI, and x-of-y share bar.",
                artifacts=[
                    StatGridArtifact(
                        title="Run overview",
                        items=[
                            StatItem(label="New", value=1, tone="warn"),
                            StatItem(label="Recurring", value=2, tone="neutral"),
                            StatItem(label="Resolved", value=1, tone="good"),
                            StatItem(label="Sessions", value=214, unit="sessions"),
                        ],
                    ),
                    RateArtifact(
                        title="Sampled failure rate",
                        presentation="range",
                        rate=0.14,
                        ci_low=0.10,
                        ci_high=0.20,
                        numerator=31,
                        denominator=214,
                        label="Policy check skipped",
                        basis="Every session read",
                        tone="risk",
                    ),
                    RateArtifact(
                        title="Affected session share",
                        presentation="share_bar",
                        rate=31 / 214,
                        numerator=31,
                        denominator=214,
                        label="31 of 214 sessions",
                        tone="warn",
                    ),
                ],
            ),
            InsightFinding(
                id="categorical-and-compare",
                title="2. Compare & part-of-a-whole (categorical + comparison + table)",
                summary="Ranked bars, baseline bars, donut, dot plot, and table.",
                artifacts=[
                    CategoricalArtifact(
                        title="Failure causes",
                        presentation="ranked_bars",
                        unit="sessions",
                        values=[
                            CategoryValue(
                                label="Policy check skipped",
                                value=24,
                                tone="danger",
                                focus=True,
                            ),
                            CategoryValue(
                                label="Wrong order looked up", value=14, tone="risk"
                            ),
                            CategoryValue(label="Tool error", value=9, tone="warn"),
                            CategoryValue(label="Other", value=5, tone="neutral"),
                        ],
                    ),
                    CategoricalArtifact(
                        title="Skip rate by order edit",
                        presentation="baseline_bars",
                        unit="percent",
                        baseline_value=14,
                        values=[
                            CategoryValue(
                                label="Order edited", value=66, tone="warn", focus=True
                            ),
                            CategoryValue(label="No order edit", value=0),
                            CategoryValue(label="All sessions", value=14),
                        ],
                    ),
                    CategoricalArtifact(
                        title="Session outcomes",
                        presentation="donut",
                        unit="sessions",
                        values=[
                            CategoryValue(label="Resolved", value=158, tone="good"),
                            CategoryValue(label="Escalated", value=41, tone="warn"),
                            CategoryValue(label="Abandoned", value=15, tone="neutral"),
                        ],
                    ),
                    ComparisonArtifact(
                        title="Outcomes with vs. without order edit",
                        presentation="dot_plot",
                        before_label="No order edit",
                        after_label="Order edited",
                        unit="percent",
                        rows=[
                            ComparisonRow(
                                label="Failed",
                                before=12,
                                after=66,
                                before_detail="20 of 167",
                                after_detail="31 of 47",
                                tone="danger",
                                focus=True,
                            ),
                            ComparisonRow(label="Escalated", before=14, after=38),
                            ComparisonRow(label="Abandoned", before=7, after=9),
                        ],
                    ),
                    ComparisonArtifact(
                        title="Change since last import",
                        presentation="import_status",
                        before_label="Oct 12",
                        after_label="Oct 19",
                        unit="percent",
                        rows=[
                            ComparisonRow(
                                label="Policy check skipped",
                                before=0,
                                after=14,
                                status="new",
                                tone="danger",
                            ),
                            ComparisonRow(
                                label="Wrong order looked up",
                                before=11,
                                after=7,
                                status="narrowed",
                                tone="warn",
                            ),
                            ComparisonRow(
                                label="Stale cache hit",
                                before=9,
                                after=0,
                                status="resolved",
                                tone="good",
                            ),
                        ],
                    ),
                    TableArtifact(
                        title="Per-tool failure rates",
                        columns=[
                            TableColumn(key="tool", label="Tool", format="mono"),
                            TableColumn(key="calls", label="Calls", format="number"),
                            TableColumn(
                                key="failures", label="Failures", format="number"
                            ),
                            TableColumn(
                                key="rate", label="Failure rate", format="percent_bar"
                            ),
                        ],
                        rows=[
                            {
                                "tool": "search",
                                "calls": 1840,
                                "failures": 112,
                                "rate": 6.1,
                            },
                            {
                                "tool": "fetch_page",
                                "calls": 1310,
                                "failures": 40,
                                "rate": 3.1,
                            },
                            {
                                "tool": "run_sql",
                                "calls": 420,
                                "failures": 9,
                                "rate": 2.1,
                            },
                        ],
                    ),
                ],
            ),
            InsightFinding(
                id="distributions-and-trends",
                title="3. Distributions & trends (binned + session_dots + time_series)",
                summary="Histogram columns, per-session dot strip, and trend lines.",
                artifacts=[
                    BinnedArtifact(
                        title="Session duration distribution",
                        presentation="columns",
                        unit="seconds",
                        median=4.2,
                        p95=18.5,
                        bins=[
                            Bin(lower_bound=0, upper_bound=5, count=112),
                            Bin(lower_bound=5, upper_bound=10, count=64),
                            Bin(lower_bound=10, upper_bound=20, count=26),
                            Bin(lower_bound=20, upper_bound=None, count=12, focus=True),
                        ],
                    ),
                    SessionDotsArtifact(
                        title="Slowest individual sessions",
                        unit="seconds",
                        scale_min=0,
                        scale_max=25,
                        points=[
                            SessionDot(
                                session_id=s1.session_id,
                                session_label=s1.label,
                                value=21.4,
                                highlight=True,
                            ),
                            SessionDot(
                                session_id=s2.session_id,
                                session_label=s2.label,
                                value=16.8,
                                highlight=True,
                            ),
                            SessionDot(
                                session_id=s3.session_id,
                                session_label=s3.label,
                                value=5.2,
                            ),
                            SessionDot(
                                session_id=s4.session_id,
                                session_label=s4.label,
                                value=3.1,
                            ),
                        ],
                    ),
                    TimeSeriesArtifact(
                        title="Failure rate over time",
                        presentation="highlight_line",
                        x_labels=["Oct 1", "Oct 5", "Oct 10", "Oct 15", "Oct 19"],
                        unit="percent",
                        series=[
                            TimeSeriesLine(
                                label="Policy check skipped",
                                values=[2, 3, 4, 11, 14],
                                tone="danger",
                                focus=True,
                            ),
                            TimeSeriesLine(
                                label="Wrong order looked up", values=[7, 6, 7, 6, 7]
                            ),
                            TimeSeriesLine(label="Tool error", values=[5, 4, 5, 4, 4]),
                        ],
                        annotations=[
                            SeriesAnnotation(x_index=3, label="prompt v4 shipped")
                        ],
                    ),
                    TimeSeriesArtifact(
                        title="Daily session outcomes",
                        presentation="stacked_columns",
                        x_labels=["Oct 14", "Oct 15", "Oct 16", "Oct 17", "Oct 18"],
                        unit="sessions",
                        series=[
                            TimeSeriesLine(
                                label="Resolved",
                                values=[30, 32, 31, 33, 32],
                                tone="good",
                            ),
                            TimeSeriesLine(
                                label="Escalated", values=[8, 9, 8, 7, 9], tone="warn"
                            ),
                            TimeSeriesLine(
                                label="Abandoned",
                                values=[3, 2, 4, 3, 3],
                                tone="neutral",
                            ),
                        ],
                    ),
                ],
            ),
            InsightFinding(
                id="steps-and-flow",
                title="4. Steps & flow (7 step, matrix, flow, and timeline blocks)",
                summary=(
                    "Step strip, failure matrix, Sankey, story, cycle, diff, waterfall."
                ),
                artifacts=[
                    StepStripArtifact(
                        title="Where failures start by step",
                        presentation="rows",
                        total_steps=10,
                        rows=[
                            StepStripRow(
                                label="Policy check skipped",
                                session_count=24,
                                last_good_step=6,
                                first_wrong_step=7,
                                severity="high",
                                session_ids=[s1.session_id, s2.session_id],
                            )
                        ],
                    ),
                    FailureMatrixArtifact(
                        title="Last good to first failing tool",
                        state_by="tool",
                        rows=["edit_order", "lookup_order", "search"],
                        cols=["issue_refund", "fetch_page", "escalate"],
                        total_failed=47,
                        cells=[
                            FailureMatrixCell(
                                from_state="edit_order",
                                to_state="issue_refund",
                                count=24,
                                attempts=47,
                                sessions=[
                                    MatrixSessionRef(
                                        session_id=s1.session_id,
                                        session_label=s1.label,
                                        node_id=s1.node_id,
                                        step_label="step 7",
                                    )
                                ],
                            ),
                            FailureMatrixCell(
                                from_state="lookup_order",
                                to_state="issue_refund",
                                count=14,
                                attempts=80,
                            ),
                            FailureMatrixCell(
                                from_state="search",
                                to_state="fetch_page",
                                count=9,
                                attempts=120,
                            ),
                        ],
                    ),
                    FlowSankeyArtifact(
                        title="Order edit to outcome flow",
                        stages=[
                            SankeyStage(
                                label="Entry",
                                nodes=[
                                    SankeyNode(
                                        id="edit", label="Order edited", count=47
                                    ),
                                    SankeyNode(id="plain", label="No edit", count=167),
                                ],
                            ),
                            SankeyStage(
                                label="Check",
                                nodes=[
                                    SankeyNode(
                                        id="checked",
                                        label="Policy checked",
                                        count=183,
                                        tone="good",
                                    ),
                                    SankeyNode(
                                        id="skipped",
                                        label="Check skipped",
                                        count=31,
                                        tone="danger",
                                    ),
                                ],
                            ),
                            SankeyStage(
                                label="Outcome",
                                nodes=[
                                    SankeyNode(
                                        id="pass", label="Pass", count=190, tone="good"
                                    ),
                                    SankeyNode(
                                        id="fail", label="Fail", count=24, tone="danger"
                                    ),
                                ],
                            ),
                        ],
                        links=[
                            SankeyLink(
                                source="edit", target="skipped", count=31, tone="danger"
                            ),
                            SankeyLink(
                                source="edit", target="checked", count=16, tone="good"
                            ),
                            SankeyLink(
                                source="plain", target="checked", count=167, tone="good"
                            ),
                            SankeyLink(
                                source="checked", target="pass", count=183, tone="good"
                            ),
                            SankeyLink(
                                source="skipped", target="fail", count=24, tone="danger"
                            ),
                            SankeyLink(
                                source="skipped", target="pass", count=7, tone="warn"
                            ),
                        ],
                    ),
                    StepStoryArtifact(
                        title="How it happened",
                        session_id=s1.session_id,
                        session_label=s1.label,
                        steps=[
                            StoryStep(
                                number=5,
                                role="tool",
                                tool_name="edit_order",
                                text="Updated shipping address on ORD-4200.",
                            ),
                            StoryStep(
                                number=6,
                                role="agent",
                                text="Skipped policy check assuming prior check held.",
                                tone="warn",
                            ),
                            StoryStep(
                                number=7,
                                role="tool",
                                tool_name="issue_refund",
                                node_id=s1.node_id,
                                text="Issued full refund without check_refund_policy.",
                                tone="danger",
                                note="Scored Fail on policy_adherence",
                            ),
                        ],
                    ),
                    ToolCycleArtifact(
                        title="Repeating tool loop",
                        steps=[
                            CycleStep(name="search", status="ok"),
                            CycleStep(name="fetch_page", status="error"),
                        ],
                        repeat_count=14,
                        session_count=23,
                        note="Repeats up to 14x when search returns stale URLs.",
                    ),
                    SessionComparisonArtifact(
                        title="Failing vs. passing session",
                        left=SessionSide(
                            session_id=s1.session_id,
                            session_label=s1.label,
                            verdict="fail",
                            steps=[
                                AlignedStep(label="lookup_order", status="present"),
                                AlignedStep(label="edit_order", status="present"),
                                AlignedStep(label=None, status="missing"),
                                AlignedStep(
                                    label="issue_refund",
                                    node_id=s1.node_id,
                                    status="highlight_bad",
                                ),
                            ],
                        ),
                        right=SessionSide(
                            session_id=s4.session_id,
                            session_label=s4.label,
                            verdict="pass",
                            steps=[
                                AlignedStep(label="lookup_order", status="present"),
                                AlignedStep(label="edit_order", status="present"),
                                AlignedStep(
                                    label="check_refund_policy",
                                    node_id=s4.node_id,
                                    status="highlight_good",
                                ),
                                AlignedStep(label="issue_refund", status="present"),
                            ],
                        ),
                    ),
                    SessionTimelineArtifact(
                        title="Single-session latency waterfall",
                        session_id=s1.session_id,
                        session_label=s1.label,
                        total_duration_ms=11_400,
                        spans=[
                            TimelineSpan(
                                name="lookup_order", start_ms=0, duration_ms=1_200
                            ),
                            TimelineSpan(
                                name="edit_order", start_ms=1_400, duration_ms=2_800
                            ),
                            TimelineSpan(
                                name="issue_refund",
                                node_id=s1.node_id,
                                start_ms=4_500,
                                duration_ms=6_900,
                                tone="danger",
                            ),
                        ],
                    ),
                ],
            ),
            InsightFinding(
                id="evidence-and-text",
                title="5. Evidence & text blocks (evidence + text)",
                summary="Compact evidence rows plus callout and hypothesis.",
                artifacts=[
                    EvidenceArtifact(
                        title="Evidence",
                        presentation="compact",
                        total_sessions=31,
                        rows=[
                            EvidenceRow(
                                session_id=s1.session_id,
                                session_label=s1.label,
                                step_label="step 7",
                                signal="issue_refund",
                                verdict="fail",
                            ),
                            EvidenceRow(
                                session_id=s4.session_id,
                                session_label=s4.label,
                                step_label="step 6",
                                signal="check_refund_policy",
                                verdict="pass",
                                is_counterexample=True,
                            ),
                        ],
                    ),
                    TextArtifact(
                        style="callout",
                        label="Next step",
                        content="Call `check_refund_policy` after every `edit_order`.",
                    ),
                    TextArtifact(
                        style="hypothesis",
                        label="Prompt diff hypothesis",
                        content=(
                            "Add a guard in `refund_prompt.md`: whenever `edit_order` "
                            "succeeds, clear cached eligibility checks."
                        ),
                        file_path="agents/meridian/refund_prompt.md",
                        revision="HEAD",
                    ),
                ],
            ),
        ],
        suggested_questions=[
            SuggestedQuestion(label="Which chart types are shown here?"),
            SuggestedQuestion(label="Compare the failing and passing sessions"),
        ],
    )
