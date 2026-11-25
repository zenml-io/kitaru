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
"""Typed visualization and evidence artifact blocks for Insight reports."""

import uuid
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import (
    DiscriminatedRequestModel,
    FiniteFloat,
    JsonValue,
    RequestModel,
)

Tone = Literal["neutral", "risk", "danger", "good", "warn"]
Severity = Literal["high", "medium", "low"]
FindingKind = Literal["finding", "bright_spot", "opportunity", "coverage_gap"]
ChangeStatus = Literal["new", "recurring", "narrowed", "resolved"]
Verdict = Literal["pass", "fail", "unclear"]
InsightStatus = Literal["new", "reviewed", "archived"]

_SPLIT_BAR_MIN_SHARE = 0.02


class StatItem(RequestModel):
    """Single headline count or value inside a stat grid."""

    label: str = Field(min_length=1, max_length=60, description="Metric label.")
    value: FiniteFloat | str = Field(description="Measured count or formatted value.")
    unit: str | None = Field(default=None, max_length=32, description="Optional unit.")
    tone: Tone = Field(default="neutral", description="Semantic emphasis tone.")
    chat_prompt: str | None = Field(
        default=None,
        max_length=1_000,
        description="Optional prompt when clicking this number to open chat.",
    )


class StatGridArtifact(DiscriminatedRequestModel):
    """One to six headline counts at a glance."""

    type: Literal["stat_grid"] = Field(default="stat_grid")
    items: list[StatItem] = Field(
        min_length=1, max_length=6, description="Headline stat tiles."
    )


class RateArtifact(DiscriminatedRequestModel):
    """Single rate with optional confidence range, or an x-of-y share bar."""

    type: Literal["rate"] = Field(default="rate")
    presentation: Literal["range", "share_bar"] | None = Field(
        default=None,
        description="Use 'range' for sampled rates with CI, 'share_bar' for x of y.",
    )
    rate: FiniteFloat = Field(ge=0, le=1, description="Rate between 0 and 1.")
    ci_low: FiniteFloat | None = Field(
        default=None, ge=0, le=1, description="Lower confidence bound."
    )
    ci_high: FiniteFloat | None = Field(
        default=None, ge=0, le=1, description="Upper confidence bound."
    )
    numerator: int | None = Field(
        default=None, ge=0, description="Matching count (renders 'x of y' when small)."
    )
    denominator: int | None = Field(
        default=None, ge=1, description="Total count behind the rate."
    )
    label: str | None = Field(
        default=None, max_length=120, description="Short rate label."
    )
    basis: str | None = Field(
        default=None, max_length=120, description="Sampling basis note."
    )
    tone: Tone = Field(default="risk", description="Semantic tone.")
    chat_prompt: str | None = Field(
        default=None,
        max_length=1_000,
        description="Optional prompt when clicking the number to open chat.",
    )

    @model_validator(mode="after")
    def _validate_bounds(self) -> Self:
        """Require consistent confidence interval and numerator/denominator bounds."""
        if (self.ci_low is None) != (self.ci_high is None):
            raise ValueError("Provide both ci_low and ci_high, or omit both.")
        if (
            self.ci_low is not None
            and self.ci_high is not None
            and not (self.ci_low <= self.rate <= self.ci_high)
        ):
            raise ValueError("Expected ci_low <= rate <= ci_high.")
        if (
            self.numerator is not None
            and self.denominator is not None
            and self.numerator > self.denominator
        ):
            raise ValueError("numerator cannot exceed denominator.")
        return self


class CategoryValue(RequestModel):
    """Category value."""

    label: str = Field(min_length=1, description="Category label.")
    value: FiniteFloat = Field(ge=0, description="Measured value.")
    total: FiniteFloat | None = Field(
        default=None,
        ge=0,
        description="Optional outer total for focus_bars subset-within-total.",
    )
    tone: Tone | None = Field(default=None, description="Optional semantic tone.")
    focus: bool = Field(default=False, description="Highlight this category.")
    chat_prompt: str | None = Field(
        default=None,
        max_length=1_000,
        description="Optional prompt when clicking this count to open chat.",
    )

    @model_validator(mode="after")
    def _validate_total(self) -> Self:
        """Require outer total to be at least the focused value when provided."""
        if self.total is not None and self.value > self.total:
            raise ValueError("Category value cannot exceed its outer total.")
        return self


class CategoricalArtifact(DiscriminatedRequestModel):
    """Ranked bars, focus-within-total, baseline-tick, path bars, split, or donut."""

    type: Literal["categorical"] = Field(default="categorical")
    presentation: (
        Literal[
            "ranked_bars",
            "focus_bars",
            "baseline_bars",
            "path_bars",
            "split_bar",
            "donut",
        ]
        | None
    ) = Field(default=None, description="Visual layout hint.")
    unit: str | None = Field(default=None, description="Unit of the values.")
    baseline_value: FiniteFloat | None = Field(
        default=None,
        ge=0,
        description="Overall baseline reference tick for baseline_bars.",
    )
    values: list[CategoryValue] = Field(
        min_length=2, description="Values per category (at least 2)."
    )

    @model_validator(mode="after")
    def _validate_limits(self) -> Self:
        """Enforce unique labels and chart-type row/segment limits."""
        labels = [value.label for value in self.values]
        if len(labels) != len(set(labels)):
            raise ValueError("Category labels must be unique.")
        count = len(self.values)
        if self.presentation == "donut" and not (3 <= count <= 4):
            raise ValueError(
                "Donut requires 3 to 4 parts; use 'rate' (share_bar) for 2 parts "
                "or 'ranked_bars' (up to 8 rows) for 5+ parts."
            )
        if self.presentation == "split_bar":
            if count > 4:
                raise ValueError(
                    "Split bar allows at most 4 segments; fold smaller segments "
                    "into 'Other' or switch to 'ranked_bars'."
                )
            total = sum(value.value for value in self.values)
            if total > 0 and any(
                0 < (value.value / total) < _SPLIT_BAR_MIN_SHARE
                for value in self.values
            ):
                raise ValueError(
                    "Every non-zero split_bar segment must be at least 2% of the "
                    "total; fold smaller segments into 'Other'."
                )
        if self.presentation in (None, "ranked_bars", "path_bars") and count > 8:
            raise ValueError(
                "Ranked and path bars allow at most 8 rows; fold the rest into "
                "'Other', always last."
            )
        return self


class ComparisonRow(RequestModel):
    """One before/after or two-group comparison row."""

    label: str = Field(min_length=1, max_length=120, description="Row label.")
    before: FiniteFloat = Field(description="Baseline or first-group value.")
    after: FiniteFloat = Field(description="Current or second-group value.")
    before_detail: str | None = Field(
        default=None,
        max_length=80,
        description="Exact baseline detail for tooltips (e.g. '8 of 210').",
    )
    after_detail: str | None = Field(
        default=None,
        max_length=80,
        description="Exact current detail for tooltips (e.g. '21 of 190').",
    )
    status: ChangeStatus | None = Field(
        default=None, description="Status badge for import_status rows."
    )
    tone: Tone | None = Field(default=None, description="Optional semantic tone.")
    focus: bool = Field(default=False, description="Highlight this row.")
    chat_prompt: str | None = Field(
        default=None,
        max_length=1_000,
        description="Optional prompt when clicking this row to open chat.",
    )


class ComparisonArtifact(DiscriminatedRequestModel):
    """Two-group or two-period comparison across one or more rows."""

    type: Literal["comparison"] = Field(default="comparison")
    presentation: (
        Literal[
            "side_by_side",
            "slopegraph",
            "dumbbell",
            "dot_plot",
            "split_pair",
            "import_status",
        ]
        | None
    ) = Field(default=None, description="Visual layout hint.")
    before_label: str = Field(
        min_length=1, max_length=60, description="Label for the baseline or group A."
    )
    after_label: str = Field(
        min_length=1, max_length=60, description="Label for the current or group B."
    )
    unit: str | None = Field(default=None, max_length=32, description="Shared unit.")
    rows: list[ComparisonRow] = Field(
        min_length=1, max_length=12, description="Comparison rows."
    )

    @model_validator(mode="after")
    def _validate_limits(self) -> Self:
        """Enforce row bounds per comparison layout."""
        if self.presentation in ("dot_plot", "slopegraph") and len(self.rows) > 8:
            raise ValueError(
                f"{self.presentation} allows at most 8 items; keep the top 8 by change."
            )
        if self.presentation == "side_by_side" and len(self.rows) > 3:
            raise ValueError(
                "Paired side_by_side bars allow at most 3 items; switch to "
                "'dot_plot' or 'slopegraph' (up to 8 items)."
            )
        return self


class TableColumn(RequestModel):
    """Typed column definition for a structured table."""

    key: str = Field(min_length=1, max_length=64, description="Row dict key.")
    label: str = Field(min_length=1, max_length=80, description="Column heading.")
    format: Literal[
        "text",
        "mono",
        "number",
        "percent",
        "percent_bar",
        "delta",
        "badge",
        "session_link",
    ] = Field(default="text", description="Cell formatting hint.")


class TableArtifact(DiscriminatedRequestModel):
    """Structured table with optional inline rate bars and session links."""

    type: Literal["table"] = Field(default="table")
    columns: list[TableColumn] = Field(
        min_length=1, max_length=10, description="Ordered table columns."
    )
    rows: list[dict[str, JsonValue]] = Field(
        min_length=1, max_length=50, description="Row records keyed by column key."
    )

    @model_validator(mode="after")
    def _validate_columns(self) -> Self:
        """Require unique column keys."""
        keys = [col.key for col in self.columns]
        if len(keys) != len(set(keys)):
            raise ValueError("Table column keys must be unique.")
        return self


class Bin(RequestModel):
    """Contiguous histogram bin."""

    lower_bound: FiniteFloat | None = Field(
        default=None,
        description="Inclusive lower bound, None on an open-ended first bin.",
    )
    upper_bound: FiniteFloat | None = Field(
        default=None,
        description="Exclusive upper bound, None on an open-ended last bin.",
    )
    count: int = Field(ge=0, description="Observations in the bin.")
    focus: bool = Field(default=False, description="Highlight this bin.")


class BinnedArtifact(DiscriminatedRequestModel):
    """Contiguous histogram for spread across ordered bins."""

    type: Literal["binned"] = Field(default="binned")
    presentation: Literal["columns", "horizontal", "micro"] | None = Field(
        default=None, description="Visual layout hint."
    )
    unit: str | None = Field(default=None, description="Unit of the values.")
    median: FiniteFloat | None = Field(default=None, description="Optional median.")
    p95: FiniteFloat | None = Field(default=None, description="Optional p95.")
    bins: list[Bin] = Field(
        min_length=2, max_length=12, description="Bins, in ascending order (2 to 12)."
    )

    @model_validator(mode="after")
    def _validate_bins(self) -> Self:
        """Require bins contiguous and sorted, with open bounds only at the ends."""
        last = len(self.bins) - 1
        previous_upper: float | None = None
        for index, bin_ in enumerate(self.bins):
            if bin_.lower_bound is None and index != 0:
                raise ValueError("Only the first bin may have an open lower bound.")
            if bin_.upper_bound is None and index != last:
                raise ValueError("Only the last bin may have an open upper bound.")
            if (
                bin_.lower_bound is not None
                and bin_.upper_bound is not None
                and bin_.lower_bound >= bin_.upper_bound
            ):
                raise ValueError("Bin lower bound must be less than its upper bound.")
            if index != 0 and bin_.lower_bound != previous_upper:
                raise ValueError(
                    "Bin lower bound must equal the previous bin's upper bound."
                )
            previous_upper = bin_.upper_bound
        return self


class SessionDot(RequestModel):
    """One session point on a dot strip."""

    session_id: uuid.UUID = Field(description="Session id.")
    session_label: str | None = Field(
        default=None, max_length=80, description="Short display label."
    )
    value: FiniteFloat = Field(description="Measured value for this session.")
    highlight: bool = Field(default=False, description="Highlight as outlier/focus.")


class SessionDotsArtifact(DiscriminatedRequestModel):
    """Dot strip showing every session when n <= 100; past 100 use binned."""

    type: Literal["session_dots"] = Field(default="session_dots")
    unit: str | None = Field(default=None, max_length=32, description="Value unit.")
    scale_min: FiniteFloat | None = Field(default=None, description="Optional min.")
    scale_max: FiniteFloat | None = Field(default=None, description="Optional max.")
    points: list[SessionDot] = Field(
        min_length=1,
        max_length=100,
        description="At most 100 sessions; past 100 switch to 'binned'.",
    )

    @model_validator(mode="after")
    def _validate_scale(self) -> Self:
        """Require scale_min < scale_max when both are supplied."""
        if (
            self.scale_min is not None
            and self.scale_max is not None
            and self.scale_min >= self.scale_max
        ):
            raise ValueError("scale_min must be less than scale_max.")
        return self


class TimeSeriesLine(RequestModel):
    """One named series over the shared x_labels."""

    label: str = Field(min_length=1, max_length=80, description="Series label.")
    values: list[FiniteFloat | None] = Field(
        min_length=3, description="Values aligned with x_labels."
    )
    tone: Tone | None = Field(default=None, description="Optional semantic tone.")
    focus: bool = Field(default=False, description="Highlight this series.")


class SeriesAnnotation(RequestModel):
    """Event marker placed at one x_labels index."""

    x_index: int = Field(ge=0, description="Zero-based index into x_labels.")
    label: str = Field(min_length=1, max_length=60, description="Marker label.")


class TimeSeriesArtifact(DiscriminatedRequestModel):
    """Change over 3+ points in time or versions on one shared scale."""

    type: Literal["time_series"] = Field(default="time_series")
    presentation: (
        Literal[
            "line",
            "highlight_line",
            "ordered_columns",
            "stacked_columns",
            "sparkline",
            "small_multiples",
        ]
        | None
    ) = Field(default=None, description="Visual layout hint.")
    x_labels: list[str] = Field(
        min_length=3,
        description="Ordered time or version labels (3+ points; use comparison for 2).",
    )
    unit: str | None = Field(
        default=None, max_length=32, description="Single shared unit (no dual axes)."
    )
    series: list[TimeSeriesLine] = Field(
        min_length=1, max_length=12, description="One or more series."
    )
    annotations: list[SeriesAnnotation] = Field(
        default_factory=list, max_length=6, description="Optional event markers."
    )

    @model_validator(mode="after")
    def _validate_limits(self) -> Self:
        """Enforce aligned lengths and Zuri's series/column limits."""
        x_len = len(self.x_labels)
        if any(len(line.values) != x_len for line in self.series):
            raise ValueError("Every series must match x_labels length.")
        if any(ann.x_index >= x_len for ann in self.annotations):
            raise ValueError("Annotation x_index must be within x_labels.")
        if self.presentation in ("ordered_columns", "stacked_columns") and x_len > 12:
            raise ValueError(
                "Column charts allow at most 12 time or version buckets; "
                "switch presentation to 'line'."
            )
        if self.presentation == "stacked_columns" and len(self.series) > 3:
            raise ValueError(
                "Stacked columns allow at most 3 segments; "
                "switch presentation to 'small_multiples'."
            )
        if self.presentation in (None, "line") and len(self.series) > 5:
            raise ValueError(
                "Direct-label line charts allow at most 5 series; "
                "switch presentation to 'highlight_line' with one focused series."
            )
        if (
            self.presentation == "highlight_line"
            and sum(line.focus for line in self.series) != 1
        ):
            raise ValueError(
                "highlight_line requires exactly one series with focus=True."
            )
        if self.presentation == "small_multiples" and len(self.series) > 6:
            raise ValueError("Small multiples allow at most 6 panels; keep the top 6.")
        return self


class StepStripRow(RequestModel):
    """One row of step blocks pinpointing last good and first wrong step."""

    label: str = Field(min_length=1, max_length=120, description="Cause or agent name.")
    session_count: int | None = Field(
        default=None, ge=0, description="Matching session count."
    )
    last_good_step: int | None = Field(
        default=None, ge=1, description="1-based last good step."
    )
    first_wrong_step: int | None = Field(
        default=None, ge=1, description="1-based first wrong step."
    )
    severity: Severity | None = Field(default=None, description="Optional severity.")
    session_ids: list[uuid.UUID] = Field(
        default_factory=list, max_length=50, description="Example sessions."
    )
    chat_prompt: str | None = Field(
        default=None,
        max_length=1_000,
        description="Optional prompt when clicking this count to open chat.",
    )


class StepStripArtifact(DiscriminatedRequestModel):
    """Where in a run failures start, as stacked rows or per-agent small multiples."""

    type: Literal["step_strip"] = Field(default="step_strip")
    presentation: Literal["rows", "small_multiples"] = Field(
        default="rows", description="Stacked rows or 2x3 small multiples grid."
    )
    total_steps: int = Field(
        ge=2, le=30, description="Number of step blocks on the shared scale."
    )
    rows: list[StepStripRow] = Field(
        min_length=1, max_length=12, description="Step strip rows."
    )

    @model_validator(mode="after")
    def _validate_limits(self) -> Self:
        """Enforce small-multiples cap and step bounds."""
        if self.presentation == "small_multiples" and len(self.rows) > 6:
            raise ValueError("Small multiples allow at most 6 panels; keep the top 6.")
        for row in self.rows:
            if row.last_good_step is not None and row.last_good_step > self.total_steps:
                raise ValueError("last_good_step cannot exceed total_steps.")
            if (
                row.first_wrong_step is not None
                and row.first_wrong_step > self.total_steps
            ):
                raise ValueError("first_wrong_step cannot exceed total_steps.")
        return self


class MatrixSessionRef(RequestModel):
    """Session reference inside a failure matrix cell."""

    session_id: uuid.UUID = Field(description="Session id.")
    session_label: str | None = Field(
        default=None, max_length=80, description="Short display label."
    )
    node_id: uuid.UUID | None = Field(default=None, description="Failing node id.")
    step_label: str | None = Field(
        default=None, max_length=40, description="Short step label (e.g. 'step 7')."
    )


class FailureMatrixCell(RequestModel):
    """One populated transition cell in a failure matrix."""

    from_state: str = Field(min_length=1, description="Last good step or tool.")
    to_state: str = Field(min_length=1, description="First wrong step or tool.")
    count: int = Field(ge=0, description="Failed sessions on this transition.")
    attempts: int | None = Field(
        default=None, ge=0, description="Total sessions traversing this transition."
    )
    sessions: list[MatrixSessionRef] = Field(
        default_factory=list, max_length=20, description="Drill-down sessions."
    )


class FailureMatrixArtifact(DiscriminatedRequestModel):
    """2D last-good x first-wrong transition heatmap, strictly max 6x6."""

    type: Literal["failure_matrix"] = Field(default="failure_matrix")
    state_by: Literal["tool", "span", "node", "step"] = Field(
        default="tool", description="Granularity of matrix states."
    )
    rows: list[str] = Field(
        min_length=1,
        max_length=6,
        description="Last good states (max 6; fold rare steps into 'Other').",
    )
    cols: list[str] = Field(
        min_length=1,
        max_length=6,
        description="First wrong states (max 6; fold rare steps into 'Other').",
    )
    total_failed: int = Field(ge=0, description="Total failed sessions.")
    cells: list[FailureMatrixCell] = Field(description="Populated transition cells.")

    @model_validator(mode="after")
    def _validate_cells(self) -> Self:
        """Require every cell to reference declared row and column states."""
        row_set = set(self.rows)
        col_set = set(self.cols)
        for cell in self.cells:
            if cell.from_state not in row_set or cell.to_state not in col_set:
                raise ValueError(
                    "Every failure_matrix cell must reference declared rows and cols."
                )
        return self


class SankeyNode(RequestModel):
    """One node inside a Sankey stage."""

    id: str = Field(min_length=1, max_length=64, description="Unique node id.")
    label: str = Field(min_length=1, max_length=80, description="Display label.")
    count: int = Field(ge=0, description="Sessions at this node.")
    tone: Tone = Field(default="neutral", description="Semantic tone.")


class SankeyStage(RequestModel):
    """One vertical stage in a Sankey flow."""

    label: str | None = Field(default=None, max_length=60, description="Stage label.")
    nodes: list[SankeyNode] = Field(
        min_length=1, max_length=8, description="Nodes in this stage."
    )


class SankeyLink(RequestModel):
    """Directed flow link between two Sankey nodes."""

    source: str = Field(min_length=1, description="Source node id.")
    target: str = Field(min_length=1, description="Target node id.")
    count: int = Field(ge=0, description="Sessions following this link.")
    tone: Tone = Field(default="neutral", description="Semantic tone.")


class FlowSankeyArtifact(DiscriminatedRequestModel):
    """Session split flow; strictly max 3 stages and 8 nodes, else use path_bars."""

    type: Literal["flow_sankey"] = Field(default="flow_sankey")
    stages: list[SankeyStage] = Field(
        min_length=2,
        max_length=3,
        description="2 to 3 flow stages; past 3 use categorical path_bars.",
    )
    links: list[SankeyLink] = Field(min_length=1, description="Links between nodes.")

    @model_validator(mode="after")
    def _validate_limits(self) -> Self:
        """Enforce max 8 unique nodes and valid link endpoints."""
        node_ids = [node.id for stage in self.stages for node in stage.nodes]
        if len(node_ids) > 8:
            raise ValueError(
                "Sankey allows at most 8 nodes across at most 3 stages; "
                "switch to categorical with presentation='path_bars'."
            )
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("Sankey node IDs must be unique.")
        known = set(node_ids)
        if any(
            link.source not in known or link.target not in known for link in self.links
        ):
            raise ValueError("Every Sankey link must reference known node IDs.")
        return self


class StoryStep(RequestModel):
    """One numbered step in a step story."""

    number: int = Field(ge=1, description="1-based step number.")
    text: str = Field(min_length=1, max_length=400, description="What happened.")
    role: Literal["user", "agent", "tool", "system"] | None = Field(
        default=None, description="Optional actor role."
    )
    tool_name: str | None = Field(
        default=None, max_length=120, description="Optional tool name."
    )
    node_id: uuid.UUID | None = Field(default=None, description="Optional node id.")
    tone: Tone = Field(default="neutral", description="Use 'danger' for the bad step.")
    note: str | None = Field(
        default=None, max_length=240, description="Optional inline annotation."
    )


class StepStoryArtifact(DiscriminatedRequestModel):
    """Numbered step-by-step story of a failure, with optional session link."""

    type: Literal["step_story"] = Field(default="step_story")
    session_id: uuid.UUID | None = Field(
        default=None, description="Optional example session id."
    )
    session_label: str | None = Field(
        default=None, max_length=80, description="Short display label."
    )
    steps: list[StoryStep] = Field(
        min_length=1, max_length=12, description="Ordered story steps."
    )


class CycleStep(RequestModel):
    """One tool in a repeating cycle."""

    name: str = Field(min_length=1, max_length=120, description="Tool name.")
    status: Literal["ok", "error"] = Field(default="ok", description="Call status.")


class ToolCycleArtifact(DiscriminatedRequestModel):
    """Repeating tool loop and its count."""

    type: Literal["tool_cycle"] = Field(default="tool_cycle")
    steps: list[CycleStep] = Field(
        min_length=1, max_length=6, description="Tools in the repeating cycle."
    )
    repeat_count: int = Field(ge=2, description="Maximum or typical repeat count.")
    session_count: int = Field(ge=1, description="Sessions showing this loop.")
    note: str | None = Field(
        default=None, max_length=200, description="Optional impact note."
    )


class AlignedStep(RequestModel):
    """One slot in a side-by-side session comparison."""

    label: str | None = Field(
        default=None, max_length=120, description="Step name, None for a missing slot."
    )
    node_id: uuid.UUID | None = Field(default=None, description="Optional node id.")
    status: Literal["present", "missing", "highlight_bad", "highlight_good"] = Field(
        default="present", description="Visual emphasis for this slot."
    )


class SessionSide(RequestModel):
    """One side (failing or counterexample/replay) of a session comparison."""

    session_id: uuid.UUID = Field(description="Session id.")
    session_label: str | None = Field(
        default=None, max_length=80, description="Short display label."
    )
    verdict: Verdict = Field(description="Outcome verdict.")
    steps: list[AlignedStep] = Field(
        min_length=1, max_length=15, description="Aligned steps."
    )


class SessionComparisonArtifact(DiscriminatedRequestModel):
    """Failing session side by side with a passing counterexample or replay."""

    type: Literal["session_comparison"] = Field(default="session_comparison")
    left: SessionSide = Field(description="Left session (typically failing).")
    right: SessionSide = Field(description="Right session (counterexample or replay).")


class TimelineSpan(RequestModel):
    """One timed span in a single session's waterfall."""

    name: str = Field(min_length=1, max_length=120, description="Step or tool name.")
    node_id: uuid.UUID | None = Field(default=None, description="Optional node id.")
    start_ms: int = Field(ge=0, description="Start offset in milliseconds.")
    duration_ms: int = Field(ge=0, description="Duration in milliseconds.")
    tone: Tone = Field(default="neutral", description="Semantic tone.")


class SessionTimelineArtifact(DiscriminatedRequestModel):
    """Duration waterfall for one session; never use for aggregate failure location."""

    type: Literal["session_timeline"] = Field(default="session_timeline")
    session_id: uuid.UUID = Field(description="Session id.")
    session_label: str | None = Field(
        default=None, max_length=80, description="Short display label."
    )
    total_duration_ms: int = Field(
        ge=1, description="Total session duration in milliseconds."
    )
    spans: list[TimelineSpan] = Field(
        min_length=1, max_length=40, description="Ordered spans."
    )


class EvidenceRow(RequestModel):
    """One session/node citation proving a finding."""

    session_id: uuid.UUID = Field(description="Session id.")
    session_label: str | None = Field(
        default=None, max_length=80, description="Short display label."
    )
    node_id: uuid.UUID | None = Field(default=None, description="Optional node id.")
    step_label: str | None = Field(
        default=None, max_length=40, description="Short step label (e.g. 'step 7')."
    )
    signal: str | None = Field(
        default=None, max_length=120, description="Tool or short signal label."
    )
    verdict: Verdict | None = Field(default=None, description="Optional verdict.")
    is_counterexample: bool = Field(
        default=False, description="Marks a passing counterexample row."
    )
    counterexample_note: str | None = Field(
        default=None, max_length=160, description="Why this counterexample differs."
    )


class EvidenceArtifact(DiscriminatedRequestModel):
    """Concrete sessions proving a finding as a step table or compact rows."""

    type: Literal["evidence"] = Field(default="evidence")
    presentation: Literal["table", "compact"] = Field(
        default="table", description="'table' for full R1 evidence, 'compact' for dots."
    )
    total_sessions: int = Field(
        ge=1, description="Total sessions behind this finding for 'Open all N'."
    )
    rows: list[EvidenceRow] = Field(
        min_length=1, max_length=25, description="Example evidence rows."
    )


class TextArtifact(DiscriminatedRequestModel):
    """Markdown prose, R2 callout banner, or labeled code/prompt hypothesis."""

    type: Literal["text"] = Field(default="text")
    style: Literal["body", "callout", "hypothesis"] = Field(
        default="body", description="Visual style."
    )
    label: str | None = Field(
        default=None, max_length=60, description="Optional banner/hypothesis label."
    )
    content: str = Field(min_length=1, description="Markdown content.")
    file_path: str | None = Field(
        default=None, max_length=300, description="Optional source file for hypothesis."
    )
    revision: str | None = Field(
        default=None, max_length=64, description="Optional git SHA for hypothesis."
    )


InsightArtifact = Annotated[
    StatGridArtifact
    | RateArtifact
    | CategoricalArtifact
    | ComparisonArtifact
    | TableArtifact
    | BinnedArtifact
    | SessionDotsArtifact
    | TimeSeriesArtifact
    | StepStripArtifact
    | FailureMatrixArtifact
    | FlowSankeyArtifact
    | StepStoryArtifact
    | ToolCycleArtifact
    | SessionComparisonArtifact
    | SessionTimelineArtifact
    | EvidenceArtifact
    | TextArtifact,
    Field(discriminator="type"),
]
