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
"""Structured Insight report API models."""

import uuid
from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from kitaru.api_models.v1.base import (
    JsonValue,
    OwnedResponseModel,
    RequestModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.insight_artifacts import (
    AlignedStep,
    Bin,
    BinnedArtifact,
    CategoricalArtifact,
    CategoryValue,
    ChangeStatus,
    ComparisonArtifact,
    ComparisonRow,
    CycleStep,
    EvidenceArtifact,
    EvidenceRow,
    FailureMatrixArtifact,
    FailureMatrixCell,
    FindingKind,
    FlowSankeyArtifact,
    InsightArtifact,
    InsightStatus,
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
    Severity,
    StatGridArtifact,
    StatItem,
    StepStoryArtifact,
    StepStripArtifact,
    StepStripRow,
    StoryStep,
    TableArtifact,
    TableColumn,
    TextArtifact,
    TimelineSpan,
    TimeSeriesArtifact,
    TimeSeriesLine,
    Tone,
    ToolCycleArtifact,
    Verdict,
)

MAX_INSIGHT_BATCH_SIZE = 100
MAX_FINDINGS_PER_INSIGHT = 30
MAX_ARTIFACTS_PER_FINDING = 12
MAX_SUGGESTED_QUESTIONS = 5
MAX_CONTRIBUTING_SESSIONS_PER_FINDING = 1000


class InsightCoverage(RequestModel):
    """Honest accounting of the sessions inspected by an insight report."""

    sessions_analyzed: int = Field(ge=0, description="Sessions read for the report.")
    sessions_available: int | None = Field(
        default=None, ge=0, description="Total sessions in scope before limits."
    )
    skipped: int = Field(default=0, ge=0, description="Sessions skipped.")
    failed: int = Field(default=0, ge=0, description="Sessions that failed to parse.")
    started_after: AwareDatetime | None = Field(
        default=None, description="Inclusive start of the analyzed time window."
    )
    started_before: AwareDatetime | None = Field(
        default=None, description="Exclusive end of the analyzed time window."
    )
    caveats: list[str] = Field(
        default_factory=list, max_length=10, description="Coverage caveats."
    )

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        """Keep analyzed count within available count when both are given."""
        if (
            self.sessions_available is not None
            and self.sessions_analyzed > self.sessions_available
        ):
            raise ValueError("sessions_analyzed cannot exceed sessions_available.")
        return self


class InsightAction(RequestModel):
    """Actionable handoff button attached to a finding or top-level next step."""

    label: str = Field(min_length=1, max_length=60, description="Short button label.")
    prompt: str = Field(
        min_length=1,
        max_length=16_000,
        description="Handoff briefing sent when the button is clicked.",
    )
    summary: str | None = Field(
        default=None,
        max_length=300,
        description="Short banner copy paired with the action.",
    )


class SuggestedQuestion(RequestModel):
    """Suggested follow-up chat prompt shown in the report sidebar."""

    label: str = Field(min_length=1, max_length=80, description="Short pill text.")
    prompt: str | None = Field(
        default=None,
        max_length=2_000,
        description="Optional fuller prompt sent to chat (defaults to label).",
    )


class InsightHighlight(RequestModel):
    """Selects which finding and artifact power the Grid view card."""

    finding_id: str = Field(min_length=1, max_length=120, description="Finding id.")
    artifact_index: int = Field(
        default=0, ge=0, description="Zero-based index into that finding's artifacts."
    )


class InsightFinding(RequestModel):
    """One ranked section inside an insight report."""

    id: str = Field(
        min_length=1, max_length=120, description="Stable slug within the report."
    )
    title: str = Field(min_length=1, max_length=255, description="Finding headline.")
    summary: str | None = Field(
        default=None,
        max_length=320,
        description="Plain-text one-sentence claim for compact lists and sidebar.",
    )
    body: str | None = Field(
        default=None, description="Markdown narrative with concrete details."
    )
    tier: Literal["primary", "secondary"] = Field(
        default="primary",
        description="'primary' for main findings, 'secondary' for baseline metrics.",
    )
    kind: FindingKind = Field(default="finding", description="Finding category.")
    severity: Severity | None = Field(default=None, description="Optional harm level.")
    change_status: ChangeStatus | None = Field(
        default=None, description="Status relative to earlier runs."
    )
    artifacts: list[InsightArtifact] = Field(
        default_factory=list,
        max_length=MAX_ARTIFACTS_PER_FINDING,
        description="Ordered typed visual and evidence blocks.",
    )
    contributing_session_ids: list[uuid.UUID] = Field(
        default_factory=list,
        max_length=MAX_CONTRIBUTING_SESSIONS_PER_FINDING,
        description="Session UUIDs supporting this finding.",
    )
    check_first: str | None = Field(
        default=None,
        max_length=1_000,
        description="First thing a human should verify.",
    )
    action: InsightAction | None = Field(
        default=None, description="Optional investigate handoff button."
    )
    suggested_questions: list[SuggestedQuestion] = Field(
        default_factory=list,
        max_length=MAX_SUGGESTED_QUESTIONS,
        description="Follow-up chat questions scoped to this finding.",
    )


class InsightInput(RequestModel):
    """Structured insight report input."""

    name: str = Field(min_length=1, max_length=255, description="Insight name.")
    title: str = Field(min_length=1, max_length=255, description="Report title.")
    summary: str | None = Field(
        default=None,
        max_length=320,
        description="One-sentence plain summary for grid and list views.",
    )
    source_provider: str | None = Field(
        default=None,
        max_length=80,
        description="Trace source label (e.g. langfuse, langsmith, recorded).",
    )
    coverage: InsightCoverage | None = Field(
        default=None, description="Session coverage accounting."
    )
    highlight: InsightHighlight | None = Field(
        default=None, description="Finding and artifact shown on the grid tile."
    )
    next_step: InsightAction | None = Field(
        default=None, description="Top-of-report recommended next step."
    )
    findings: list[InsightFinding] = Field(
        min_length=1,
        max_length=MAX_FINDINGS_PER_INSIGHT,
        description="Ordered findings inside this report.",
    )
    suggested_questions: list[SuggestedQuestion] = Field(
        default_factory=list,
        max_length=MAX_SUGGESTED_QUESTIONS,
        description="Report-level follow-up chat questions for the sidebar.",
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict,
        description="Opaque producer metadata (run receipts, seeds, versions).",
    )

    @model_validator(mode="after")
    def _validate_findings_and_highlight(self) -> Self:
        """Require unique finding IDs and a valid highlight pointer when set."""
        by_id: dict[str, InsightFinding] = {}
        for finding in self.findings:
            if finding.id in by_id:
                raise ValueError(f"Duplicate finding id '{finding.id}'.")
            by_id[finding.id] = finding
        if self.highlight is not None:
            target = by_id.get(self.highlight.finding_id)
            if target is None:
                raise ValueError(
                    f"Highlight finding_id '{self.highlight.finding_id}' does not "
                    "match any finding."
                )
            if (
                target.artifacts
                and self.highlight.artifact_index >= len(target.artifacts)
            ) or (not target.artifacts and self.highlight.artifact_index > 0):
                raise ValueError(
                    "Highlight artifact_index is out of range for the target finding."
                )
        return self


class InsightBatchCreateRequest(RequestModel):
    """Insight batch create request."""

    agent_id: uuid.UUID = Field(description="Agent the insights belong to.")
    insights: list[InsightInput] = Field(
        min_length=1,
        max_length=MAX_INSIGHT_BATCH_SIZE,
        description="Insight reports to create, in input order.",
    )


class InsightUpdateRequest(RequestModel):
    """Insight update request."""

    title: str | None = Field(default=None, description="New insight title.")
    summary: str | None = Field(default=None, description="New insight summary.")
    status: InsightStatus | None = Field(default=None, description="New review status.")


class InsightListParams(FilterableListParams):
    """Insight list params."""


class InsightResponse(OwnedResponseModel):
    """Structured insight report response."""

    id: uuid.UUID = Field(description="Insight id.")
    agent_id: uuid.UUID = Field(description="Agent the insight belongs to.")
    import_id: uuid.UUID | None = Field(
        default=None, description="Import whose sessions produced the insight."
    )
    task_id: uuid.UUID | None = Field(
        default=None, description="Analysis task that produced the insight."
    )
    analyzer_version_id: uuid.UUID | None = Field(
        default=None, description="Analyzer version that produced the insight."
    )
    analyzer_params: dict[str, JsonValue] | None = Field(
        default=None, description="Params the analyzer ran with."
    )
    source_provider: str | None = Field(default=None, description="Trace source label.")
    name: str = Field(description="Insight name.")
    title: str = Field(description="Insight title.")
    summary: str | None = Field(default=None, description="One-sentence summary.")
    status: InsightStatus = Field(default="new", description="Review status.")
    coverage: InsightCoverage | None = Field(
        default=None, description="Session coverage accounting."
    )
    highlight: InsightHighlight | None = Field(
        default=None, description="Finding and artifact shown on the grid tile."
    )
    next_step: InsightAction | None = Field(
        default=None, description="Top-of-report recommended next step."
    )
    findings: list[InsightFinding] = Field(
        description="Ordered findings inside this report."
    )
    suggested_questions: list[SuggestedQuestion] = Field(
        default_factory=list,
        description="Report-level follow-up chat questions.",
    )
    metadata: dict[str, JsonValue] = Field(description="Opaque producer metadata.")


__all__ = [
    "MAX_ARTIFACTS_PER_FINDING",
    "MAX_CONTRIBUTING_SESSIONS_PER_FINDING",
    "MAX_FINDINGS_PER_INSIGHT",
    "MAX_INSIGHT_BATCH_SIZE",
    "MAX_SUGGESTED_QUESTIONS",
    "AlignedStep",
    "Bin",
    "BinnedArtifact",
    "CategoricalArtifact",
    "CategoryValue",
    "ChangeStatus",
    "ComparisonArtifact",
    "ComparisonRow",
    "CycleStep",
    "EvidenceArtifact",
    "EvidenceRow",
    "FailureMatrixArtifact",
    "FailureMatrixCell",
    "FindingKind",
    "FlowSankeyArtifact",
    "InsightAction",
    "InsightArtifact",
    "InsightBatchCreateRequest",
    "InsightCoverage",
    "InsightFinding",
    "InsightHighlight",
    "InsightInput",
    "InsightListParams",
    "InsightResponse",
    "InsightStatus",
    "InsightUpdateRequest",
    "MatrixSessionRef",
    "RateArtifact",
    "SankeyLink",
    "SankeyNode",
    "SankeyStage",
    "SeriesAnnotation",
    "SessionComparisonArtifact",
    "SessionDot",
    "SessionDotsArtifact",
    "SessionSide",
    "SessionTimelineArtifact",
    "Severity",
    "StatGridArtifact",
    "StatItem",
    "StepStoryArtifact",
    "StepStripArtifact",
    "StepStripRow",
    "StoryStep",
    "SuggestedQuestion",
    "TableArtifact",
    "TableColumn",
    "TextArtifact",
    "TimeSeriesArtifact",
    "TimeSeriesLine",
    "TimelineSpan",
    "Tone",
    "ToolCycleArtifact",
    "Verdict",
]
