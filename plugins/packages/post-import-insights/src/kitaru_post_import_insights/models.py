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
"""Typed output contract for post-import insight report generation."""

import re
import uuid
from enum import StrEnum
from typing import Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from kitaru.api_models.v1.insight import InsightInput

INSIGHT_METADATA_KEY = "kitaru.insights/v1"
DISTRIBUTION_TOP_BIN_SIGNAL = "distribution-highest-occupied-bin"
MAX_CTA_LABEL_LENGTH = 60
DEFAULT_CTA_LABEL = "Investigate this finding"
DEFAULT_MAX_PRIMARY_FINDINGS = 8
MAX_PRIMARY_FINDINGS = 15
MAX_SECONDARY_FINDINGS = 6
MAX_INSIGHTS = MAX_PRIMARY_FINDINGS
MAX_EVIDENCE_LOCATORS = 20
MAX_CONTRIBUTING_SESSIONS = 1000
MAX_NAME_LENGTH = 255
MAX_SERVER_URL_LENGTH = 2048
MAX_INVESTIGATION_PROMPT_LENGTH = 16_000

FocusArea = Literal["tools", "cost", "language", "latency", "outcomes"]

_NAME_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_-]*[A-Za-z0-9])?$")


def _require_utf8(value: object) -> object:
    """Reject strings that cannot cross the JSON UTF-8 serialization boundary."""
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as error:
            raise ValueError("must be valid UTF-8 text") from error
    return value


class _InsightGenerationModel(BaseModel):
    """Base for generator-owned values embedded in Insight metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class GenerationMode(StrEnum):
    """How a generation result was produced."""

    DETERMINISTIC = "deterministic"
    MODEL_BACKED = "model_backed"
    DETERMINISTIC_FALLBACK = "deterministic_fallback"


class SourceImportContext(_InsightGenerationModel):
    """Stable context for the one import that created every analyzed session."""

    import_id: uuid.UUID = Field(description="Import that created the sessions.")
    provider: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_NAME_LENGTH,
        description="Source provider recorded on the sessions, when available.",
    )

    _validate_provider_utf8 = field_validator("provider", mode="before")(_require_utf8)


class InsightGenerationContext(_InsightGenerationModel):
    """Identity and source context for one generation run."""

    agent_id: uuid.UUID = Field(description="Agent the generated Insight belongs to.")
    agent_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_NAME_LENGTH,
        description="Human-readable agent name, when supplied by the caller.",
    )
    source_import: SourceImportContext | None = Field(
        default=None,
        description=(
            "Import that created every analyzed session, or None when the sessions "
            "were recorded live or came from more than one import."
        ),
    )
    source_provider: str | None = Field(
        default=None,
        min_length=1,
        max_length=80,
        description="Trace provider label shared across analyzed sessions, when known.",
    )
    server_url: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_SERVER_URL_LENGTH,
        description="Kitaru server holding the analyzed sessions, when known.",
    )

    _validate_text_fields_utf8 = field_validator(
        "agent_name", "source_provider", "server_url", mode="before"
    )(_require_utf8)


class CoverageTruncation(_InsightGenerationModel):
    """One input dimension reduced by a configured generation bound."""

    dimension: str = Field(min_length=1, max_length=80)
    available: int = Field(ge=0)
    analyzed: int = Field(ge=0)

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        """Require the analyzed count not to exceed the available count."""
        if self.analyzed > self.available:
            raise ValueError("truncation analyzed count exceeds available count")
        return self


class Coverage(_InsightGenerationModel):
    """Honest accounting of the normalized input inspected by the generator."""

    sessions_available: int = Field(default=0, ge=0)
    sessions_analyzed: int = Field(default=0, ge=0)
    nodes_available: int = Field(default=0, ge=0)
    nodes_analyzed: int = Field(default=0, ge=0)
    inspected_text_bytes: int = Field(default=0, ge=0)
    started_after: AwareDatetime | None = None
    started_before: AwareDatetime | None = None
    truncations: list[CoverageTruncation] = Field(default_factory=list, max_length=10)
    caveats: list[str] = Field(default_factory=list, max_length=10)

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        """Keep analyzed coverage within the corresponding available input."""
        if self.sessions_analyzed > self.sessions_available:
            raise ValueError("analyzed sessions exceed available sessions")
        if self.nodes_analyzed > self.nodes_available:
            raise ValueError("analyzed nodes exceed available nodes")
        return self


class EvidenceLocator(_InsightGenerationModel):
    """Opaque pointer to source evidence without retaining source content."""

    session_id: uuid.UUID = Field(description="Session containing the evidence.")
    node_id: uuid.UUID | None = Field(
        default=None,
        description="Specific node containing the evidence, when applicable.",
    )
    signal: str = Field(
        min_length=1,
        max_length=80,
        description="Deterministic signal that selected this location.",
    )


class GenerationVersions(_InsightGenerationModel):
    """Versions needed to reproduce deterministic analysis and prompt assembly."""

    analysis: str = Field(min_length=1, max_length=80)
    prompt: str = Field(min_length=1, max_length=80)


class FindingMetadata(_InsightGenerationModel):
    """Validated per-finding provenance stored under report metadata."""

    eyebrow: str = Field(min_length=1, max_length=80)
    position: int = Field(ge=0)
    recommended: bool = False
    cta_label: str = Field(
        default=DEFAULT_CTA_LABEL,
        strict=True,
        min_length=1,
        max_length=MAX_CTA_LABEL_LENGTH,
    )

    @field_validator("cta_label")
    @classmethod
    def _validate_cta_label(cls, value: str) -> str:
        _require_utf8(value)
        if not value.strip():
            raise ValueError("CTA label must not be blank")
        return value


class InsightReportMetadata(_InsightGenerationModel):
    """Validated report-level provenance attached under `kitaru.insights/v1`."""

    schema_version: Literal["2"] = "2"
    recommended_finding_id: str = Field(min_length=1, max_length=120)
    omitted_candidates: list[str] = Field(default_factory=list, max_length=30)
    coverage: Coverage
    context: InsightGenerationContext
    generation: GenerationVersions
    findings: dict[str, FindingMetadata] = Field(min_length=1)


class PageIntro(_InsightGenerationModel):
    """Editorial copy introducing the generated report."""

    eyebrow: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    description: str = Field(min_length=1, max_length=1000)


class PageRecommendation(_InsightGenerationModel):
    """Report-level recommendation pointing to the top finding."""

    insight_name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    title: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    description: str = Field(min_length=1, max_length=1000)


class ProviderReceipt(_InsightGenerationModel):
    """Sanitized operational receipt for one provider request."""

    stage: Literal["analyst", "editor"]
    request_id: str | None = Field(default=None, min_length=1, max_length=255)
    model: str | None = Field(default=None, min_length=1, max_length=255)
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    latency_ms: int = Field(ge=0)
    outcome: Literal["succeeded", "failed", "timed_out"]

    _validate_provider_text_utf8 = field_validator(
        "request_id", "model", mode="before"
    )(_require_utf8)


class GenerationDiagnostics(_InsightGenerationModel):
    """Bounded, content-free diagnostics for one generation run."""

    provider_receipts: list[ProviderReceipt] = Field(
        default_factory=list,
        max_length=2,
    )
    warnings: list[str] = Field(default_factory=list, max_length=10)
    fallback_reason: str | None = Field(default=None, min_length=1, max_length=1000)

    @model_validator(mode="after")
    def _validate_receipts(self) -> Self:
        """Allow at most one provider request receipt for each model stage."""
        stages = [receipt.stage for receipt in self.provider_receipts]
        if len(stages) != len(set(stages)):
            raise ValueError("provider receipt stages must be unique")
        return self


class InsightGenerationResult(_InsightGenerationModel):
    """Serializable output of one post-import insight generation run."""

    context: InsightGenerationContext
    coverage: Coverage
    mode: GenerationMode
    page_intro: PageIntro | None = None
    recommendation: PageRecommendation | None = None
    diagnostics: GenerationDiagnostics = Field(default_factory=GenerationDiagnostics)
    empty_reason: str | None = Field(default=None, min_length=1, max_length=1000)
    insights: list[InsightInput] = Field(default_factory=list, max_length=1)

    @staticmethod
    def report_metadata(insight: InsightInput) -> InsightReportMetadata:
        """Parse the namespaced provenance attached to a generated report."""
        if set(insight.metadata) != {INSIGHT_METADATA_KEY}:
            raise ValueError(
                f"Insight metadata must contain only {INSIGHT_METADATA_KEY!r}"
            )
        return InsightReportMetadata.model_validate(
            insight.metadata[INSIGHT_METADATA_KEY]
        )

    @model_validator(mode="after")
    def _validate_result(self) -> Self:
        """Validate structured report identity, findings, and recommendation."""
        if not self.insights:
            if self.empty_reason is None:
                raise ValueError("an empty result requires an empty reason")
            if self.page_intro is not None or self.recommendation is not None:
                raise ValueError("an empty result cannot include page copy")
            return self

        if self.empty_reason is not None:
            raise ValueError("a non-empty result cannot include an empty reason")
        if self.page_intro is None:
            raise ValueError("a non-empty result requires a page intro")
        if self.recommendation is None:
            raise ValueError("a non-empty result requires a recommendation")

        report = self.insights[0]
        if not 1 <= len(report.name) <= MAX_NAME_LENGTH or not _NAME_PATTERN.fullmatch(
            report.name
        ):
            raise ValueError("Insight name violates domain name constraints")
        if not 1 <= len(report.title) <= MAX_NAME_LENGTH:
            raise ValueError("Insight title violates domain title constraints")
        if not report.findings:
            raise ValueError("generated Insight report must contain findings")

        metadata = self.report_metadata(report)
        if metadata.context != self.context:
            raise ValueError("report context must match generation context")
        if metadata.coverage != self.coverage:
            raise ValueError("report coverage must match result coverage")

        finding_ids = [finding.id for finding in report.findings]
        if set(metadata.findings) != set(finding_ids):
            raise ValueError("report metadata findings must match report.findings")

        positions = [metadata.findings[fid].position for fid in finding_ids]
        if positions != list(range(len(finding_ids))):
            raise ValueError("finding positions must match list order")

        recommended_ids = [
            fid for fid in finding_ids if metadata.findings[fid].recommended
        ]
        if len(recommended_ids) != 1:
            raise ValueError("exactly one finding must be recommended")
        if metadata.recommended_finding_id != recommended_ids[0]:
            raise ValueError("recommended_finding_id must match recommended finding")
        if self.recommendation.insight_name != recommended_ids[0]:
            raise ValueError("page recommendation must match the recommended finding")

        for finding in report.findings:
            if not finding.artifacts:
                raise ValueError(
                    "each generated finding must contain at least one artifact"
                )
        return self
