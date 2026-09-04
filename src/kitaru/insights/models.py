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
"""Typed output contract for post-import insight generation."""

import re
import uuid
from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.api_models.v1.insight import (
    BinnedInsightData,
    CategoricalInsightData,
    InsightInput,
)

INSIGHT_METADATA_KEY = "kitaru.insights/v1"
MAX_INSIGHTS = 6
MAX_EVIDENCE_LOCATORS = 20
MAX_CONTRIBUTING_SESSIONS = 1000
MAX_NAME_LENGTH = 255
MAX_INVESTIGATION_PROMPT_LENGTH = 16_000

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
    """Base for generator-owned values embedded in arbitrary Insight metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class GenerationMode(StrEnum):
    """How a generation result was produced."""

    DETERMINISTIC = "deterministic"
    MODEL_BACKED = "model_backed"
    DETERMINISTIC_FALLBACK = "deterministic_fallback"


class SourceImportContext(_InsightGenerationModel):
    """Stable context for the import whose sessions were analyzed."""

    task_id: uuid.UUID = Field(description="Import task that created the sessions.")
    provider: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_NAME_LENGTH,
        description="Source provider recorded on the import task, when available.",
    )

    _validate_provider_utf8 = field_validator("provider", mode="before")(_require_utf8)


class InsightGenerationContext(_InsightGenerationModel):
    """Caller-supplied identity and source context for one generation run."""

    agent_id: uuid.UUID = Field(description="Agent the generated Insights belong to.")
    agent_name: str = Field(
        min_length=1,
        max_length=MAX_NAME_LENGTH,
        description="Human-readable agent name used in investigation prompts.",
    )
    source_import: SourceImportContext = Field(
        description="Import scope whose normalized sessions were analyzed."
    )

    _validate_agent_name_utf8 = field_validator("agent_name", mode="before")(
        _require_utf8
    )


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


class InsightCardMetadata(_InsightGenerationModel):
    """Validated renderer and investigation context for one generated card."""

    schema_version: Literal["1"] = "1"
    eyebrow: str = Field(min_length=1, max_length=80)
    position: int = Field(ge=0, lt=MAX_INSIGHTS)
    recommended: bool = False
    contributing_session_ids: list[uuid.UUID] = Field(
        min_length=1,
        max_length=MAX_CONTRIBUTING_SESSIONS,
    )
    evidence: list[EvidenceLocator] = Field(
        default_factory=list,
        max_length=MAX_EVIDENCE_LOCATORS,
    )
    coverage: Coverage
    investigation_prompt: str = Field(
        min_length=1, max_length=MAX_INVESTIGATION_PROMPT_LENGTH
    )
    context: InsightGenerationContext
    generation: GenerationVersions

    @model_validator(mode="after")
    def _validate_contributions(self) -> Self:
        """Keep contribution and evidence session references self-consistent."""
        contributing = set(self.contributing_session_ids)
        if len(contributing) != len(self.contributing_session_ids):
            raise ValueError("contributing session IDs must be unique")
        if any(item.session_id not in contributing for item in self.evidence):
            raise ValueError("evidence session must be a contributing session")
        return self


class PageIntro(_InsightGenerationModel):
    """Editorial copy introducing the generated set of cards."""

    eyebrow: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    description: str = Field(min_length=1, max_length=1000)


class PageRecommendation(_InsightGenerationModel):
    """Page-level recommendation pointing to exactly one generated card."""

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
    insights: list[InsightInput] = Field(default_factory=list, max_length=MAX_INSIGHTS)

    @staticmethod
    def card_metadata(insight: InsightInput) -> InsightCardMetadata:
        """Parse the namespaced metadata attached to a generated Insight."""
        if set(insight.metadata) != {INSIGHT_METADATA_KEY}:
            raise ValueError(
                f"Insight metadata must contain only {INSIGHT_METADATA_KEY!r}"
            )
        return InsightCardMetadata.model_validate(
            insight.metadata[INSIGHT_METADATA_KEY]
        )

    @model_validator(mode="after")
    def _validate_result(self) -> Self:
        """Validate card identity, ordering, and recommendation consistency."""
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

        names: list[str] = []
        positions: list[int] = []
        recommended_names: list[str] = []
        for insight in self.insights:
            if not 1 <= len(insight.name) <= MAX_NAME_LENGTH:
                raise ValueError("Insight name violates domain name constraints")
            if not _NAME_PATTERN.fullmatch(insight.name):
                raise ValueError("Insight name violates domain name constraints")
            if not 1 <= len(insight.title) <= MAX_NAME_LENGTH:
                raise ValueError("Insight title violates domain title constraints")
            if not isinstance(insight.data, CategoricalInsightData | BinnedInsightData):
                raise ValueError("generated Insight data must be categorical or binned")

            metadata = self.card_metadata(insight)
            if metadata.context != self.context:
                raise ValueError("card context must match generation context")
            if metadata.coverage != self.coverage:
                raise ValueError("card coverage must match result coverage")
            names.append(insight.name)
            positions.append(metadata.position)
            if metadata.recommended:
                recommended_names.append(insight.name)

        if len(names) != len(set(names)):
            raise ValueError("Insight names must be unique")
        if positions != list(range(len(self.insights))):
            raise ValueError("Insight positions must match list order")
        if len(recommended_names) != 1:
            raise ValueError("exactly one Insight must be recommended")
        if self.recommendation.insight_name != recommended_names[0]:
            raise ValueError("page recommendation must match the recommended card")
        return self
