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
"""Candidate finding model and the wording rules every finding briefing shares."""

import uuid
from collections.abc import Sequence
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from kitaru.api_models.v1.insight import (
    Bin,
    BinnedInsightData,
    CategoricalInsightData,
)
from kitaru_post_import_insights.models import (
    MAX_CONTRIBUTING_SESSIONS,
    MAX_EVIDENCE_LOCATORS,
    MAX_INVESTIGATION_PROMPT_LENGTH,
    EvidenceLocator,
)
from kitaru_post_import_insights.profiling_state import Histogram, SessionReferences


class _FindingModel(BaseModel):
    """Base for strict immutable profiler output."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class DeterministicFact(_FindingModel):
    """One computed fact safe to include in a model projection."""

    name: str = Field(min_length=1, max_length=80)
    value: int | float | str


class CandidateCoverage(_FindingModel):
    """Signal-specific coverage and bounded-reference accounting."""

    sessions_analyzed: int = Field(ge=0)
    affected_sessions: int = Field(ge=0)
    occurrences: int = Field(ge=0)
    evidence_available: int = Field(ge=0)
    evidence_retained: int = Field(ge=0)
    contributing_sessions_available: int = Field(ge=0)
    contributing_sessions_retained: int = Field(ge=0)

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        """Keep retained and affected counts within their available totals."""
        if self.affected_sessions > self.sessions_analyzed:
            raise ValueError("affected sessions exceed analyzed sessions")
        if self.evidence_retained > self.evidence_available:
            raise ValueError("retained evidence exceeds available evidence")
        if self.contributing_sessions_retained > self.contributing_sessions_available:
            raise ValueError("retained contributions exceed available contributions")
        return self


class CandidateFinding(_FindingModel):
    """One chart-backed finding eligible for model selection."""

    id: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    family: str = Field(min_length=1, max_length=80)
    rank: int = Field(ge=0)
    eyebrow: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=255)
    fallback_description: str = Field(min_length=1, max_length=1000)
    caveat: str | None = Field(default=None, min_length=1, max_length=1000)
    data: CategoricalInsightData | BinnedInsightData
    facts: list[DeterministicFact] = Field(default_factory=list, max_length=20)
    coverage: CandidateCoverage
    contributing_session_ids: list[uuid.UUID] = Field(
        min_length=1, max_length=MAX_CONTRIBUTING_SESSIONS
    )
    evidence: list[EvidenceLocator] = Field(
        default_factory=list, max_length=MAX_EVIDENCE_LOCATORS
    )
    investigation_prompt: str = Field(
        min_length=1, max_length=MAX_INVESTIGATION_PROMPT_LENGTH
    )

    @model_validator(mode="after")
    def _validate_references(self) -> Self:
        """Keep contribution and evidence references consistent."""
        contributing = set(self.contributing_session_ids)
        if len(contributing) != len(self.contributing_session_ids):
            raise ValueError("contributing session IDs must be unique")
        if any(item.session_id not in contributing for item in self.evidence):
            raise ValueError("evidence session must be a contributing session")
        return self


class ContributionSelector(Protocol):
    """Choose a bounded contribution list and record any truncation it causes."""

    def __call__(
        self,
        session_ids: SessionReferences,
        *,
        reserved_ids: Sequence[uuid.UUID] = (),
    ) -> list[uuid.UUID]:
        """Return the retained contributing session IDs in canonical order."""


def calculate_percent(numerator: int, denominator: int) -> int | float:
    """Round percentages, preserving the scale of very sparse shares."""
    if not denominator:
        return 0
    exact = Decimal(numerator) * 100 / Decimal(denominator)
    quantum = Decimal("0.01")
    value = exact.quantize(quantum, rounding=ROUND_HALF_UP)
    while 0 < numerator < denominator and value in (0, 100):
        quantum /= 10
        value = exact.quantize(quantum, rounding=ROUND_HALF_UP)
    if value == value.to_integral_value():
        return int(value)
    return float(value)


BRIEFING_CAUTION = (
    "This pattern is a lead, not proof of a bad result. The finding data at the "
    "end of this prompt carries the exact session IDs, the chart, and any "
    "evidence locators; use those, not this summary, when choosing sessions."
)


def format_briefing(
    *,
    odd: str,
    look_first: str,
    cohort: str,
    hypothesis: str,
    held: str,
) -> str:
    """Lay out one finding briefing in the fixed section order."""
    return (
        f"What is odd: {odd}\n"
        f"Where to look first: {look_first}\n"
        f"Candidate cohort: {cohort}\n"
        f"One hypothesis to test: {hypothesis}\n"
        f"The hypothesis held if: {held}\n"
        f"{BRIEFING_CAUTION}"
    )


def pluralize(count: int, singular: str, plural: str | None = None) -> str:
    """Return the count with the matching noun form."""
    noun = singular if count == 1 else (plural or f"{singular}s")
    return f"{count} {noun}"


def build_binned(values: Histogram, *, unit: str) -> BinnedInsightData:
    """Count values into contiguous half-open bins."""
    bounds = values.bounds
    bins = [
        Bin(
            lower_bound=None if index == 0 else bounds[index - 1],
            upper_bound=bounds[index] if index < len(bounds) else None,
            count=count,
        )
        for index, count in enumerate(values.bins)
    ]
    return BinnedInsightData(unit=unit, bins=bins)
