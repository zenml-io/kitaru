#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded provider-neutral analyst and editor pipeline."""

import asyncio
import re
import time
import uuid
from typing import Generic, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.insight import BinnedInsightData, CategoricalInsightData
from kitaru.insights.models import (
    MAX_INSIGHTS,
    EvidenceLocator,
    GenerationDiagnostics,
    GenerationMode,
    ProviderReceipt,
)
from kitaru.insights.observability import (
    GenerationEvent,
    GenerationObserver,
    observe_safely,
)
from kitaru.insights.profiling import (
    CandidateFinding,
    DeterministicFact,
    ProfilingResult,
)

_NUMERIC_TOKEN = re.compile(r"\d+(?:[.,]\d+)*(?:%|[A-Za-z]+)?")
_QUANTITY_TOKEN = re.compile(
    r"\b(?:none|zero|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|"
    r"nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|"
    r"hundreds?|thousands?|millions?|billions?|trillions?|dozens?|"
    r"once|twice|thrice|all|every|each|both|half|halves|"
    r"double|doubled|doubles|doubling|triple|tripled|triples|tripling|"
    r"several|many|few|fewer|fewest|multiple|multiples|couple|couples|"
    r"majority|majorities|minority|minorities|numerous|handful|handfuls)\b",
    flags=re.IGNORECASE,
)
_LINK = re.compile(r"(?:https?://|www\.)", flags=re.IGNORECASE)
_MARKUP = re.compile(
    r"(?:<[^>]+>|\[[^\]]+\]\([^\)]+\)|```|^\s{0,3}#{1,6}\s)", re.MULTILINE
)
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_UNSUPPORTED_CLAIM = re.compile(
    r"\b(?:causes?|caused|because|due to|results? in|leads? to|"
    r"more|most|less|fewer|higher|lower|increase[ds]?|decrease[ds]?|"
    r"slower|slowest|faster|fastest|better|best|worse|worst|"
    r"healthy|correct|incorrect)\b",
    flags=re.IGNORECASE,
)
_OUTCOME_TOKEN = re.compile(
    r"\b(?:fail(?:ed|ing|s)?|failures?|succeed(?:ed|ing|s)?|"
    r"success|successes|successful|successfully|complete(?:d|s)?|"
    r"completion(?:s)?|completing|"
    r"in[ -]progress|"
    r"timed?[ -]out|timeouts?|cancel(?:ed|led|ation|ations|ing|s)?|"
    r"abandon(?:ed|ing|s)?|abandonments?|errors?|errored)\b",
    flags=re.IGNORECASE,
)
_SESSION_STATUS_OUTCOMES = {
    "failed": "failure",
    "completed": "completion",
    "in_progress": "in_progress",
}
_NON_IDENTITY_LABEL_WORDS = {
    "a",
    "an",
    "the",
    "of",
    "session",
    "sessions",
    "call",
    "calls",
    "request",
    "requests",
    "response",
    "responses",
    "result",
    "results",
    "occurrence",
    "occurrences",
    "percent",
    "percentage",
    "percentages",
    "retry",
    "retries",
    "error",
    "errors",
    "failure",
    "failures",
    "completion",
    "completions",
    "item",
    "items",
    "run",
    "runs",
    "time",
    "times",
    "tool",
    "tools",
}


class _GenerationModel(BaseModel):
    """Base for strict immutable provider-neutral generation values."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ModelGenerationConfig(_GenerationModel):
    """Credential-free hard limits for one model-backed run."""

    model: str = Field(min_length=1, max_length=255)
    total_timeout_seconds: float = Field(default=25.0, gt=0, le=120)
    analyst_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    editor_timeout_seconds: float = Field(default=12.0, gt=0, le=120)
    max_input_bytes: int = Field(default=512_000, ge=1_000, le=10_000_000)
    analyst_max_output_tokens: int = Field(default=1_000, ge=64, le=8_000)
    editor_max_output_tokens: int = Field(default=2_000, ge=64, le=8_000)


class CandidateProjection(_GenerationModel):
    """Bounded inert deterministic facts visible to a model stage."""

    id: str
    family: str
    rank: int
    deterministic_title: str
    deterministic_description: str
    detector_description: str
    caveat: str | None
    facts: list[DeterministicFact]
    chart_data: CategoricalInsightData | BinnedInsightData
    evidence_locators: list[EvidenceLocator]
    contributing_session_count: int = Field(ge=1)


class AnalystProjection(_GenerationModel):
    """Complete allowlisted input to the analyst."""

    content_hash: str
    candidates: list[CandidateProjection] = Field(min_length=1, max_length=100)


class AnalystPlan(_GenerationModel):
    """Provider-neutral candidate selection returned by the analyst."""

    selected_candidate_ids: list[str] = Field(min_length=1, max_length=MAX_INSIGHTS)
    recommended_candidate_id: str
    rationale: str = Field(min_length=1, max_length=1000)


class SelectedCandidateProjection(CandidateProjection):
    """One immutable selected candidate supplied to the editor."""

    position: int = Field(ge=0, lt=MAX_INSIGHTS)
    recommended: bool


class EditorialProjection(_GenerationModel):
    """Validated immutable analyst selection supplied to the editor."""

    content_hash: str
    recommendation_id: str
    candidates: list[SelectedCandidateProjection] = Field(
        min_length=1, max_length=MAX_INSIGHTS
    )


class EditorialCardCopy(_GenerationModel):
    """Model-authored explanatory copy for one selected candidate."""

    id: str
    eyebrow: str = Field(min_length=1, max_length=80)
    description: str = Field(min_length=1, max_length=1000)


class EditorialPlan(_GenerationModel):
    """Provider-neutral page and card copy returned by the editor."""

    intro_eyebrow: str = Field(min_length=1, max_length=80)
    intro_title: str = Field(min_length=1, max_length=255)
    intro_description: str = Field(min_length=1, max_length=1000)
    recommendation_title: str = Field(min_length=1, max_length=255)
    recommendation_description: str = Field(min_length=1, max_length=1000)
    insights: list[EditorialCardCopy] = Field(min_length=1, max_length=MAX_INSIGHTS)


T = TypeVar("T", bound=BaseModel)


class ModelStageResponse(_GenerationModel, Generic[T]):
    """Provider-neutral successful response and sanitized receipt."""

    value: T
    receipt: ProviderReceipt


class InsightModelGenerator(Protocol):
    """Perform the two settled model operations without provider leakage."""

    async def analyze(
        self,
        *,
        projection: AnalystProjection,
        config: ModelGenerationConfig,
        timeout_seconds: float,
    ) -> ModelStageResponse[AnalystPlan]:
        """Select and order deterministic candidates."""

    async def edit(
        self,
        *,
        projection: EditorialProjection,
        config: ModelGenerationConfig,
        timeout_seconds: float,
    ) -> ModelStageResponse[EditorialPlan]:
        """Edit the validated selection without changing its facts."""


class ModelGenerationPlan(_GenerationModel):
    """Validated selection and copy ready for deterministic assembly."""

    selection: AnalystPlan
    editorial: EditorialPlan
    mode: GenerationMode
    diagnostics: GenerationDiagnostics


def _candidate_projection(candidate: CandidateFinding) -> CandidateProjection:
    return CandidateProjection(
        id=candidate.id,
        family=candidate.family,
        rank=candidate.rank,
        deterministic_title=candidate.title,
        deterministic_description=candidate.fallback_description,
        detector_description=candidate.eyebrow,
        caveat=candidate.caveat,
        facts=candidate.facts,
        chart_data=candidate.data,
        evidence_locators=candidate.evidence,
        contributing_session_count=len(candidate.contributing_session_ids),
    )


def build_analyst_projection(profiling: ProfilingResult) -> AnalystProjection:
    """Build the only deterministic envelope the analyst may receive."""
    projection = AnalystProjection(
        content_hash=profiling.content_hash,
        candidates=[_candidate_projection(item) for item in profiling.candidates],
    )
    return projection


def validate_analyst_plan(
    plan: AnalystPlan, candidates: list[CandidateFinding]
) -> AnalystPlan:
    """Require one to six distinct known IDs and an in-selection recommendation."""
    selected = plan.selected_candidate_ids
    if len(selected) != len(set(selected)):
        raise ValueError("analyst candidate IDs must be unique")
    known = {candidate.id for candidate in candidates}
    if not set(selected).issubset(known):
        raise ValueError("analyst selected an unknown candidate ID")
    if plan.recommended_candidate_id not in selected:
        raise ValueError("analyst recommendation must be in the selection")
    if _CONTROL.search(plan.rationale) or _LINK.search(plan.rationale):
        raise ValueError("analyst rationale contains unsafe content")
    if _MARKUP.search(plan.rationale):
        raise ValueError("analyst rationale contains markup")
    return plan


def build_editorial_projection(
    profiling: ProfilingResult, selection: AnalystPlan
) -> EditorialProjection:
    """Freeze the validated selection before the editor runs."""
    candidates = {candidate.id: candidate for candidate in profiling.candidates}
    return EditorialProjection(
        content_hash=profiling.content_hash,
        recommendation_id=selection.recommended_candidate_id,
        candidates=[
            SelectedCandidateProjection(
                **_candidate_projection(candidates[candidate_id]).model_dump(),
                position=position,
                recommended=candidate_id == selection.recommended_candidate_id,
            )
            for position, candidate_id in enumerate(selection.selected_candidate_ids)
        ],
    )


def _page_copy(plan: EditorialPlan) -> list[str]:
    return [
        plan.intro_eyebrow,
        plan.intro_title,
        plan.intro_description,
        plan.recommendation_title,
        plan.recommendation_description,
    ]


def _validate_copy_safety(value: str) -> None:
    """Reject content that the frontend must not render as editorial copy."""
    if _CONTROL.search(value):
        raise ValueError("editor copy contains control characters")
    if _LINK.search(value):
        raise ValueError("editor copy contains a link")
    if _MARKUP.search(value):
        raise ValueError("editor copy contains markup")
    if _UNSUPPORTED_CLAIM.search(value):
        raise ValueError("editor copy contains an unsupported claim")


def _quantified_labels(candidate: CandidateFinding) -> set[str]:
    """Return quantity-bearing labels that also contain an identity token."""
    if not isinstance(candidate.data, CategoricalInsightData):
        return set()
    return {
        item.label
        for item in candidate.data.values
        if _is_identity_bearing_quantified_label(item.label)
    }


def _is_identity_bearing_quantified_label(label: str) -> bool:
    """Distinguish names containing quantities from labels that are quantities."""
    if not (_NUMERIC_TOKEN.search(label) or _QUANTITY_TOKEN.search(label)):
        return False
    without_quantities = _QUANTITY_TOKEN.sub("", label)
    without_quantities = _NUMERIC_TOKEN.sub("", without_quantities)
    remaining_words = {
        word.lower() for word in re.findall(r"[A-Za-z]+", without_quantities)
    }
    return bool(remaining_words - _NON_IDENTITY_LABEL_WORDS)


def _without_known_labels(value: str, labels: set[str]) -> str:
    """Mask exact known labels so identity tokens are not treated as claims."""
    for label in sorted(labels, key=len, reverse=True):
        value = re.sub(
            rf"(?<![A-Za-z0-9]){re.escape(label)}(?![A-Za-z0-9])",
            "",
            value,
        )
    return value


def _outcome_categories(value: str) -> set[str]:
    """Normalize grammatical variants of outcome words into factual classes."""
    categories: set[str] = set()
    for match in _OUTCOME_TOKEN.finditer(value):
        token = match.group(0).lower()
        if token.startswith("fail"):
            categories.add("failure")
        elif token.startswith(("success", "succeed")):
            categories.add("success")
        elif token.startswith(("complete", "completion", "completing")):
            categories.add("completion")
        elif token.startswith("in"):
            categories.add("in_progress")
        elif token.startswith(("timed", "timeout")):
            categories.add("timeout")
        elif token.startswith("cancel"):
            categories.add("cancellation")
        elif token.startswith("abandon"):
            categories.add("abandonment")
        else:
            categories.add("error")
    return categories


def _trusted_outcome_categories(candidate: CandidateFinding) -> set[str]:
    """Derive permitted outcome language from profiler-controlled semantics."""
    if (
        candidate.id == "session-outcomes"
        and candidate.family == "outcome"
        and isinstance(candidate.data, CategoricalInsightData)
    ):
        return {
            outcome
            for item in candidate.data.values
            if item.value > 0
            and (outcome := _SESSION_STATUS_OUTCOMES.get(item.label)) is not None
        }
    semantic_names = " ".join(
        (
            candidate.id.replace("_", " "),
            candidate.family.replace("_", " "),
            *(fact.name.replace("_", " ") for fact in candidate.facts),
        )
    )
    return _outcome_categories(semantic_names)


def _validate_page_copy(value: str) -> None:
    """Validate friendly page framing separately from candidate facts."""
    _validate_copy_safety(value)
    if _NUMERIC_TOKEN.search(value) or _QUANTITY_TOKEN.search(value):
        raise ValueError("editor page copy contains a numeric or quantitative claim")
    if _OUTCOME_TOKEN.search(value):
        raise ValueError("editor page copy contains an unsupported outcome claim")


def _validate_card_copy(value: str, candidate: CandidateFinding) -> None:
    """Validate one card only against the deterministic candidate it explains."""
    _validate_copy_safety(value)
    allowed_outcomes = _trusted_outcome_categories(candidate)
    remaining = _without_known_labels(value, _quantified_labels(candidate))
    if _NUMERIC_TOKEN.search(remaining) or _QUANTITY_TOKEN.search(remaining):
        raise ValueError("editor card copy contains a numeric or quantitative claim")
    output_outcomes = _outcome_categories(value)
    if not output_outcomes.issubset(allowed_outcomes):
        raise ValueError("editor card copy contains an unsupported outcome claim")


def validate_editorial_plan(
    plan: EditorialPlan,
    selection: AnalystPlan,
    candidates: list[CandidateFinding],
) -> EditorialPlan:
    """Reject copy that changes selection or introduces unsupported claims."""
    actual = [item.id for item in plan.insights]
    if actual != selection.selected_candidate_ids:
        raise ValueError("editor must preserve selection membership and order")

    selected = {
        item.id: item
        for item in candidates
        if item.id in selection.selected_candidate_ids
    }
    for value in _page_copy(plan):
        _validate_page_copy(value)
    for item in plan.insights:
        for value in (item.eyebrow, item.description):
            _validate_card_copy(value, selected[item.id])
    return plan


def deterministic_selection(candidates: list[CandidateFinding]) -> AnalystPlan:
    """Select up to six stable, non-redundant candidates for fallback."""
    selected: list[CandidateFinding] = []
    families: set[str] = set()
    for candidate in sorted(candidates, key=lambda item: (item.rank, item.id)):
        if candidate.family in families:
            continue
        selected.append(candidate)
        families.add(candidate.family)
        if len(selected) == MAX_INSIGHTS:
            break
    if not selected:
        raise ValueError("cannot select from an empty candidate set")
    return AnalystPlan(
        selected_candidate_ids=[item.id for item in selected],
        recommended_candidate_id=selected[0].id,
        rationale="Stable deterministic fallback ordering.",
    )


def deterministic_editorial(
    selection: AnalystPlan, candidates: list[CandidateFinding]
) -> EditorialPlan:
    """Return stable page and card copy without a model request."""
    by_id = {candidate.id: candidate for candidate in candidates}
    return EditorialPlan(
        intro_eyebrow="What to look at first",
        intro_title="A few patterns are worth a closer look",
        intro_description=(
            "These evidence-backed leads can guide your first investigation."
        ),
        recommendation_title="Recommended next step",
        recommendation_description=(
            "Start with this pattern, define a focused cohort, and test a change."
        ),
        insights=[
            EditorialCardCopy(
                id=candidate_id,
                eyebrow=by_id[candidate_id].eyebrow,
                description=by_id[candidate_id].fallback_description,
            )
            for candidate_id in selection.selected_candidate_ids
        ],
    )


def _fallback(
    profiling: ProfilingResult,
    *,
    selection: AnalystPlan | None,
    receipts: list[ProviderReceipt],
    reason: str,
) -> ModelGenerationPlan:
    selected = selection or deterministic_selection(profiling.candidates)
    return ModelGenerationPlan(
        selection=selected,
        editorial=deterministic_editorial(selected, profiling.candidates),
        mode=GenerationMode.DETERMINISTIC_FALLBACK,
        diagnostics=GenerationDiagnostics(
            provider_receipts=receipts,
            warnings=["Model-backed generation was unavailable."],
            fallback_reason=reason,
        ),
    )


def generate_deterministic_plan(
    profiling: ProfilingResult,
) -> ModelGenerationPlan:
    """Produce a zero-request deterministic generation plan."""
    selection = deterministic_selection(profiling.candidates)
    return ModelGenerationPlan(
        selection=selection,
        editorial=deterministic_editorial(selection, profiling.candidates),
        mode=GenerationMode.DETERMINISTIC,
        diagnostics=GenerationDiagnostics(),
    )


async def generate_model_plan(
    profiling: ProfilingResult,
    *,
    generator: InsightModelGenerator,
    config: ModelGenerationConfig,
    observer: GenerationObserver | None = None,
    run_id: str | None = None,
) -> ModelGenerationPlan:
    """Run at most one analyst and one editor request under one deadline."""
    if not profiling.candidates:
        raise ValueError("model generation requires at least one candidate")
    effective_run_id = run_id or str(uuid.uuid4())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + config.total_timeout_seconds
    receipts: list[ProviderReceipt] = []
    projection = build_analyst_projection(profiling)
    if len(projection.model_dump_json().encode()) > config.max_input_bytes:
        return _fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason="analyst_input_too_large",
        )
    analyst_started = time.monotonic()
    try:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError
        async with asyncio.timeout(min(config.analyst_timeout_seconds, remaining)):
            analyst_response = await generator.analyze(
                projection=projection,
                config=config,
                timeout_seconds=min(config.analyst_timeout_seconds, remaining),
            )
        if analyst_response.receipt.stage != "analyst":
            raise ValueError("analyst returned the wrong receipt stage")
        selection = validate_analyst_plan(analyst_response.value, profiling.candidates)
        receipts.append(analyst_response.receipt)
        observation_started = loop.time()
        await observe_safely(
            observer,
            GenerationEvent(
                name="analyst",
                run_id=effective_run_id,
                stage="analyst",
                metadata={
                    "outcome": "succeeded",
                    "selected_count": len(selection.selected_candidate_ids),
                },
            ),
        )
        deadline += loop.time() - observation_started
    except TimeoutError:
        receipts.append(
            ProviderReceipt(
                stage="analyst",
                latency_ms=int((time.monotonic() - analyst_started) * 1000),
                outcome="timed_out",
            )
        )
        await observe_safely(
            observer,
            GenerationEvent(
                name="analyst",
                run_id=effective_run_id,
                stage="analyst",
                metadata={"outcome": "timed_out"},
            ),
        )
        return _fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason="analyst_timed_out",
        )
    except Exception:
        receipts.append(
            ProviderReceipt(
                stage="analyst",
                latency_ms=int((time.monotonic() - analyst_started) * 1000),
                outcome="failed",
            )
        )
        await observe_safely(
            observer,
            GenerationEvent(
                name="analyst",
                run_id=effective_run_id,
                stage="analyst",
                metadata={"outcome": "failed"},
            ),
        )
        return _fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason="analyst_failed",
        )

    editorial_projection = build_editorial_projection(profiling, selection)
    if len(editorial_projection.model_dump_json().encode()) > config.max_input_bytes:
        return _fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason="editor_input_too_large",
        )
    editor_started = time.monotonic()
    try:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError
        async with asyncio.timeout(min(config.editor_timeout_seconds, remaining)):
            editor_response = await generator.edit(
                projection=editorial_projection,
                config=config,
                timeout_seconds=min(config.editor_timeout_seconds, remaining),
            )
        if editor_response.receipt.stage != "editor":
            raise ValueError("editor returned the wrong receipt stage")
        editorial = validate_editorial_plan(
            editor_response.value, selection, profiling.candidates
        )
        receipts.append(editor_response.receipt)
        await observe_safely(
            observer,
            GenerationEvent(
                name="editor",
                run_id=effective_run_id,
                stage="editor",
                metadata={
                    "outcome": "succeeded",
                    "copy_count": len(editorial.insights),
                },
            ),
        )
    except TimeoutError:
        receipts.append(
            ProviderReceipt(
                stage="editor",
                latency_ms=int((time.monotonic() - editor_started) * 1000),
                outcome="timed_out",
            )
        )
        await observe_safely(
            observer,
            GenerationEvent(
                name="editor",
                run_id=effective_run_id,
                stage="editor",
                metadata={"outcome": "timed_out"},
            ),
        )
        return _fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason="editor_timed_out",
        )
    except Exception:
        receipts.append(
            ProviderReceipt(
                stage="editor",
                latency_ms=int((time.monotonic() - editor_started) * 1000),
                outcome="failed",
            )
        )
        await observe_safely(
            observer,
            GenerationEvent(
                name="editor",
                run_id=effective_run_id,
                stage="editor",
                metadata={"outcome": "failed"},
            ),
        )
        return _fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason="editor_failed",
        )
    return ModelGenerationPlan(
        selection=selection,
        editorial=editorial,
        mode=GenerationMode.MODEL_BACKED,
        diagnostics=GenerationDiagnostics(provider_receipts=receipts),
    )
