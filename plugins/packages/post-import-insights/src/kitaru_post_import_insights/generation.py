#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded provider-neutral analyst and editor pipeline for structured reports."""

import asyncio
import time
from typing import Generic, Literal, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.insight import InsightArtifact
from kitaru_post_import_insights.copy_validation import validate_card_copy
from kitaru_post_import_insights.models import (
    DEFAULT_MAX_PRIMARY_FINDINGS,
    MAX_PRIMARY_FINDINGS,
    MAX_SECONDARY_FINDINGS,
    EvidenceLocator,
    FocusArea,
    GenerationDiagnostics,
    GenerationMode,
    ProviderReceipt,
)
from kitaru_post_import_insights.profiling import (
    CandidateFinding,
    DeterministicFact,
    ProfilingResult,
)

_FOCUS_FAMILIES: dict[FocusArea, frozenset[str]] = {
    "tools": frozenset({"tool_health", "trajectory", "tool_paths", "failures"}),
    "cost": frozenset({"cost", "prompt_cache", "context", "model"}),
    "language": frozenset({"language"}),
    "latency": frozenset({"timing", "time_share"}),
    "outcomes": frozenset({"outcome", "failures"}),
}


class _GenerationModel(BaseModel):
    """Base for strict immutable provider-neutral generation values."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ModelGenerationConfig(_GenerationModel):
    """Credential-free hard limits for one model-backed run."""

    model: str = Field(min_length=1, max_length=255)
    reasoning_effort: Literal["low", "medium", "high"] | None = None
    total_timeout_seconds: float = Field(default=25.0, gt=0, le=120)
    analyst_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    editor_timeout_seconds: float = Field(default=12.0, gt=0, le=120)
    max_input_bytes: int = Field(default=512_000, ge=1_000, le=10_000_000)
    analyst_max_output_tokens: int = Field(default=1_000, ge=64, le=8_000)
    editor_max_output_tokens: int = Field(default=2_500, ge=64, le=8_000)


class CandidateProjection(_GenerationModel):
    """Bounded inert deterministic facts visible to a model stage."""

    id: str
    family: str
    tier: Literal["primary", "secondary"] = "primary"
    rank: int
    deterministic_title: str
    deterministic_description: str
    detector_description: str
    caveat: str | None
    facts: list[DeterministicFact]
    chart_data: InsightArtifact
    evidence_locators: list[EvidenceLocator]
    contributing_session_count: int = Field(ge=1)


class AnalystProjection(_GenerationModel):
    """Complete allowlisted input to the analyst."""

    content_hash: str
    max_findings: int = Field(
        default=DEFAULT_MAX_PRIMARY_FINDINGS, ge=1, le=MAX_PRIMARY_FINDINGS
    )
    focus: FocusArea | None = None
    candidates: list[CandidateProjection] = Field(min_length=1, max_length=100)


class AnalystPlan(_GenerationModel):
    """Provider-neutral candidate selection returned by the analyst."""

    selected_candidate_ids: list[str] = Field(
        min_length=1, max_length=MAX_PRIMARY_FINDINGS
    )
    recommended_candidate_id: str
    rationale: str = Field(min_length=1, max_length=1000)


class SelectedCandidateProjection(CandidateProjection):
    """One immutable selected candidate supplied to the editor."""

    position: int = Field(ge=0, lt=MAX_PRIMARY_FINDINGS + MAX_SECONDARY_FINDINGS)
    recommended: bool


class EditorialProjection(_GenerationModel):
    """Validated immutable analyst selection supplied to the editor."""

    content_hash: str
    recommendation_id: str
    candidates: list[SelectedCandidateProjection] = Field(
        min_length=1, max_length=MAX_PRIMARY_FINDINGS + MAX_SECONDARY_FINDINGS
    )


class EditorialCardCopy(_GenerationModel):
    """Model-authored explanatory copy for one selected finding."""

    id: str
    eyebrow: str = Field(min_length=1, max_length=80)
    description: str = Field(min_length=1, max_length=1000)


class EditorialCardPlan(_GenerationModel):
    """Provider-neutral finding copy returned by the editor."""

    insights: list[EditorialCardCopy] = Field(
        min_length=1, max_length=MAX_PRIMARY_FINDINGS + MAX_SECONDARY_FINDINGS
    )


DEFAULT_INTRO_EYEBROW = "What to look at first"
DEFAULT_INTRO_TITLE = "A few patterns are worth a closer look"
DEFAULT_INTRO_DESCRIPTION = (
    "These evidence-backed leads can guide your first investigation."
)
DEFAULT_RECOMMENDATION_TITLE = "Recommended next step"
DEFAULT_RECOMMENDATION_DESCRIPTION = (
    "Start with this pattern, define a focused cohort, and test a change."
)


class EditorialPlan(EditorialCardPlan):
    """Finding copy combined with deterministic report framing."""

    intro_eyebrow: str = Field(
        default=DEFAULT_INTRO_EYEBROW, min_length=1, max_length=80
    )
    intro_title: str = Field(default=DEFAULT_INTRO_TITLE, min_length=1, max_length=255)
    intro_description: str = Field(
        default=DEFAULT_INTRO_DESCRIPTION, min_length=1, max_length=1000
    )
    recommendation_title: str = Field(
        default=DEFAULT_RECOMMENDATION_TITLE, min_length=1, max_length=255
    )
    recommendation_description: str = Field(
        default=DEFAULT_RECOMMENDATION_DESCRIPTION, min_length=1, max_length=1000
    )


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
        """Select and order deterministic primary candidates."""

    async def edit(
        self,
        *,
        projection: EditorialProjection,
        config: ModelGenerationConfig,
        timeout_seconds: float,
    ) -> ModelStageResponse[EditorialCardPlan]:
        """Write finding copy for the validated selection without changing its facts."""


class ModelGenerationPlan(_GenerationModel):
    """Validated selection, secondary context, and copy ready for report assembly."""

    selection: AnalystPlan
    secondary_candidate_ids: list[str] = Field(
        default_factory=list, max_length=MAX_SECONDARY_FINDINGS
    )
    omitted_candidate_ids: list[str] = Field(default_factory=list)
    editorial: EditorialPlan
    mode: GenerationMode
    diagnostics: GenerationDiagnostics


def _candidate_sort_key(
    candidate: CandidateFinding, *, focus: FocusArea | None
) -> tuple[int, int, str]:
    """Order candidates with optional focus boost, then rank and stable ID."""
    focused = focus is not None and candidate.family in _FOCUS_FAMILIES[focus]
    return (0 if focused else 1, candidate.rank, candidate.id)


def _partition_candidates(
    candidates: list[CandidateFinding],
) -> tuple[list[CandidateFinding], list[CandidateFinding]]:
    """Split primary and secondary candidates, promoting secondary if none qualify."""
    primary = [item for item in candidates if item.tier == "primary"]
    secondary = [item for item in candidates if item.tier == "secondary"]
    if not primary:
        return secondary, []
    return primary, secondary


def _select_secondary_ids(
    secondary: list[CandidateFinding],
    *,
    excluded_ids: set[str],
    focus: FocusArea | None,
) -> list[str]:
    """Select compact baseline context findings for the bottom of the report."""
    ordered = sorted(
        (item for item in secondary if item.id not in excluded_ids),
        key=lambda item: _candidate_sort_key(item, focus=focus),
    )
    return [item.id for item in ordered[:MAX_SECONDARY_FINDINGS]]


def _build_candidate_projection(candidate: CandidateFinding) -> CandidateProjection:
    return CandidateProjection(
        id=candidate.id,
        family=candidate.family,
        tier=candidate.tier,
        rank=candidate.rank,
        deterministic_title=candidate.title,
        deterministic_description=candidate.fallback_description,
        detector_description=candidate.eyebrow,
        caveat=candidate.caveat,
        facts=candidate.facts,
        chart_data=candidate.data,
        evidence_locators=candidate.evidence,
        contributing_session_count=candidate.coverage.contributing_sessions_available,
    )


def build_analyst_projection(
    profiling: ProfilingResult,
    *,
    max_findings: int = DEFAULT_MAX_PRIMARY_FINDINGS,
    focus: FocusArea | None = None,
) -> AnalystProjection:
    """Build the only deterministic envelope the analyst may receive."""
    primary, _ = _partition_candidates(profiling.candidates)
    ordered = sorted(primary, key=lambda item: _candidate_sort_key(item, focus=focus))
    return AnalystProjection(
        content_hash=profiling.content_hash,
        max_findings=max_findings,
        focus=focus,
        candidates=[_build_candidate_projection(item) for item in ordered],
    )


def validate_analyst_plan(
    plan: AnalystPlan,
    candidates: list[CandidateFinding],
    *,
    max_findings: int = DEFAULT_MAX_PRIMARY_FINDINGS,
) -> AnalystPlan:
    """Require distinct known IDs within max_findings and a recommended pick."""
    selected = plan.selected_candidate_ids
    if len(selected) > max_findings:
        raise ValueError(f"analyst selected more than {max_findings} candidates")
    if len(selected) != len(set(selected)):
        raise ValueError("analyst candidate IDs must be unique")
    known = {candidate.id for candidate in candidates}
    if not set(selected).issubset(known):
        raise ValueError("analyst selected an unknown candidate ID")
    if plan.recommended_candidate_id not in selected:
        raise ValueError("analyst recommendation must be in the selection")
    return plan


def _order_selection_with_recommendation_first(plan: AnalystPlan) -> AnalystPlan:
    """Keep the recommended finding at index 0 so the report leads with it."""
    rec = plan.recommended_candidate_id
    ordered = [rec, *(cid for cid in plan.selected_candidate_ids if cid != rec)]
    return plan.model_copy(update={"selected_candidate_ids": ordered})


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
                **_build_candidate_projection(candidates[candidate_id]).model_dump(),
                position=position,
                recommended=candidate_id == selection.recommended_candidate_id,
            )
            for position, candidate_id in enumerate(selection.selected_candidate_ids)
        ],
    )


def _validate_selection_order(cards: EditorialCardPlan, selection: AnalystPlan) -> None:
    """Reject finding copy that drops, adds, or reorders the selected candidates."""
    if [item.id for item in cards.insights] != selection.selected_candidate_ids:
        raise ValueError("editor must preserve selection membership and order")


def _validate_card(item: EditorialCardCopy, candidate: CandidateFinding) -> None:
    for value in (item.eyebrow, item.description):
        validate_card_copy(value, candidate)


def _is_valid_card(item: EditorialCardCopy, candidate: CandidateFinding) -> bool:
    """Return whether both strings of one finding pass candidate-grounded checks."""
    try:
        _validate_card(item, candidate)
    except ValueError:
        return False
    return True


def validate_editorial_plan(
    plan: EditorialCardPlan,
    selection: AnalystPlan,
    candidates: list[CandidateFinding],
) -> EditorialPlan:
    """Reject copy that changes selection or introduces unsupported claims."""
    _validate_selection_order(plan, selection)
    by_id = {item.id: item for item in candidates}
    for item in plan.insights:
        _validate_card(item, by_id[item.id])
    if isinstance(plan, EditorialPlan):
        return plan
    return EditorialPlan(**plan.model_dump())


def apply_editorial_copy(
    cards: EditorialCardPlan,
    selection: AnalystPlan,
    candidates: list[CandidateFinding],
) -> tuple[EditorialPlan, list[str]]:
    """Keep validated finding copy and use deterministic copy for the rest."""
    by_id = {item.id: item for item in candidates}
    copy_by_id = {item.id: item for item in cards.insights}
    accepted: list[EditorialCardCopy] = []
    rejected: list[str] = []
    for candidate_id in selection.selected_candidate_ids:
        candidate = by_id[candidate_id]
        item = copy_by_id.get(candidate_id)
        if item is not None and _is_valid_card(item, candidate):
            accepted.append(item)
            continue
        rejected.append(candidate_id)
        accepted.append(
            EditorialCardCopy(
                id=candidate_id,
                eyebrow=candidate.eyebrow,
                description=candidate.fallback_description,
            )
        )
    return EditorialPlan(insights=accepted), rejected


def deterministic_selection(
    candidates: list[CandidateFinding],
    *,
    max_findings: int = DEFAULT_MAX_PRIMARY_FINDINGS,
    focus: FocusArea | None = None,
) -> AnalystPlan:
    """Select up to max_findings primary candidates without hiding same-family signals.

    Pass 1 picks the top candidate per distinct family so no single family
    monopolizes the report. Pass 2 fills remaining slots up to ``max_findings``
    with the highest-ranked remaining candidates across all families so related
    signals (such as retries, tool cycles, and per-tool health) surface together.
    """
    primary, _ = _partition_candidates(candidates)
    if not primary:
        raise ValueError("cannot select from an empty candidate set")
    ordered = sorted(primary, key=lambda item: _candidate_sort_key(item, focus=focus))
    selected: list[CandidateFinding] = []
    seen_ids: set[str] = set()
    seen_families: set[str] = set()

    # Pass 1: diversity across families.
    for candidate in ordered:
        if candidate.family in seen_families:
            continue
        selected.append(candidate)
        seen_ids.add(candidate.id)
        seen_families.add(candidate.family)
        if len(selected) == max_findings:
            break

    # Pass 2: fill remaining primary slots across all families (fixes #1516).
    if len(selected) < max_findings:
        for candidate in ordered:
            if candidate.id in seen_ids:
                continue
            selected.append(candidate)
            seen_ids.add(candidate.id)
            if len(selected) == max_findings:
                break

    selected.sort(key=lambda item: _candidate_sort_key(item, focus=focus))
    return AnalystPlan(
        selected_candidate_ids=[item.id for item in selected],
        recommended_candidate_id=selected[0].id,
        rationale="Two-pass deterministic selection with family diversity and refill.",
    )


def deterministic_editorial(
    selection: AnalystPlan, candidates: list[CandidateFinding]
) -> EditorialPlan:
    """Return stable report and finding copy without a model request."""
    by_id = {candidate.id: candidate for candidate in candidates}
    return EditorialPlan(
        insights=[
            EditorialCardCopy(
                id=candidate_id,
                eyebrow=by_id[candidate_id].eyebrow,
                description=by_id[candidate_id].fallback_description,
            )
            for candidate_id in selection.selected_candidate_ids
        ],
    )


def _compute_secondary_and_omitted(
    candidates: list[CandidateFinding],
    selection: AnalystPlan,
    *,
    focus: FocusArea | None,
) -> tuple[list[str], list[str]]:
    """Return secondary baseline IDs and any primary candidate IDs left off the page."""
    primary, secondary = _partition_candidates(candidates)
    selected_set = set(selection.selected_candidate_ids)
    secondary_ids = _select_secondary_ids(
        secondary, excluded_ids=selected_set, focus=focus
    )
    omitted_ids = [
        item.id
        for item in sorted(
            primary, key=lambda item: _candidate_sort_key(item, focus=focus)
        )
        if item.id not in selected_set
    ]
    return secondary_ids, omitted_ids


def _build_fallback(
    profiling: ProfilingResult,
    *,
    selection: AnalystPlan | None,
    receipts: list[ProviderReceipt],
    reason: str,
    max_findings: int = DEFAULT_MAX_PRIMARY_FINDINGS,
    focus: FocusArea | None = None,
) -> ModelGenerationPlan:
    selected = selection or deterministic_selection(
        profiling.candidates, max_findings=max_findings, focus=focus
    )
    secondary_ids, omitted_ids = _compute_secondary_and_omitted(
        profiling.candidates, selected, focus=focus
    )
    return ModelGenerationPlan(
        selection=selected,
        secondary_candidate_ids=secondary_ids,
        omitted_candidate_ids=omitted_ids,
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
    *,
    max_findings: int = DEFAULT_MAX_PRIMARY_FINDINGS,
    focus: FocusArea | None = None,
) -> ModelGenerationPlan:
    """Produce a zero-request deterministic generation plan."""
    selection = deterministic_selection(
        profiling.candidates, max_findings=max_findings, focus=focus
    )
    secondary_ids, omitted_ids = _compute_secondary_and_omitted(
        profiling.candidates, selection, focus=focus
    )
    return ModelGenerationPlan(
        selection=selection,
        secondary_candidate_ids=secondary_ids,
        omitted_candidate_ids=omitted_ids,
        editorial=deterministic_editorial(selection, profiling.candidates),
        mode=GenerationMode.DETERMINISTIC,
        diagnostics=GenerationDiagnostics(),
    )


async def generate_model_plan(
    profiling: ProfilingResult,
    *,
    generator: InsightModelGenerator,
    config: ModelGenerationConfig,
    max_findings: int = DEFAULT_MAX_PRIMARY_FINDINGS,
    focus: FocusArea | None = None,
) -> ModelGenerationPlan:
    """Run at most one analyst and one editor request under one deadline."""
    if not profiling.candidates:
        raise ValueError("model generation requires at least one candidate")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + config.total_timeout_seconds
    receipts: list[ProviderReceipt] = []
    projection = build_analyst_projection(
        profiling, max_findings=max_findings, focus=focus
    )
    if len(projection.model_dump_json().encode()) > config.max_input_bytes:
        return _build_fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason="analyst_input_too_large",
            max_findings=max_findings,
            focus=focus,
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
        selection = _order_selection_with_recommendation_first(
            validate_analyst_plan(
                analyst_response.value,
                profiling.candidates,
                max_findings=max_findings,
            )
        )
        receipts.append(analyst_response.receipt)
    except TimeoutError:
        receipts.append(
            ProviderReceipt(
                stage="analyst",
                latency_ms=int((time.monotonic() - analyst_started) * 1000),
                outcome="timed_out",
            )
        )
        return _build_fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason="analyst_timed_out",
            max_findings=max_findings,
            focus=focus,
        )
    except Exception as error:
        receipts.append(
            ProviderReceipt(
                stage="analyst",
                latency_ms=int((time.monotonic() - analyst_started) * 1000),
                outcome="failed",
            )
        )
        return _build_fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason=f"analyst_failed: {type(error).__name__}",
            max_findings=max_findings,
            focus=focus,
        )

    editorial_projection = build_editorial_projection(profiling, selection)
    if len(editorial_projection.model_dump_json().encode()) > config.max_input_bytes:
        return _build_fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason="editor_input_too_large",
            max_findings=max_findings,
            focus=focus,
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
    except TimeoutError:
        receipts.append(
            ProviderReceipt(
                stage="editor",
                latency_ms=int((time.monotonic() - editor_started) * 1000),
                outcome="timed_out",
            )
        )
        return _build_fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason="editor_timed_out",
            max_findings=max_findings,
            focus=focus,
        )
    except Exception as error:
        receipts.append(
            ProviderReceipt(
                stage="editor",
                latency_ms=int((time.monotonic() - editor_started) * 1000),
                outcome="failed",
            )
        )
        return _build_fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason=f"editor_failed: {type(error).__name__}",
            max_findings=max_findings,
            focus=focus,
        )

    receipts.append(editor_response.receipt)
    editorial, rejected_ids = apply_editorial_copy(
        editor_response.value, selection, profiling.candidates
    )
    secondary_ids, omitted_ids = _compute_secondary_and_omitted(
        profiling.candidates, selection, focus=focus
    )
    return ModelGenerationPlan(
        selection=selection,
        secondary_candidate_ids=secondary_ids,
        omitted_candidate_ids=omitted_ids,
        editorial=editorial,
        mode=GenerationMode.MODEL_BACKED,
        diagnostics=GenerationDiagnostics(
            provider_receipts=receipts,
            warnings=[
                f"Finding copy for {candidate_id} failed validation and uses "
                "deterministic text."
                for candidate_id in rejected_ids
            ],
        ),
    )
