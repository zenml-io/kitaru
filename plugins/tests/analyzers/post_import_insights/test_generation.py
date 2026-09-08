#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Tests for the bounded analyst-editor pipeline."""

import asyncio
import uuid
from collections.abc import Sequence
from typing import Literal

import pytest

from kitaru.api_models.v1.insight import CategoricalInsightData, CategoryValue
from kitaru_post_import_insights.generation import (
    DEFAULT_INTRO_TITLE,
    AnalystPlan,
    EditorialCardCopy,
    EditorialPlan,
    InsightModelGenerator,
    ModelGenerationConfig,
    ModelStageResponse,
    apply_editorial_copy,
    build_analyst_projection,
    build_editorial_projection,
    generate_deterministic_plan,
    generate_model_plan,
    validate_analyst_plan,
    validate_editorial_plan,
)
from kitaru_post_import_insights.models import (
    Coverage,
    EvidenceLocator,
    GenerationMode,
    ProviderReceipt,
)
from kitaru_post_import_insights.profiling import (
    CandidateCoverage,
    CandidateFinding,
    DeterministicFact,
    ProfilingResult,
)


def _receipt(stage: Literal["analyst", "editor"]) -> ProviderReceipt:
    return ProviderReceipt(
        stage=stage,
        request_id=f"request-{stage}",
        model="test-model",
        input_tokens=10,
        output_tokens=5,
        latency_ms=1,
        outcome="succeeded",
    )


@pytest.fixture
def profiling_result() -> ProfilingResult:
    session_id = uuid.UUID("01990000-0000-7000-8000-000000000001")
    candidates = [
        CandidateFinding(
            id=f"candidate-{index}",
            family=f"family-{index}",
            rank=index,
            eyebrow="Tool behavior",
            title=f"Candidate {index}",
            fallback_description=f"Pattern {index} is worth inspecting.",
            data=CategoricalInsightData(
                values=[CategoryValue(label="Observed", value=index + 1)]
            ),
            facts=[DeterministicFact(name="count", value=index + 1)],
            coverage=CandidateCoverage(
                sessions_analyzed=2,
                affected_sessions=1,
                occurrences=1,
                evidence_available=1,
                evidence_retained=1,
                contributing_sessions_available=1,
                contributing_sessions_retained=1,
            ),
            contributing_session_ids=[session_id],
            evidence=[EvidenceLocator(session_id=session_id, signal="test")],
            investigation_prompt="Investigate this pattern and define a cohort.",
        )
        for index in range(2)
    ]
    return ProfilingResult(
        content_hash="a" * 64,
        coverage=Coverage(sessions_available=2, sessions_analyzed=2),
        candidates=candidates,
    )


class FakeGenerator(InsightModelGenerator):
    def __init__(
        self,
        analyst: AnalystPlan | Exception,
        editor: EditorialPlan | Exception,
    ) -> None:
        self.analyst = analyst
        self.editor = editor
        self.calls: list[str] = []

    async def analyze(self, *, projection, config, timeout_seconds):
        self.calls.append("analyst")
        if isinstance(self.analyst, Exception):
            raise self.analyst
        return ModelStageResponse(value=self.analyst, receipt=_receipt("analyst"))

    async def edit(self, *, projection, config, timeout_seconds):
        self.calls.append("editor")
        if isinstance(self.editor, Exception):
            raise self.editor
        return ModelStageResponse(value=self.editor, receipt=_receipt("editor"))


def _editor(ids: Sequence[str]) -> EditorialPlan:
    return EditorialPlan(
        intro_eyebrow="Worth looking at first",
        intro_title="Patterns deserve attention",
        intro_description="These patterns can guide the first investigation.",
        recommendation_title="Recommended next step",
        recommendation_description="Start here and compare a focused cohort.",
        insights=[
            EditorialCardCopy(
                id=item,
                eyebrow="Tool behavior",
                description="This pattern is worth a closer look.",
            )
            for item in ids
        ],
    )


def test_editor_allows_exact_known_word_quantity_label(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "data": CategoricalInsightData(
                values=[CategoryValue(label="Model Two", value=1)]
            )
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Model mix",
                    description="Model Two is worth investigating.",
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_known_label_must_match_at_an_exact_boundary(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "data": CategoricalInsightData(
                values=[CategoryValue(label="gpt-5.4", value=1)]
            )
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Model mix",
                    description="gpt-5.4x is worth investigating.",
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="numeric"):
        validate_editorial_plan(copy, selection, [candidate])


def test_comparative_substrings_in_plain_prose_are_allowed(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description=(
                        "Breakfast requests and bestseller paths are worth inspecting."
                    ),
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_extended_comparative_substrings_in_plain_prose_are_allowed(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description=("Enlargers and smallholders are worth investigating."),
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_causal_substrings_in_plain_prose_are_allowed(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description=(
                        "Causality, resultant metrics, leadership, driveways, "
                        "undriven paths, "
                        "producers, creatures, triggerfish, accountants, "
                        "accountability, giveaways, and upbringings are worth "
                        "inspecting."
                    ),
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_editor_rejects_non_utf8_card_copy(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    invalid_card = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                _editor([candidate.id])
                .insights[0]
                .model_copy(update={"description": "broken-\udfff-description"})
            ]
        }
    )

    with pytest.raises(ValueError, match="valid UTF-8"):
        validate_editorial_plan(invalid_card, selection, [candidate])


def test_editor_allows_valid_unicode_copy(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "intro_title": "Patterns worth investigating 日本語",
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description="Modèle à inspecter.",
                )
            ],
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_quantity_substrings_in_plain_prose_are_allowed(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description=(
                        "Couplets, multiplexers, quartermasters, fractionalizers, "
                        "and proportionalists are worth investigating."
                    ),
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_editor_allows_exact_known_indefinite_quantity_label(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "data": CategoricalInsightData(
                values=[CategoryValue(label="Model Many", value=1)]
            )
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Model mix",
                    description="Model Many is worth investigating.",
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_editor_allows_exact_known_fractional_quantity_label(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "data": CategoricalInsightData(
                values=[CategoryValue(label="Model Quarter", value=1)]
            )
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Model mix",
                    description="Model Quarter is worth investigating.",
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_editor_allows_friendly_variant_of_grounded_outcome(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "session-outcomes",
            "family": "outcome",
            "fallback_description": "A session failed.",
            "data": CategoricalInsightData(
                values=[CategoryValue(label="failed", value=1)]
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Session outcomes",
                    description="Sessions failed.",
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


@pytest.mark.parametrize(
    "description",
    [
        "Completed sessions are worth investigating.",
        "In-progress sessions are worth investigating.",
        "Sessions finished normally.",
        "These sessions are done.",
    ],
)
def test_session_outcomes_rejects_unobserved_status_language(
    profiling_result: ProfilingResult,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "session-outcomes",
            "family": "outcome",
            "data": CategoricalInsightData(
                values=[CategoryValue(label="failed", value=1)]
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Session outcomes",
                    description=description,
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="unsupported outcome"):
        validate_editorial_plan(copy, selection, [candidate])


@pytest.mark.parametrize(
    "description",
    [
        "Sessions finish normally.",
        "Sessions finished normally.",
        "Sessions are finishing normally.",
        "This session finishes normally.",
        "These sessions are done.",
    ],
)
def test_session_outcomes_allows_observed_completion_synonyms(
    profiling_result: ProfilingResult,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "session-outcomes",
            "family": "outcome",
            "data": CategoricalInsightData(
                values=[CategoryValue(label="completed", value=1)]
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Session outcomes",
                    description=description,
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


@pytest.mark.parametrize(
    "description",
    [
        "Sessions pass.",
        "This session passes.",
        "Sessions passed.",
        "Sessions are passing.",
    ],
)
def test_completed_sessions_do_not_authorize_successful_pass_wording(
    profiling_result: ProfilingResult,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "session-outcomes",
            "family": "outcome",
            "data": CategoricalInsightData(
                values=[CategoryValue(label="completed", value=1)]
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Session outcomes",
                    description=description,
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="unsupported outcome"):
        validate_editorial_plan(copy, selection, [candidate])


@pytest.mark.parametrize(
    "description",
    [
        "The agent works as expected.",
        "The tool worked as expected.",
        "The model is working as intended.",
        "The system works correctly.",
        "The agent worked properly.",
        "The run is working normally.",
        "The agent resolved these requests.",
        "The agent resolves requests.",
        "The agent is resolving requests.",
        "Agents resolve requests.",
        "The agent fixed the issue.",
        "The agent fixes issues.",
        "The agent is fixing issues.",
        "Agents fix issues.",
    ],
)
def test_completed_sessions_do_not_authorize_business_success_claims(
    profiling_result: ProfilingResult,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "session-outcomes",
            "family": "outcome",
            "data": CategoricalInsightData(
                values=[CategoryValue(label="completed", value=1)]
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Session outcomes",
                    description=description,
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="unsupported outcome"):
        validate_editorial_plan(copy, selection, [candidate])


@pytest.mark.parametrize(
    "description",
    [
        "Work on this pattern next.",
        "Coworkers and workflows are worth investigating.",
        "Inspect the resolver configuration and fixture setup.",
    ],
)
def test_non_outcome_work_language_is_allowed(
    profiling_result: ProfilingResult,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description=description,
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_pass_substrings_in_plain_prose_are_allowed(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description=(
                        "Bypass routes, passengers, and passageways are worth "
                        "investigating."
                    ),
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


@pytest.mark.parametrize(
    "description",
    [
        "No sessions failed.",
        "Failures are not present.",
        "Failures were not detected.",
        "Failures weren't detected.",
        "Failures weren\u2019t detected.",
        "Sessions never failed.",
        "Sessions completed without failures.",
        "Failures should not be ignored.",
    ],
)
def test_session_outcomes_rejects_negated_observed_status(
    profiling_result: ProfilingResult,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "session-outcomes",
            "family": "outcome",
            "data": CategoricalInsightData(
                values=[CategoryValue(label="failed", value=1)]
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Session outcomes",
                    description=description,
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="negated outcome"):
        validate_editorial_plan(copy, selection, [candidate])


def test_editor_allows_not_without_an_outcome_negation(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description="This pattern is not yet understood.",
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_editor_allows_negation_in_a_separate_clause_from_an_outcome(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "session-outcomes",
            "family": "outcome",
            "data": CategoricalInsightData(
                values=[CategoryValue(label="failed", value=1)]
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Session outcomes",
                    description=(
                        "This pattern is not yet understood. "
                        "Failures are worth investigating."
                    ),
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_chart_label_cannot_authorize_outcome_claim(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "data": CategoricalInsightData(
                values=[CategoryValue(label="failed", value=1)]
            )
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Model mix",
                    description="Failures are worth investigating.",
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="unsupported outcome"):
        validate_editorial_plan(copy, selection, [candidate])


def test_deterministic_tool_failure_authorizes_failure_copy(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "tool-error-mix",
            "family": "tool_health",
            "title": "A recorded tool call failed in a completed session",
            "fallback_description": (
                "The recorded tool failure may have been recovered later."
            ),
            "data": CategoricalInsightData(
                values=[CategoryValue(label="lookup_order", value=1)]
            ),
            "facts": [DeterministicFact(name="occurrences", value=1)],
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description="Tool calls failed.",
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy


def test_tool_failure_does_not_authorize_session_failure_copy(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "id": "tool-error-mix",
            "family": "tool_health",
            "title": "Recorded tool errors affect one session",
            "fallback_description": "Recorded tool errors are worth inspecting.",
            "caveat": (
                "A recorded tool error may be recovered later and is not the same "
                "as a failed session."
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description="Session errors are worth investigating.",
                )
            ],
        }
    )

    with pytest.raises(ValueError, match="unsupported outcome"):
        validate_editorial_plan(copy, selection, [candidate])


def test_deterministic_plan_makes_no_model_call(
    profiling_result: ProfilingResult,
) -> None:
    result = generate_deterministic_plan(profiling_result)
    assert result.mode == GenerationMode.DETERMINISTIC
    assert result.diagnostics.provider_receipts == []


async def test_two_calls_on_valid_path(profiling_result: ProfilingResult) -> None:
    events = []

    class Observer:
        async def record(self, event) -> None:
            events.append(event)

    first = profiling_result.candidates[0].id
    generator = FakeGenerator(
        AnalystPlan(
            selected_candidate_ids=[first],
            recommended_candidate_id=first,
            rationale="Useful.",
        ),
        _editor([first]),
    )
    result = await generate_model_plan(
        profiling_result,
        generator=generator,
        config=ModelGenerationConfig(model="test-model"),
        observer=Observer(),
        run_id="pipeline-run",
    )
    assert generator.calls == ["analyst", "editor"]
    assert result.mode == GenerationMode.MODEL_BACKED
    assert len(result.diagnostics.provider_receipts) == 2
    assert [event.name for event in events] == ["analyst", "editor"]
    assert {event.run_id for event in events} == {"pipeline-run"}


async def test_observer_wait_does_not_consume_model_deadline(
    profiling_result: ProfilingResult,
) -> None:
    class TimedGenerator(FakeGenerator):
        async def analyze(self, *, projection, config, timeout_seconds):
            await asyncio.sleep(0.04)
            return await super().analyze(
                projection=projection,
                config=config,
                timeout_seconds=timeout_seconds,
            )

        async def edit(self, *, projection, config, timeout_seconds):
            await asyncio.sleep(0.04)
            return await super().edit(
                projection=projection,
                config=config,
                timeout_seconds=timeout_seconds,
            )

    class SlowObserver:
        async def record(self, event) -> None:
            await asyncio.sleep(0.08)

    first = profiling_result.candidates[0].id
    generator = TimedGenerator(
        AnalystPlan(
            selected_candidate_ids=[first],
            recommended_candidate_id=first,
            rationale="Useful.",
        ),
        _editor([first]),
    )
    result = await generate_model_plan(
        profiling_result,
        generator=generator,
        config=ModelGenerationConfig(
            model="test-model",
            total_timeout_seconds=0.12,
            analyst_timeout_seconds=0.1,
            editor_timeout_seconds=0.1,
        ),
        observer=SlowObserver(),
    )
    assert generator.calls == ["analyst", "editor"]
    assert result.mode == GenerationMode.MODEL_BACKED


async def test_analyst_failure_skips_editor(profiling_result: ProfilingResult) -> None:
    generator = FakeGenerator(
        RuntimeError("secret provider details"),
        _editor([profiling_result.candidates[0].id]),
    )
    result = await generate_model_plan(
        profiling_result,
        generator=generator,
        config=ModelGenerationConfig(model="test-model"),
    )
    assert generator.calls == ["analyst"]
    assert result.mode == GenerationMode.DETERMINISTIC_FALLBACK
    assert "secret" not in result.diagnostics.model_dump_json()


async def test_provider_timeout_gets_timeout_receipt_and_fallback(
    profiling_result: ProfilingResult,
) -> None:
    generator = FakeGenerator(
        TimeoutError("provider detail"),
        _editor([profiling_result.candidates[0].id]),
    )
    result = await generate_model_plan(
        profiling_result,
        generator=generator,
        config=ModelGenerationConfig(model="test-model"),
    )

    assert generator.calls == ["analyst"]
    assert result.diagnostics.fallback_reason == "analyst_timed_out"
    assert result.diagnostics.provider_receipts[0].outcome == "timed_out"
    assert "provider detail" not in result.diagnostics.model_dump_json()


async def test_invalid_analyst_output_skips_editor(
    profiling_result: ProfilingResult,
) -> None:
    generator = FakeGenerator(
        AnalystPlan(
            selected_candidate_ids=["unknown"],
            recommended_candidate_id="unknown",
            rationale="Useful.",
        ),
        _editor([profiling_result.candidates[0].id]),
    )
    result = await generate_model_plan(
        profiling_result,
        generator=generator,
        config=ModelGenerationConfig(model="test-model"),
    )
    assert generator.calls == ["analyst"]
    assert result.diagnostics.fallback_reason == "analyst_failed"


async def test_editor_failure_preserves_analyst_selection(
    profiling_result: ProfilingResult,
) -> None:
    first = profiling_result.candidates[-1].id
    generator = FakeGenerator(
        AnalystPlan(
            selected_candidate_ids=[first],
            recommended_candidate_id=first,
            rationale="Useful.",
        ),
        RuntimeError("failed"),
    )
    result = await generate_model_plan(
        profiling_result,
        generator=generator,
        config=ModelGenerationConfig(model="test-model"),
    )
    assert generator.calls == ["analyst", "editor"]
    assert result.selection.selected_candidate_ids == [first]
    assert result.mode == GenerationMode.DETERMINISTIC_FALLBACK


@pytest.mark.parametrize(
    "description",
    [
        "Inspect **this pattern**.",
        "Read docs.example.com/guide for details.",
        "Read docs.example.com for details.",
        "Inspect *this pattern*.",
        "Inspect __this pattern__.",
        "Inspect _this pattern_.",
        "Inspect `lookup_order`.",
        "Inspect ``lookup_order``.",
        "Inspect ~~this pattern~~.",
        "> Inspect this pattern.",
        "- Inspect this pattern.",
        "This pattern improved quality.",
        "It outperformed the alternative.",
        "The agent resolved these requests.",
        "The agent fixed the issue.",
    ],
)
async def test_editor_unsafe_copy_uses_deterministic_copy(
    profiling_result: ProfilingResult,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0]
    editor = _editor([candidate.id])
    editor = editor.model_copy(
        update={
            "insights": [
                editor.insights[0].model_copy(update={"description": description})
            ]
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )

    result = await generate_model_plan(
        profiling_result,
        generator=FakeGenerator(selection, editor),
        config=ModelGenerationConfig(model="test-model"),
    )

    assert result.selection == selection
    assert result.mode == GenerationMode.MODEL_BACKED
    assert result.diagnostics.fallback_reason is None
    assert result.diagnostics.warnings == [
        f"Card copy for {candidate.id} failed validation and uses deterministic text."
    ]
    assert result.diagnostics.provider_receipts == [
        _receipt("analyst"),
        _receipt("editor"),
    ]
    assert result.editorial.insights[0].description == candidate.fallback_description


@pytest.mark.parametrize(
    "description",
    [
        "Inspect lookup_order and lookup_customer_record.",
        "Inspect the tool's behavior (including retries).",
        "Inspect lookup_* calls.",
        "Inspect retries - then compare the cohort.",
        "Inspect the pattern, e.g. repeated lookups.",
    ],
)
def test_editor_preserves_plain_text_punctuation(
    profiling_result: ProfilingResult,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    editor = _editor([candidate.id])
    editor = editor.model_copy(
        update={
            "insights": [
                editor.insights[0].model_copy(update={"description": description})
            ]
        }
    )

    assert validate_editorial_plan(editor, selection, [candidate]) == editor


async def test_non_utf8_editor_copy_uses_deterministic_card_copy(
    profiling_result: ProfilingResult,
) -> None:
    first = profiling_result.candidates[0].id
    editor = _editor([first])
    invalid_editor = editor.model_copy(
        update={
            "insights": [
                editor.insights[0].model_copy(
                    update={"description": "broken-\ud800-description"}
                )
            ]
        }
    )
    generator = FakeGenerator(
        AnalystPlan(
            selected_candidate_ids=[first],
            recommended_candidate_id=first,
            rationale="Useful.",
        ),
        invalid_editor,
    )

    result = await generate_model_plan(
        profiling_result,
        generator=generator,
        config=ModelGenerationConfig(model="test-model"),
    )

    assert generator.calls == ["analyst", "editor"]
    assert result.mode == GenerationMode.MODEL_BACKED
    assert result.diagnostics.fallback_reason is None
    assert len(result.diagnostics.warnings) == 1
    assert result.diagnostics.provider_receipts == [
        _receipt("analyst"),
        _receipt("editor"),
    ]
    assert "broken" not in result.model_dump_json()


async def test_one_total_deadline_stops_before_editor(
    profiling_result: ProfilingResult,
) -> None:
    class SlowGenerator(FakeGenerator):
        async def analyze(self, *, projection, config, timeout_seconds):
            self.calls.append("analyst")
            await asyncio.sleep(0.05)
            return await super().analyze(
                projection=projection,
                config=config,
                timeout_seconds=timeout_seconds,
            )

    first = profiling_result.candidates[0].id
    generator = SlowGenerator(
        AnalystPlan(
            selected_candidate_ids=[first],
            recommended_candidate_id=first,
            rationale="Useful.",
        ),
        _editor([first]),
    )
    result = await generate_model_plan(
        profiling_result,
        generator=generator,
        config=ModelGenerationConfig(
            model="test-model",
            total_timeout_seconds=0.01,
            analyst_timeout_seconds=1,
            editor_timeout_seconds=1,
        ),
    )
    assert result.mode == GenerationMode.DETERMINISTIC_FALLBACK
    assert generator.calls == ["analyst"]


@pytest.fixture
def candidate_finding(profiling_result: ProfilingResult) -> CandidateFinding:
    return profiling_result.candidates[0]


def _single_selection(candidate: CandidateFinding) -> AnalystPlan:
    return AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )


def _card(candidate: CandidateFinding, description: str) -> EditorialPlan:
    return EditorialPlan(
        insights=[
            EditorialCardCopy(
                id=candidate.id, eyebrow="Tool behavior", description=description
            )
        ]
    )


@pytest.mark.parametrize(
    "description",
    [
        "The profiler found 3 empty results across 2 sessions.",
        "This affects 14.29% of the analyzed sessions.",
        "This affects 14.3% of the analyzed sessions.",
        "This affects 14% of the analyzed sessions.",
        "It takes 0.5 seconds on average.",
        "Every session in this group repeats the same call.",
        "Several sessions retry twice.",
        "This group has more retries than the rest.",
        "Look at the sessions with the longer durations first.",
    ],
)
def test_card_copy_may_restate_grounded_numbers_and_plain_quantities(
    profiling_result: ProfilingResult, description: str
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "facts": [
                DeterministicFact(name="empty_results", value=3),
                DeterministicFact(name="affected_share_percent", value=14.29),
                DeterministicFact(name="mean_seconds", value="0.5s"),
            ]
        }
    )
    copy = _card(candidate, description)
    assert (
        validate_editorial_plan(
            copy, selection := _single_selection(candidate), [candidate]
        )
        == copy
    )
    assert apply_editorial_copy(copy, selection, [candidate]) == (copy, [])


@pytest.mark.parametrize(
    "description",
    [
        "This affects 42% of sessions.",
        "It takes 17 ms to respond.",
        "About 1,500 calls were recorded.",
        "42% is worth investigating.",
    ],
)
def test_card_copy_rejects_numbers_absent_from_the_candidate(
    profiling_result: ProfilingResult, description: str
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "data": CategoricalInsightData(values=[CategoryValue(label="42%", value=1)])
        }
    )
    with pytest.raises(ValueError, match="numeric claim absent"):
        validate_editorial_plan(
            _card(candidate, description), _single_selection(candidate), [candidate]
        )


def test_apply_editorial_copy_degrades_only_the_rejected_card(
    profiling_result: ProfilingResult,
) -> None:
    first, second = profiling_result.candidates
    selection = AnalystPlan(
        selected_candidate_ids=[first.id, second.id],
        recommended_candidate_id=first.id,
        rationale="Useful.",
    )
    cards = EditorialPlan(
        insights=[
            EditorialCardCopy(
                id=first.id, eyebrow="Tool behavior", description="It takes 17 ms."
            ),
            EditorialCardCopy(
                id=second.id, eyebrow="Tool behavior", description="Fresh wording."
            ),
        ]
    )
    plan, rejected = apply_editorial_copy(cards, selection, [first, second])
    assert rejected == [first.id]
    assert plan.insights[0].description == first.fallback_description
    assert plan.insights[0].eyebrow == first.eyebrow
    assert plan.insights[1] == cards.insights[1]
    assert plan.intro_title == DEFAULT_INTRO_TITLE


def test_apply_editorial_copy_follows_the_selection_not_the_editor(
    profiling_result: ProfilingResult,
) -> None:
    first, second = profiling_result.candidates
    selection = AnalystPlan(
        selected_candidate_ids=[first.id, second.id],
        recommended_candidate_id=first.id,
        rationale="Useful.",
    )
    cards = EditorialPlan(
        insights=[
            EditorialCardCopy(
                id=second.id, eyebrow="Tool behavior", description="Fresh wording."
            ),
            EditorialCardCopy(
                id="candidate-9", eyebrow="Tool behavior", description="Extra card."
            ),
        ]
    )
    plan, rejected = apply_editorial_copy(cards, selection, [first, second])
    assert [item.id for item in plan.insights] == [first.id, second.id]
    assert rejected == [first.id]
    assert plan.insights[0].description == first.fallback_description
    assert plan.insights[1].description == "Fresh wording."


async def test_partially_rejected_editor_copy_keeps_model_backed_mode(
    profiling_result: ProfilingResult,
) -> None:
    first, second = profiling_result.candidates
    selection = AnalystPlan(
        selected_candidate_ids=[first.id, second.id],
        recommended_candidate_id=first.id,
        rationale="Useful.",
    )
    editor = EditorialPlan(
        insights=[
            EditorialCardCopy(
                id=first.id, eyebrow="Tool behavior", description="Fresh wording."
            ),
            EditorialCardCopy(
                id=second.id, eyebrow="Tool behavior", description="It takes 17 ms."
            ),
        ]
    )
    result = await generate_model_plan(
        profiling_result,
        generator=FakeGenerator(selection, editor),
        config=ModelGenerationConfig(model="test-model"),
    )
    assert result.mode == GenerationMode.MODEL_BACKED
    assert result.editorial.insights[0].description == "Fresh wording."
    assert result.editorial.insights[1].description == second.fallback_description
    assert result.diagnostics.warnings == [
        f"Card copy for {second.id} failed validation and uses deterministic text."
    ]


def test_card_copy_may_quote_the_candidate_caveat(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "title": "Sessions immediately retry the same failed call",
            "caveat": (
                "A recorded failure may be recovered later and is not the same as "
                "a failed session."
            ),
        }
    )
    selection = _single_selection(candidate)
    quoted = _card(
        candidate,
        "Start with the matching sessions, and keep in mind a recorded failure "
        "may be recovered later and is not the same as a failed session.",
    )
    assert validate_editorial_plan(quoted, selection, [candidate]) == quoted

    invented = _card(candidate, "No sessions failed in this group.")
    with pytest.raises(ValueError, match="negated outcome"):
        validate_editorial_plan(invented, selection, [candidate])


@pytest.mark.parametrize("stage", ["analyst", "editor"])
def test_model_projection_preserves_exact_count_with_bounded_references(
    profiling_result: ProfilingResult, stage: str
) -> None:
    candidate = profiling_result.candidates[0]
    session_ids = [uuid.uuid4(), uuid.uuid4()]
    candidate = candidate.model_copy(
        update={
            "coverage": CandidateCoverage(
                sessions_analyzed=10,
                affected_sessions=10,
                occurrences=10,
                evidence_available=10,
                evidence_retained=2,
                contributing_sessions_available=10,
                contributing_sessions_retained=2,
            ),
            "contributing_session_ids": session_ids,
            "evidence": [
                EvidenceLocator(session_id=session_id, signal="test")
                for session_id in session_ids
            ],
        }
    )
    profile = profiling_result.model_copy(update={"candidates": [candidate]})
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    projection = (
        build_analyst_projection(profile)
        if stage == "analyst"
        else build_editorial_projection(profile, selection)
    )

    projected = projection.candidates[0]
    assert projected.contributing_session_count == 10
    assert len(candidate.contributing_session_ids) == 2
    assert projected.evidence_locators == candidate.evidence
    assert len(projected.evidence_locators) == 2


def test_analyst_plan_requires_known_unique_ids(
    profiling_result: ProfilingResult,
) -> None:
    known = profiling_result.candidates
    first = known[0].id
    valid = AnalystPlan(
        selected_candidate_ids=[first],
        recommended_candidate_id=first,
        rationale="Strong and actionable.",
    )
    assert validate_analyst_plan(valid, known) == valid

    with pytest.raises(ValueError, match="unknown"):
        validate_analyst_plan(
            valid.model_copy(update={"selected_candidate_ids": ["unknown"]}), known
        )
    with pytest.raises(ValueError, match="unique"):
        validate_analyst_plan(
            valid.model_copy(update={"selected_candidate_ids": [first, first]}), known
        )
    with pytest.raises(ValueError, match="recommendation"):
        validate_analyst_plan(
            valid.model_copy(update={"recommended_candidate_id": "unknown"}), known
        )


async def test_analyst_selection_keeps_recommendation_and_diversifies_families(
    profiling_result: ProfilingResult,
) -> None:
    first, second = profiling_result.candidates
    duplicate = first.model_copy(
        update={"id": "candidate-duplicate", "family": first.family, "rank": 1}
    )
    unused = second.model_copy(
        update={"id": "candidate-unused", "family": "family-unused", "rank": 3}
    )
    profiling = profiling_result.model_copy(
        update={"candidates": [first, duplicate, second, unused]}
    )
    analyst = AnalystPlan(
        selected_candidate_ids=[first.id, duplicate.id, second.id],
        recommended_candidate_id=duplicate.id,
        rationale="Useful.",
    )
    expected_ids = [duplicate.id, second.id, unused.id]

    result = await generate_model_plan(
        profiling,
        generator=FakeGenerator(analyst, _editor(expected_ids)),
        config=ModelGenerationConfig(model="test-model"),
    )

    assert result.selection.selected_candidate_ids == expected_ids
    assert result.selection.recommended_candidate_id == duplicate.id
    candidates = {candidate.id: candidate for candidate in profiling.candidates}
    assert len(
        {
            candidates[candidate_id].family
            for candidate_id in result.selection.selected_candidate_ids
        }
    ) == len(result.selection.selected_candidate_ids)


def test_editor_preserves_selection_and_allows_known_digit_label(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "title": "gpt-5.4 appears in the model mix",
            "data": CategoricalInsightData(
                values=[CategoryValue(label="gpt-5.4", value=1)]
            ),
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Model mix",
                    description="gpt-5.4 appears often enough to inspect.",
                )
            ]
        }
    )
    assert validate_editorial_plan(copy, selection, [candidate]) == copy

    novel = copy.model_copy(
        update={
            "insights": [
                copy.insights[0].model_copy(
                    update={"description": "This affects 42% of sessions."}
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="numeric"):
        validate_editorial_plan(novel, selection, [candidate])

    with pytest.raises(ValueError, match="membership and order"):
        validate_editorial_plan(
            _editor(["unknown"]),
            selection,
            [candidate],
        )


def test_editor_validates_numbers_against_each_card_only(
    profiling_result: ProfilingResult,
) -> None:
    first, second = profiling_result.candidates
    first = first.model_copy(
        update={
            "data": CategoricalInsightData(
                values=[CategoryValue(label="gpt-5.4", value=1)]
            )
        }
    )
    second = second.model_copy(
        update={
            "data": CategoricalInsightData(
                values=[CategoryValue(label="claude-3.7", value=1)]
            )
        }
    )
    selection = AnalystPlan(
        selected_candidate_ids=[first.id, second.id],
        recommended_candidate_id=first.id,
        rationale="Useful.",
    )
    copy = _editor(selection.selected_candidate_ids).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=first.id,
                    eyebrow="Model mix",
                    description="claude-3.7 appears in this pattern.",
                ),
                EditorialCardCopy(
                    id=second.id,
                    eyebrow="Model mix",
                    description="This pattern is worth a closer look.",
                ),
            ]
        }
    )
    with pytest.raises(ValueError, match="numeric"):
        validate_editorial_plan(copy, selection, [first, second])


@pytest.mark.parametrize(
    ("description", "message"),
    [
        ("It takes 17 ms to respond.", "numeric"),
        ("These sessions timed out.", "outcome"),
        ("The agent resolved these requests.", "outcome"),
        ("The agent fixed the issue.", "outcome"),
        ("Read https://example.com for details.", "link"),
        ("Read docs.example.com/guide for details.", "link"),
        ("Read docs.example.com for details.", "link"),
        ("Read docs.example.email for details.", "link"),
        ("Read example.ai for details.", "link"),
        ("Read example.ai/guide for details.", "link"),
        ("Read DOCS.EXAMPLE.COM/guide for details.", "link"),
        ("# Tool behavior", "markup"),
        ("---", "markup"),
        ("***", "markup"),
        ("_ _ _", "markup"),
        ("Tool behavior\n---", "markup"),
        ("Tool behavior\n===", "markup"),
        ("~~~\ninspect\n~~~", "markup"),
        ("Inspect this\x00pattern.", "control"),
        ("This causes retries.", "unsupported claim"),
        ("This is causing retries.", "unsupported claim"),
        ("This results in retries.", "unsupported claim"),
        ("This resulted in retries.", "unsupported claim"),
        ("This is resulting in retries.", "unsupported claim"),
        ("This leads to retries.", "unsupported claim"),
        ("This led to retries.", "unsupported claim"),
        ("This is leading to retries.", "unsupported claim"),
        ("This pattern drives retries.", "unsupported claim"),
        ("This pattern drove retries.", "unsupported claim"),
        ("This pattern may drive retries.", "unsupported claim"),
        ("This pattern is driving retries.", "unsupported claim"),
        ("Retries are driven by this pattern.", "unsupported claim"),
        ("This pattern has driven retries.", "unsupported claim"),
        ("This pattern stems from retries.", "unsupported claim"),
        ("This pattern stemmed from retries.", "unsupported claim"),
        ("This pattern produces retries.", "unsupported claim"),
        ("This pattern may produce retries.", "unsupported claim"),
        ("This pattern produced retries.", "unsupported claim"),
        ("This pattern creates retries.", "unsupported claim"),
        ("This pattern may create retries.", "unsupported claim"),
        ("This pattern created retries.", "unsupported claim"),
        ("This pattern triggers retries.", "unsupported claim"),
        ("This pattern may trigger retries.", "unsupported claim"),
        ("This pattern triggered retries.", "unsupported claim"),
        ("This pattern is responsible for retries.", "unsupported claim"),
        ("Retries are attributable to this pattern.", "unsupported claim"),
        ("This pattern accounts for retries.", "unsupported claim"),
        ("This pattern may account for retries.", "unsupported claim"),
        ("This pattern accounted for retries.", "unsupported claim"),
        ("This pattern is accounting for retries.", "unsupported claim"),
        ("This pattern contributes to retries.", "unsupported claim"),
        ("This pattern gives rise to retries.", "unsupported claim"),
        ("This pattern is giving rise to retries.", "unsupported claim"),
        ("This pattern has given rise to retries.", "unsupported claim"),
        ("This pattern brought about retries.", "unsupported claim"),
        ("This pattern is bringing about retries.", "unsupported claim"),
        ("Retries arise from this pattern.", "unsupported claim"),
        ("Retries are arising from this pattern.", "unsupported claim"),
        ("Retries originated from this pattern.", "unsupported claim"),
        ("This pattern explains retries.", "unsupported claim"),
        ("This pattern determines retries.", "unsupported claim"),
        ("This pattern improved quality.", "unsupported claim"),
        ("This pattern improves quality.", "unsupported claim"),
        ("This pattern is improving quality.", "unsupported claim"),
        ("Quality improvements appeared.", "unsupported claim"),
        ("It outperformed the alternative.", "unsupported claim"),
        ("It outperforms the alternative.", "unsupported claim"),
        ("It is outperforming the alternative.", "unsupported claim"),
        ("These paths outperform the alternative.", "unsupported claim"),
    ],
)
def test_editor_rejects_fabricated_or_unsafe_card_copy(
    profiling_result: ProfilingResult,
    description: str,
    message: str,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={
            "insights": [
                EditorialCardCopy(
                    id=candidate.id,
                    eyebrow="Tool behavior",
                    description=description,
                )
            ]
        }
    )
    with pytest.raises(ValueError, match=message):
        validate_editorial_plan(copy, selection, [candidate])
