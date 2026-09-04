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
from kitaru.insights.generation import (
    AnalystPlan,
    EditorialCardCopy,
    EditorialPlan,
    InsightModelGenerator,
    ModelGenerationConfig,
    ModelStageResponse,
    generate_deterministic_plan,
    generate_model_plan,
    validate_analyst_plan,
    validate_editorial_plan,
)
from kitaru.insights.models import (
    Coverage,
    EvidenceLocator,
    GenerationMode,
    ProviderReceipt,
)
from kitaru.insights.profiling import (
    CandidateCoverage,
    CandidateFinding,
    DeterministicFact,
    ProfilingResult,
)


def _receipt(stage: Literal["analyst", "editor"]) -> ProviderReceipt:
    return ProviderReceipt(stage=stage, latency_ms=1, outcome="succeeded")


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
        intro_title="A few patterns deserve attention",
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
        ("Two sessions need attention.", "quantitative"),
        ("This happens twice as often.", "quantitative"),
        ("These sessions timed out.", "outcome"),
        ("Read https://example.com for details.", "link"),
        ("# Tool behavior", "markup"),
        ("Inspect this\x00pattern.", "control"),
        ("This causes retries.", "unsupported claim"),
        ("This has higher activity.", "unsupported claim"),
        ("This path is slower.", "unsupported claim"),
        ("This path is slowest.", "unsupported claim"),
        ("This path is faster.", "unsupported claim"),
        ("This path is fastest.", "unsupported claim"),
        ("This result is better.", "unsupported claim"),
        ("This result is best.", "unsupported claim"),
        ("This result is worse.", "unsupported claim"),
        ("This result is worst.", "unsupported claim"),
        ("All sessions need attention.", "quantitative"),
        ("Every session needs attention.", "quantitative"),
        ("Each session needs attention.", "quantitative"),
        ("Both tools need attention.", "quantitative"),
        ("Half the sessions need attention.", "quantitative"),
        ("The rate doubled.", "quantitative"),
        ("The rate is doubling.", "quantitative"),
        ("The rate tripled.", "quantitative"),
        ("The rate is tripling.", "quantitative"),
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


def test_editor_validates_page_copy_without_borrowing_card_facts(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={"fallback_description": "One session failed."}
    )
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={"intro_title": "1 session failed"}
    )
    with pytest.raises(ValueError, match=r"page copy.*numeric"):
        validate_editorial_plan(copy, selection, [candidate])


def test_editor_rejects_word_quantity_in_page_copy(
    profiling_result: ProfilingResult,
) -> None:
    candidate = profiling_result.candidates[0]
    selection = AnalystPlan(
        selected_candidate_ids=[candidate.id],
        recommended_candidate_id=candidate.id,
        rationale="Useful.",
    )
    copy = _editor([candidate.id]).model_copy(
        update={"recommendation_description": "Try this twice."}
    )
    with pytest.raises(ValueError, match="quantitative"):
        validate_editorial_plan(copy, selection, [candidate])


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


@pytest.mark.parametrize(
    ("label", "description"),
    [
        ("2", "2 is worth investigating."),
        ("100%", "100% is worth investigating."),
        ("Two", "Two is worth investigating."),
        ("All", "All are worth investigating."),
    ],
)
def test_quantity_only_label_cannot_mask_fabricated_quantity(
    profiling_result: ProfilingResult,
    label: str,
    description: str,
) -> None:
    candidate = profiling_result.candidates[0].model_copy(
        update={
            "data": CategoricalInsightData(values=[CategoryValue(label=label, value=1)])
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
                    description=description,
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="numeric or quantitative"):
        validate_editorial_plan(copy, selection, [candidate])


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
                    description="Failures are worth investigating.",
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
