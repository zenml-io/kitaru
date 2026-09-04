#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Tests for the lazy OpenAI insight generator adapter."""

import uuid
from types import SimpleNamespace

import pytest

from kitaru.api_models.v1.insight import CategoricalInsightData, CategoryValue
from kitaru.insights.generation import (
    AnalystPlan,
    AnalystProjection,
    CandidateProjection,
    ModelGenerationConfig,
)
from kitaru.insights.models import EvidenceLocator
from kitaru.insights.openai_generator import (
    MissingOpenAICredential,
    OpenAIInsightGenerator,
)


class FakeResponses:
    def __init__(self, parsed: AnalystPlan) -> None:
        self.parsed = parsed
        self.kwargs: dict[str, object] | None = None

    async def parse(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(
            id="resp_1",
            model="returned-model",
            usage=SimpleNamespace(input_tokens=10, output_tokens=5),
            output_parsed=self.parsed,
        )


def _projection() -> AnalystProjection:
    return AnalystProjection(
        content_hash="a" * 64,
        candidates=[
            CandidateProjection(
                id="candidate",
                family="tools",
                rank=0,
                deterministic_title="A tool repeats",
                deterministic_description="The same call appears again.",
                detector_description="Repeated calls",
                caveat=None,
                facts=[],
                chart_data=CategoricalInsightData(
                    values=[CategoryValue(label="Observed", value=1)]
                ),
                evidence_locators=[
                    EvidenceLocator(
                        session_id=uuid.UUID("01990000-0000-7000-8000-000000000001"),
                        signal="test",
                    )
                ],
                contributing_session_count=1,
            )
        ],
    )


def test_missing_credential_fails_closed(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(MissingOpenAICredential):
        OpenAIInsightGenerator()


async def test_openai_request_is_bounded_and_not_stored(monkeypatch) -> None:
    plan = AnalystPlan(
        selected_candidate_ids=["candidate"],
        recommended_candidate_id="candidate",
        rationale="Useful.",
    )
    responses = FakeResponses(plan)
    constructed = {}

    class FakeAsyncOpenAI:
        def __init__(self, **kwargs) -> None:
            constructed.update(kwargs)
            self.responses = responses

    monkeypatch.setattr(
        "kitaru.insights.openai_generator.importlib.import_module",
        lambda name: SimpleNamespace(AsyncOpenAI=FakeAsyncOpenAI),
    )
    generator = OpenAIInsightGenerator(api_key="test-secret")
    result = await generator.analyze(
        projection=_projection(),
        config=ModelGenerationConfig(model="gpt-test"),
        timeout_seconds=4.0,
    )

    assert constructed == {"api_key": "test-secret", "max_retries": 0}
    assert responses.kwargs is not None
    assert responses.kwargs["store"] is False
    assert responses.kwargs["model"] == "gpt-test"
    assert responses.kwargs["max_output_tokens"] == 1000
    assert responses.kwargs["timeout"] == 4.0
    assert responses.kwargs["text_format"] is AnalystPlan
    assert "test-secret" not in repr(generator)
    assert result.receipt.request_id == "resp_1"
