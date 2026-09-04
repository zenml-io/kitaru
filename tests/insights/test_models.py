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
"""Tests for the insight generation result contract."""

import uuid

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.insight import (
    Bin,
    BinnedInsightData,
    CategoricalInsightData,
    CategoryValue,
    InsightBatchCreateRequest,
    InsightInput,
    TextInsightData,
)
from kitaru.insights import (
    INSIGHT_METADATA_KEY,
    Coverage,
    CoverageTruncation,
    EvidenceLocator,
    GenerationDiagnostics,
    GenerationMode,
    GenerationVersions,
    InsightCardMetadata,
    InsightGenerationContext,
    InsightGenerationResult,
    PageIntro,
    PageRecommendation,
    ProviderReceipt,
    SourceImportContext,
)

AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
IMPORT_TASK_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
SESSION_A = uuid.UUID("01990000-0000-7000-8000-000000000003")
SESSION_B = uuid.UUID("01990000-0000-7000-8000-000000000004")
NODE_A = uuid.UUID("01990000-0000-7000-8000-000000000005")


def _context() -> InsightGenerationContext:
    return InsightGenerationContext(
        agent_id=AGENT_ID,
        agent_name="returns-agent",
        source_import=SourceImportContext(
            task_id=IMPORT_TASK_ID,
            provider="langfuse",
        ),
    )


def _coverage() -> Coverage:
    return Coverage(
        sessions_available=3,
        sessions_analyzed=2,
        nodes_available=8,
        nodes_analyzed=6,
        inspected_text_bytes=120,
        truncations=[
            CoverageTruncation(
                dimension="sessions",
                available=3,
                analyzed=2,
            )
        ],
        caveats=["One session was outside the configured limit."],
    )


def _metadata(
    *,
    position: int,
    recommended: bool,
    session_ids: list[uuid.UUID] | None = None,
    evidence: list[EvidenceLocator] | None = None,
) -> InsightCardMetadata:
    contribution_ids = session_ids or [SESSION_A]
    return InsightCardMetadata(
        eyebrow="TOOL RELIABILITY",
        position=position,
        recommended=recommended,
        contributing_session_ids=contribution_ids,
        evidence=evidence
        or [
            EvidenceLocator(
                session_id=contribution_ids[0],
                node_id=NODE_A,
                signal="tool_error",
            )
        ],
        coverage=_coverage(),
        investigation_prompt=(
            "Investigate the tool-error pattern in these sessions, define or "
            "refine a cohort, and propose one or more experiments."
        ),
        context=_context(),
        generation=GenerationVersions(
            analysis="2026-09-04.1",
            prompt="2026-09-04.1",
        ),
    )


def _insight(
    *,
    name: str,
    title: str,
    data: CategoricalInsightData | BinnedInsightData | TextInsightData,
    metadata: InsightCardMetadata,
) -> InsightInput:
    return InsightInput(
        name=name,
        title=title,
        description="A deterministic finding with editorial explanation.",
        data=data,
        metadata={INSIGHT_METADATA_KEY: metadata.model_dump(mode="json")},
    )


def _result() -> InsightGenerationResult:
    categorical = _insight(
        name="tool-error-rate",
        title="Tool errors affect 2 of 3 sessions",
        data=CategoricalInsightData(
            unit="sessions",
            values=[
                CategoryValue(label="tool error", value=2),
                CategoryValue(label="no tool error", value=1),
            ],
        ),
        metadata=_metadata(position=0, recommended=True),
    )
    binned = _insight(
        name="tool-call-tail",
        title="One session makes 10 or more tool calls",
        data=BinnedInsightData(
            unit="calls",
            bins=[
                Bin(lower_bound=0, upper_bound=5, count=2),
                Bin(lower_bound=5, upper_bound=None, count=1),
            ],
        ),
        metadata=_metadata(position=1, recommended=False),
    )
    return InsightGenerationResult(
        context=_context(),
        coverage=_coverage(),
        mode=GenerationMode.MODEL_BACKED,
        page_intro=PageIntro(
            eyebrow="WHAT TO WORK ON FIRST",
            title="A tool failure is creating avoidable work",
            description="Start with the sessions where the tool failed.",
        ),
        recommendation=PageRecommendation(
            insight_name="tool-error-rate",
            title="Recommended next step",
            description="Investigate the tool-error pattern first.",
        ),
        diagnostics=GenerationDiagnostics(
            provider_receipts=[
                ProviderReceipt(
                    stage="analyst",
                    request_id="req_123",
                    model="gpt-5.4",
                    input_tokens=100,
                    output_tokens=20,
                    latency_ms=250,
                    outcome="succeeded",
                )
            ]
        ),
        insights=[categorical, binned],
    )


def test_result_uses_canonical_insight_inputs_for_batch_creation() -> None:
    """Pass generated categorical and binned inputs directly to batch creation."""
    result = _result()

    assert isinstance(result.insights[0].data, CategoricalInsightData)
    assert isinstance(result.insights[1].data, BinnedInsightData)
    request = InsightBatchCreateRequest(
        agent_id=result.context.agent_id,
        insights=result.insights,
    )
    assert request.insights == result.insights


def test_empty_result_is_valid_without_batch_request() -> None:
    """Represent an honest empty generation result."""
    result = InsightGenerationResult(
        context=_context(),
        coverage=Coverage(),
        mode=GenerationMode.DETERMINISTIC,
        empty_reason="No eligible deterministic findings were found.",
    )

    assert result.insights == []
    assert result.page_intro is None
    assert result.recommendation is None


def test_result_round_trips_through_json() -> None:
    """Preserve UUIDs, order, metadata, coverage, and receipts through JSON."""
    result = _result()

    restored = InsightGenerationResult.model_validate_json(result.model_dump_json())

    assert restored == result
    assert restored.context.agent_id == AGENT_ID
    assert [insight.name for insight in restored.insights] == [
        "tool-error-rate",
        "tool-call-tail",
    ]
    metadata = restored.card_metadata(restored.insights[0])
    assert metadata.contributing_session_ids == [SESSION_A]
    assert metadata.evidence[0].node_id == NODE_A


def test_duplicate_positions_are_rejected() -> None:
    """Reject cards that cannot be placed in one stable order."""
    data = _result().model_dump()
    data["insights"][1]["metadata"][INSIGHT_METADATA_KEY]["position"] = 0

    with pytest.raises(ValidationError, match="positions"):
        InsightGenerationResult.model_validate(data)


def test_positions_must_match_serialized_list_order() -> None:
    """Reject contiguous positions attached to the wrong list entries."""
    data = _result().model_dump()
    data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["position"] = 1
    data["insights"][1]["metadata"][INSIGHT_METADATA_KEY]["position"] = 0

    with pytest.raises(ValidationError, match="list order"):
        InsightGenerationResult.model_validate(data)


def test_card_coverage_must_match_result_coverage() -> None:
    """Reject a card whose coverage contradicts the result envelope."""
    data = _result().model_dump()
    data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["coverage"][
        "sessions_analyzed"
    ] = 1

    with pytest.raises(ValidationError, match="coverage"):
        InsightGenerationResult.model_validate(data)


def test_multiple_recommended_cards_are_rejected() -> None:
    """Reject more than one card marked as the recommendation."""
    data = _result().model_dump()
    data["insights"][1]["metadata"][INSIGHT_METADATA_KEY]["recommended"] = True

    with pytest.raises(ValidationError, match="recommended"):
        InsightGenerationResult.model_validate(data)


def test_recommendation_must_match_recommended_card() -> None:
    """Keep page-level and card-level recommendations consistent."""
    data = _result().model_dump()
    data["recommendation"]["insight_name"] = "tool-call-tail"

    with pytest.raises(ValidationError, match="recommendation"):
        InsightGenerationResult.model_validate(data)


def test_unknown_card_metadata_fields_are_rejected() -> None:
    """Reject metadata that silently extends the card contract."""
    data = _result().model_dump()
    data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["url"] = (
        "https://example.com/insights/created-id"
    )

    with pytest.raises(ValidationError, match="url"):
        InsightGenerationResult.model_validate(data)


def test_oversized_evidence_is_rejected() -> None:
    """Bound evidence retained on each card."""
    data = _result().model_dump()
    evidence = data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["evidence"]
    data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["evidence"] = evidence * 21

    with pytest.raises(ValidationError, match="evidence"):
        InsightGenerationResult.model_validate(data)


def test_evidence_sessions_must_contribute_to_card() -> None:
    """Reject evidence referring to a session outside the contribution set."""
    with pytest.raises(ValidationError, match="contributing"):
        _metadata(
            position=0,
            recommended=True,
            session_ids=[SESSION_A],
            evidence=[
                EvidenceLocator(
                    session_id=SESSION_B,
                    node_id=None,
                    signal="long_session",
                )
            ],
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", "invalid insight name"),
        ("name", "x" * 256),
        ("title", ""),
        ("title", "x" * 256),
    ],
)
def test_card_identity_respects_domain_constraints(field: str, value: str) -> None:
    """Reject card names and titles the Insight domain cannot persist."""
    data = _result().model_dump()
    data["insights"][0][field] = value

    with pytest.raises(ValidationError, match=field):
        InsightGenerationResult.model_validate(data)


def test_text_insights_are_rejected() -> None:
    """Keep generated cards chart-backed."""
    data = _result().model_dump()
    data["insights"][0]["data"] = {"type": "text", "content": "Unsupported"}

    with pytest.raises(ValidationError, match="categorical or binned"):
        InsightGenerationResult.model_validate(data)
