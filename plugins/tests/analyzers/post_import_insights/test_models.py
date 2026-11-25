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
"""Tests for the structured insight report generation result contract."""

import json
import uuid

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.insight import (
    Bin,
    BinnedArtifact,
    CategoricalArtifact,
    CategoryValue,
    EvidenceArtifact,
    EvidenceRow,
    InsightAction,
    InsightBatchCreateRequest,
    InsightCoverage,
    InsightFinding,
    InsightHighlight,
    InsightInput,
    RateArtifact,
)
from kitaru_post_import_insights import (
    INSIGHT_METADATA_KEY,
    Coverage,
    CoverageTruncation,
    FindingMetadata,
    GenerationDiagnostics,
    GenerationMode,
    GenerationVersions,
    InsightGenerationContext,
    InsightGenerationResult,
    InsightReportMetadata,
    PageIntro,
    PageRecommendation,
    ProviderReceipt,
    SourceImportContext,
)

AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
SESSION_A = uuid.UUID("01990000-0000-7000-8000-000000000003")
SESSION_B = uuid.UUID("01990000-0000-7000-8000-000000000004")
NODE_A = uuid.UUID("01990000-0000-7000-8000-000000000005")


def _context() -> InsightGenerationContext:
    return InsightGenerationContext(
        agent_id=AGENT_ID,
        agent_name="returns-agent",
        source_import=SourceImportContext(
            import_id=IMPORT_ID,
            provider="langfuse",
        ),
        source_provider="langfuse",
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("agent_name", "broken-\ud800-name"),
        ("provider", "broken-\udfff-provider"),
    ],
)
def test_context_rejects_strings_that_are_not_utf8_encodable(
    field: str, value: str
) -> None:
    """Reject unpaired surrogates before JSON and byte-bound operations."""
    context: dict[str, object] = {
        "agent_id": AGENT_ID,
        "agent_name": "returns-agent",
        "source_import": {
            "import_id": IMPORT_ID,
            "provider": "langfuse",
        },
    }
    if field == "provider":
        context["source_import"] = {
            "import_id": IMPORT_ID,
            "provider": value,
        }
    else:
        context[field] = value

    with pytest.raises(ValidationError, match="valid UTF-8 text"):
        InsightGenerationContext.model_validate(context)


def test_context_preserves_valid_unicode() -> None:
    """Accept and preserve valid non-ASCII context text."""
    context = InsightGenerationContext(
        agent_id=AGENT_ID,
        agent_name="retürns-🤖",
        source_import=SourceImportContext(
            import_id=IMPORT_ID,
            provider="långfüse",
        ),
        source_provider="långfüse",
    )

    restored = InsightGenerationContext.model_validate_json(context.model_dump_json())
    assert restored == context


@pytest.mark.parametrize("field", ["request_id", "model"])
def test_provider_receipt_rejects_non_utf8_text(field: str) -> None:
    """Reject malformed custom-provider receipt strings at the shared boundary."""
    values = {
        "stage": "analyst",
        "latency_ms": 1,
        "outcome": "succeeded",
        field: "broken-\ud800-value",
    }

    with pytest.raises(ValidationError, match="valid UTF-8 text"):
        ProviderReceipt.model_validate(values)


def test_provider_receipt_preserves_valid_unicode() -> None:
    """Preserve valid Unicode provider receipt identifiers."""
    receipt = ProviderReceipt(
        stage="analyst",
        request_id="résp-🤖",
        model="modèle-λ",
        latency_ms=1,
        outcome="succeeded",
    )

    restored = ProviderReceipt.model_validate_json(receipt.model_dump_json())
    assert restored == receipt


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


def _report_metadata() -> InsightReportMetadata:
    return InsightReportMetadata(
        recommended_finding_id="tool-error-rate",
        coverage=_coverage(),
        context=_context(),
        generation=GenerationVersions(
            analysis="2026-11-09.1",
            prompt="2026-11-25.1",
        ),
        findings={
            "tool-error-rate": FindingMetadata(
                eyebrow="TOOL RELIABILITY",
                position=0,
                recommended=True,
                cta_label="Investigate tool errors",
            ),
            "tool-call-tail": FindingMetadata(
                eyebrow="TOOL CALLS",
                position=1,
                recommended=False,
                cta_label="Investigate tool call counts",
            ),
        },
    )


def _result() -> InsightGenerationResult:
    metadata = _report_metadata()
    findings = [
        InsightFinding(
            id="tool-error-rate",
            title="Tool errors affect 2 of 3 sessions",
            summary="A deterministic finding with editorial explanation.",
            body="A deterministic finding with editorial explanation.",
            tier="primary",
            severity="high",
            artifacts=[
                RateArtifact(
                    presentation="share_bar",
                    rate=0.6667,
                    numerator=2,
                    denominator=3,
                    tone="risk",
                ),
                CategoricalArtifact(
                    presentation="ranked_bars",
                    unit="sessions",
                    values=[
                        CategoryValue(label="tool error", value=2),
                        CategoryValue(label="no tool error", value=1),
                    ],
                ),
                EvidenceArtifact(
                    presentation="table",
                    total_sessions=2,
                    rows=[
                        EvidenceRow(
                            session_id=SESSION_A,
                            node_id=NODE_A,
                            signal="tool_error",
                        )
                    ],
                ),
            ],
            contributing_session_ids=[SESSION_A],
            check_first="A tool error may have been recovered later in the session.",
            action=InsightAction(
                label="Investigate tool errors",
                prompt="Investigate the tool-error pattern in these sessions.",
            ),
        ),
        InsightFinding(
            id="tool-call-tail",
            title="One session makes 10 or more tool calls",
            summary="Baseline distribution of tool calls per session.",
            tier="secondary",
            artifacts=[
                BinnedArtifact(
                    presentation="micro",
                    unit="calls",
                    bins=[
                        Bin(lower_bound=0, upper_bound=5, count=2),
                        Bin(lower_bound=5, upper_bound=None, count=1),
                    ],
                )
            ],
            contributing_session_ids=[SESSION_A],
            action=InsightAction(
                label="Investigate tool call counts",
                prompt="Investigate high tool call counts.",
            ),
        ),
    ]
    report = InsightInput(
        name="openai-post-import-insights",
        title="Tool errors affect 2 of 3 sessions",
        summary="A deterministic finding with editorial explanation.",
        source_provider="langfuse",
        coverage=InsightCoverage(
            sessions_analyzed=2,
            sessions_available=3,
            skipped=1,
            failed=0,
            caveats=["One session was outside the configured limit."],
        ),
        highlight=InsightHighlight(finding_id="tool-error-rate", artifact_index=0),
        next_step=InsightAction(
            label="Investigate tool errors",
            summary="A deterministic finding with editorial explanation.",
            prompt="Investigate the tool-error pattern in these sessions.",
        ),
        findings=findings,
        metadata={INSIGHT_METADATA_KEY: metadata.model_dump(mode="json")},
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
        insights=[report],
    )


def test_result_uses_canonical_insight_inputs_for_batch_creation() -> None:
    """Pass generated structured report directly to batch creation."""
    result = _result()

    assert len(result.insights) == 1
    report = result.insights[0]
    assert isinstance(report.findings[0].artifacts[0], RateArtifact)
    assert isinstance(report.findings[1].artifacts[0], BinnedArtifact)
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
    """Preserve UUIDs, order, findings, coverage, and receipts through JSON."""
    result = _result()

    restored = InsightGenerationResult.model_validate_json(result.model_dump_json())

    assert restored == result
    assert restored.context.agent_id == AGENT_ID
    report = restored.insights[0]
    assert [finding.id for finding in report.findings] == [
        "tool-error-rate",
        "tool-call-tail",
    ]
    assert report.findings[0].contributing_session_ids == [SESSION_A]
    evidence = report.findings[0].artifacts[2]
    assert isinstance(evidence, EvidenceArtifact)
    assert evidence.rows[0].node_id == NODE_A
    assert report.findings[0].check_first == (
        "A tool error may have been recovered later in the session."
    )
    metadata = restored.report_metadata(report)
    assert metadata.recommended_finding_id == "tool-error-rate"


def test_duplicate_positions_are_rejected() -> None:
    """Reject findings that cannot be placed in one stable order."""
    data = _result().model_dump()
    data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["findings"]["tool-call-tail"][
        "position"
    ] = 0

    with pytest.raises(ValidationError, match="positions"):
        InsightGenerationResult.model_validate(data)


def test_positions_must_match_serialized_list_order() -> None:
    """Reject contiguous positions attached to the wrong finding entries."""
    data = _result().model_dump()
    findings_meta = data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["findings"]
    findings_meta["tool-error-rate"]["position"] = 1
    findings_meta["tool-call-tail"]["position"] = 0

    with pytest.raises(ValidationError, match="list order"):
        InsightGenerationResult.model_validate(data)


def test_report_coverage_must_match_result_coverage() -> None:
    """Reject a report whose coverage contradicts the result envelope."""
    data = _result().model_dump()
    data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["coverage"][
        "sessions_analyzed"
    ] = 1

    with pytest.raises(ValidationError, match="coverage"):
        InsightGenerationResult.model_validate(data)


def test_multiple_recommended_findings_are_rejected() -> None:
    """Reject more than one finding marked as the recommendation."""
    data = _result().model_dump()
    data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["findings"]["tool-call-tail"][
        "recommended"
    ] = True

    with pytest.raises(ValidationError, match="recommended"):
        InsightGenerationResult.model_validate(data)


def test_recommendation_must_match_recommended_finding() -> None:
    """Keep page-level and report-level recommendations consistent."""
    data = _result().model_dump()
    data["recommendation"]["insight_name"] = "tool-call-tail"

    with pytest.raises(ValidationError, match="recommendation"):
        InsightGenerationResult.model_validate(data)


def test_unknown_report_metadata_fields_are_rejected() -> None:
    """Reject metadata that silently extends the report contract."""
    data = _result().model_dump()
    data["insights"][0]["metadata"][INSIGHT_METADATA_KEY]["url"] = (
        "https://example.com/insights/created-id"
    )

    with pytest.raises(ValidationError, match="url"):
        InsightGenerationResult.model_validate(data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", "invalid insight name"),
        ("name", "x" * 256),
        ("title", ""),
        ("title", "x" * 256),
    ],
)
def test_report_identity_respects_domain_constraints(field: str, value: str) -> None:
    """Reject report names and titles the Insight domain cannot persist."""
    data = _result().model_dump()
    data["insights"][0][field] = value

    with pytest.raises(ValidationError, match=field):
        InsightGenerationResult.model_validate(data)


def test_findings_without_artifacts_are_rejected() -> None:
    """Keep every generated finding backed by at least one artifact."""
    data = _result().model_dump()
    data["insights"][0]["findings"][0]["artifacts"] = []

    with pytest.raises(ValidationError, match="artifact"):
        InsightGenerationResult.model_validate(data)


@pytest.mark.parametrize("label", ["Copy tool retry prompt", "x" * 60])
def test_cta_label_round_trip(label: str) -> None:
    payload = _result().model_dump(mode="json")
    for meta in payload["insights"][0]["metadata"][INSIGHT_METADATA_KEY][
        "findings"
    ].values():
        meta["cta_label"] = label
    restored = InsightGenerationResult.model_validate_json(json.dumps(payload))
    report_meta = restored.report_metadata(restored.insights[0])
    assert all(meta.cta_label == label for meta in report_meta.findings.values())


@pytest.mark.parametrize("label", ["", "   ", "x" * 61, 123, "broken-\ud800"])
def test_invalid_cta_label_is_rejected(label: object) -> None:
    payload = FindingMetadata(eyebrow="TOOL", position=0, recommended=True).model_dump()
    payload["cta_label"] = label
    with pytest.raises(ValidationError):
        FindingMetadata.model_validate(payload)
