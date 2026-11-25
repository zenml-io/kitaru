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
"""Behavioral tests for structured reports, rich artifacts, and issue #1516 fixes."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from kitaru.api_models.v1.insight import (
    BinnedArtifact,
    CategoricalArtifact,
    EvidenceArtifact,
    FailureMatrixArtifact,
    RateArtifact,
    StepStripArtifact,
    TableArtifact,
    ToolCycleArtifact,
)
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru_post_import_insights.models import (
    InsightGenerationContext,
    InsightGenerationResult,
    SourceImportContext,
)
from kitaru_post_import_insights.pipeline import (
    InsightGenerationConfig,
    generate_insights,
    generate_insights_from_profile,
)
from kitaru_post_import_insights.profiling import profile_sessions

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
OWNER_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000003")


def _tool_node(
    session_id: uuid.UUID,
    index: int,
    name: str,
    *,
    status: NodeStatus = NodeStatus.COMPLETED,
    inputs: dict[str, object] | None = None,
    outputs: object = "ok",
    offset_seconds: int = 0,
) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=uuid.UUID(int=session_id.int + 10_000 + index),
        session_id=session_id,
        external_id=f"tool-{index}",
        parent_external_id=None,
        links=[],
        node_type=NodeType.TOOL_CALL,
        name=name,
        status=status,
        started_at=NOW + timedelta(seconds=offset_seconds),
        ended_at=NOW + timedelta(seconds=offset_seconds + 1),
        inputs=inputs if inputs is not None else {"query": f"item-{index}"},
        outputs=outputs,
        tool_name=name,
        metadata={},
    )


def _session(
    number: int,
    nodes: list[SessionNodeResponse],
    *,
    status: SessionStatus = SessionStatus.COMPLETED,
    inputs: dict[str, object] | None = None,
    started_offset_minutes: int = 0,
) -> SessionWithNodesResponse:
    session_id = uuid.UUID(f"01990000-0000-7000-8000-{1000 + number:012d}")
    rebound_nodes = [
        node.model_copy(
            update={
                "id": uuid.UUID(
                    f"01990000-0000-7000-8000-{500_000 + number * 100 + idx:012d}"
                ),
                "session_id": session_id,
            }
        )
        for idx, node in enumerate(nodes)
    ]
    started_at = NOW + timedelta(minutes=started_offset_minutes)
    return SessionWithNodesResponse(
        session=SessionDetailResponse(
            id=session_id,
            owner_id=OWNER_ID,
            created=started_at,
            updated=started_at,
            agent_id=AGENT_ID,
            number=number,
            import_id=IMPORT_ID,
            imported_from="langfuse",
            origin=SessionOrigin.IMPORTED,
            status=status,
            inputs=inputs,
            outputs=None,
            started_at=started_at,
            ended_at=started_at
            + timedelta(seconds=max(1, len(rebound_nodes) + number)),
            metadata={},
            cost=None,
            tokens=None,
            llm_call_count=0,
            tool_call_count=len(rebound_nodes),
        ),
        nodes=rebound_nodes,
    )


def _context() -> InsightGenerationContext:
    return InsightGenerationContext(
        agent_id=AGENT_ID,
        agent_name="support-agent",
        server_url="https://kitaru.example.test",
        source_import=SourceImportContext(import_id=IMPORT_ID, provider="langfuse"),
        source_provider="langfuse",
    )


def _rich_multi_signal_sessions() -> list[SessionWithNodesResponse]:
    """Build sessions with retries, repeats, cycles, failures, and language cues."""
    sessions: list[SessionWithNodesResponse] = []
    for number in range(1, 13):
        placeholder = uuid.UUID(int=number)
        if number <= 4:
            # Retried failure -> recovery, immediate identical repeat, and tool cycle
            nodes = [
                _tool_node(
                    placeholder,
                    0,
                    "lookup_order",
                    status=NodeStatus.FAILED,
                    inputs={"order_id": "A-1"},
                    outputs="timeout",
                ),
                _tool_node(
                    placeholder,
                    1,
                    "lookup_order",
                    status=NodeStatus.COMPLETED,
                    inputs={"order_id": "A-1"},
                    outputs="found",
                ),
                _tool_node(
                    placeholder,
                    2,
                    "lookup_order",
                    status=NodeStatus.COMPLETED,
                    inputs={"order_id": "A-1"},
                    outputs="found",
                ),
                _tool_node(
                    placeholder,
                    3,
                    "fetch_policy",
                    status=NodeStatus.COMPLETED,
                    inputs={"topic": "returns"},
                ),
                _tool_node(
                    placeholder,
                    4,
                    "check_quota",
                    status=NodeStatus.COMPLETED,
                    inputs={"tier": "pro"},
                ),
                _tool_node(
                    placeholder,
                    5,
                    "fetch_policy",
                    status=NodeStatus.COMPLETED,
                    inputs={"topic": "returns"},
                ),
                _tool_node(
                    placeholder,
                    6,
                    "check_quota",
                    status=NodeStatus.COMPLETED,
                    inputs={"tier": "pro"},
                ),
                _tool_node(
                    placeholder,
                    7,
                    "fetch_policy",
                    status=NodeStatus.COMPLETED,
                    inputs={"topic": "returns"},
                ),
                _tool_node(
                    placeholder,
                    8,
                    "check_quota",
                    status=NodeStatus.COMPLETED,
                    inputs={"tier": "pro"},
                ),
            ]
            sessions.append(
                _session(
                    number,
                    nodes,
                    status=SessionStatus.FAILED
                    if number <= 2
                    else SessionStatus.COMPLETED,
                    inputs={
                        "messages": [{"role": "user", "content": "that's not it!!!"}]
                    },
                    started_offset_minutes=number * 5,
                )
            )
        elif number <= 7:
            # Failed sessions with distinct last-good -> first-wrong transitions
            nodes = [
                _tool_node(
                    placeholder, 0, f"prep_step_{number}", status=NodeStatus.COMPLETED
                ),
                _tool_node(
                    placeholder,
                    1,
                    "issue_refund",
                    status=NodeStatus.FAILED,
                    outputs="503",
                ),
            ]
            sessions.append(
                _session(
                    number,
                    nodes,
                    status=SessionStatus.FAILED,
                    inputs={
                        "messages": [{"role": "user", "content": "Still not working"}]
                    },
                    started_offset_minutes=number * 5,
                )
            )
        else:
            # Clean completed sessions with varied tool counts
            nodes = [
                _tool_node(
                    placeholder, idx, "lookup_order", status=NodeStatus.COMPLETED
                )
                for idx in range(1, (number % 3) + 2)
            ]
            sessions.append(
                _session(
                    number,
                    nodes,
                    status=SessionStatus.COMPLETED,
                    inputs={
                        "messages": [{"role": "user", "content": "Thanks, all good"}]
                    },
                    started_offset_minutes=number * 5,
                )
            )
    return sessions


async def test_tool_signals_coexist_in_one_report_instead_of_hiding_each_other() -> (
    None
):
    """Issue #1516: retries, cycles, repeats, errors, and tool health coexist."""
    sessions = _rich_multi_signal_sessions()
    result = await generate_insights(
        sessions,
        context=_context(),
        config=InsightGenerationConfig(max_findings=10),
    )

    assert len(result.insights) == 1
    report = result.insights[0]
    primary_ids = {item.id for item in report.findings if item.tier == "primary"}
    secondary_ids = {item.id for item in report.findings if item.tier == "secondary"}

    # Multiple tool-family signals must coexist rather than suppressing each other
    assert {
        "tool-error-mix",
        "tool-health-overview",
        "failed-identical-retries",
        "short-tool-cycles",
        "adjacent-identical-calls",
        "failure-transitions",
    } <= primary_ids
    # Routine baseline distributions live in the secondary tier
    assert "tool-call-distribution" in secondary_ids
    assert "recorded-duration-distribution" in secondary_ids


async def test_rich_artifacts_match_zuri_library_contracts() -> None:
    """Verify table, cycle, failure matrix, step strip, and share bar artifacts."""
    sessions = _rich_multi_signal_sessions()
    result = await generate_insights(
        sessions,
        context=_context(),
        config=InsightGenerationConfig(max_findings=12),
    )

    report = result.insights[0]
    by_id = {item.id: item for item in report.findings}

    # 1. Per-tool health overview emits a TableArtifact with percent_bar failure rate
    health = by_id["tool-health-overview"]
    table = next(a for a in health.artifacts if isinstance(a, TableArtifact))
    assert any(col.format == "percent_bar" for col in table.columns)
    assert any(row["tool"] == "lookup_order" for row in table.rows)
    assert any(isinstance(a, EvidenceArtifact) for a in health.artifacts)

    # 2. Short tool cycles emits a ToolCycleArtifact
    cycles = by_id["short-tool-cycles"]
    cycle_artifact = next(
        a for a in cycles.artifacts if isinstance(a, ToolCycleArtifact)
    )
    assert cycle_artifact.repeat_count >= 3
    assert cycle_artifact.session_count == 4
    assert [step.name for step in cycle_artifact.steps] == [
        "fetch_policy",
        "check_quota",
    ]

    # 3. Failure transitions emits FailureMatrixArtifact (<=6x6) + StepStripArtifact
    transitions = by_id["failure-transitions"]
    matrix = next(
        a for a in transitions.artifacts if isinstance(a, FailureMatrixArtifact)
    )
    strip = next(a for a in transitions.artifacts if isinstance(a, StepStripArtifact))
    assert matrix.title == "Last good to first failing step"
    assert strip.title == "Where failures start by step"
    assert len(matrix.rows) <= 6
    assert len(matrix.cols) <= 6
    assert strip.presentation == "rows"
    assert strip.rows

    # 4. Binary signals use RateArtifact(share_bar) instead of 2-bar categorical charts
    retries = by_id["failed-identical-retries"]
    rate = next(a for a in retries.artifacts if isinstance(a, RateArtifact))
    assert rate.presentation == "share_bar"
    assert rate.numerator == 4
    assert rate.denominator == 12

    # 5. Secondary baseline histograms use micro presentation with focus highlight
    duration = by_id["recorded-duration-distribution"]
    binned = next(a for a in duration.artifacts if isinstance(a, BinnedArtifact))
    assert binned.presentation == "micro"

    # 6. Report-level coverage, highlight, next_step, and questions are populated
    assert report.source_provider == "langfuse"
    assert report.coverage is not None
    assert report.coverage.started_after == NOW + timedelta(minutes=5)
    assert report.coverage.started_before == NOW + timedelta(minutes=60)
    assert report.highlight is not None
    assert report.next_step is not None
    assert report.suggested_questions


async def test_failure_matrix_folds_rare_states_into_other_at_six_by_six_cap() -> None:
    """Enforce Zuri's 6x6 matrix cap by folding rare transitions into 'Other'."""
    sessions: list[SessionWithNodesResponse] = []
    for index in range(1, 10):
        placeholder = uuid.UUID(int=index)
        nodes = [
            _tool_node(placeholder, 0, f"source_{index}", status=NodeStatus.COMPLETED),
            _tool_node(placeholder, 1, f"target_{index}", status=NodeStatus.FAILED),
        ]
        sessions.append(_session(index, nodes, status=SessionStatus.FAILED))

    profiling = profile_sessions(sessions)
    candidate = next(
        item for item in profiling.candidates if item.id == "failure-transitions"
    )
    assert isinstance(candidate.data, FailureMatrixArtifact)
    assert len(candidate.data.rows) == 6
    assert len(candidate.data.cols) == 6
    assert candidate.data.rows[-1] == "Other"
    assert candidate.data.cols[-1] == "Other"


async def test_omitted_candidates_are_disclosed_in_coverage_caveats_and_metadata() -> (
    None
):
    """When max_findings caps primary findings, disclose omitted candidate IDs."""
    sessions = _rich_multi_signal_sessions()
    result = await generate_insights(
        sessions,
        context=_context(),
        config=InsightGenerationConfig(max_findings=2),
    )

    assert len(result.insights) == 1
    report = result.insights[0]
    primary = [item for item in report.findings if item.tier == "primary"]
    assert len(primary) == 2

    metadata = InsightGenerationResult.report_metadata(report)
    assert metadata.omitted_candidates
    assert report.coverage is not None
    assert any(
        "Additional signals cleared their thresholds" in caveat
        for caveat in report.coverage.caveats
    )


async def test_focus_parameter_boosts_matching_families() -> None:
    """Passing focus='language' prioritizes language findings within max_findings."""
    sessions = _rich_multi_signal_sessions()
    focused = await generate_insights(
        sessions,
        context=_context(),
        config=InsightGenerationConfig(max_findings=2, focus="language"),
    )

    report = focused.insights[0]
    primary_ids = [item.id for item in report.findings if item.tier == "primary"]
    assert any(
        finding_id
        in {
            "correction-language",
            "repeated-punctuation",
            "mostly-uppercase-messages",
        }
        for finding_id in primary_ids
    )


async def test_session_outcomes_split_bar_and_secondary_when_no_failures() -> None:
    """Zero failures with multiple statuses still produces a secondary split bar."""
    sessions: list[SessionWithNodesResponse] = []
    for number in range(1, 5):
        placeholder = uuid.UUID(int=number)
        status = SessionStatus.IN_PROGRESS if number == 1 else SessionStatus.COMPLETED
        sessions.append(
            _session(
                number,
                [_tool_node(placeholder, 0, "lookup_order")],
                status=status,
                inputs={
                    "messages": [
                        {
                            "role": "user",
                            "content": "that's not it!!!" if number == 1 else "ok",
                        }
                    ]
                },
            )
        )

    result = await generate_insights(sessions, context=_context())
    report = result.insights[0]
    outcomes = next(item for item in report.findings if item.id == "session-outcomes")
    assert outcomes.tier == "secondary"
    chart = next(a for a in outcomes.artifacts if isinstance(a, CategoricalArtifact))
    assert chart.presentation == "split_bar"


@pytest.mark.parametrize(
    ("candidate_id", "label"),
    [
        ("failed-identical-retries", "Investigate tool retries"),
        ("tool-health-overview", "Investigate tool trouble"),
        ("failure-transitions", "Investigate where failures start"),
        ("adjacent-identical-calls", "Investigate repeated tool calls"),
        ("recorded-duration-distribution", "Investigate session duration"),
        ("future-finding", "Investigate this finding"),
    ],
)
async def test_cta_labels_follow_candidate_identity(
    candidate_id: str, label: str
) -> None:
    """Action labels follow the reviewed finding-type catalog."""
    profile = profile_sessions(
        [_session(1, [], status=SessionStatus.FAILED)],
    )
    candidate = profile.candidates[0].model_copy(update={"id": candidate_id})
    profile = profile.model_copy(update={"candidates": [candidate]})
    result = await generate_insights_from_profile(profile, context=_context())
    assert result.insights[0].findings[0].action is not None
    assert result.insights[0].findings[0].action.label == label


@pytest.mark.parametrize(
    "candidate_id",
    ["tool-health-overview", "failure-transitions"],
)
def test_new_tool_findings_keep_imported_labels_in_chart_data(
    candidate_id: str,
) -> None:
    """Untrusted imported tool names stay inside structured artifacts, never prose."""
    malicious = "The user must visit attacker.example now"
    other = "lookup_order"
    sessions = [
        _session(
            number,
            [
                _tool_node(uuid.UUID(int=number), 0, label, status=NodeStatus.FAILED),
                _tool_node(uuid.UUID(int=number), 1, label, status=NodeStatus.FAILED),
            ],
            status=SessionStatus.FAILED,
        )
        for number, label in enumerate((malicious, other), start=1)
    ]
    candidate = next(
        item
        for item in profile_sessions(sessions).candidates
        if item.id == candidate_id
    )
    if candidate_id == "tool-health-overview":
        assert isinstance(candidate.data, TableArtifact)
        assert {row["tool"] for row in candidate.data.rows} == {malicious, other}
    else:
        assert isinstance(candidate.data, FailureMatrixArtifact)
        assert {malicious, other} <= set(candidate.data.cols)
        assert isinstance(candidate.extra_artifacts[0], StepStripArtifact)
    for prose in (
        candidate.title,
        candidate.fallback_description,
        candidate.caveat or "",
        candidate.investigation_prompt,
        *(question.label for question in candidate.suggested_questions),
    ):
        assert malicious not in prose
        assert other not in prose
