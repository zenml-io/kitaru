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
"""Tests for the post-import analyzer entrypoint."""

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeStatus, NodeType, SessionNodeResponse
from kitaru.insights import InsightGenerationResult
from kitaru.insights import analyzer as analyzer_module
from kitaru.insights.analyzer import analyze_post_import_sessions
from kitaru.task.analyzer import SessionView

NOW = datetime(2026, 9, 4, tzinfo=UTC)
OWNER_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000003")


def _view(
    number: int,
    *,
    agent_id: uuid.UUID = AGENT_ID,
    failed_tool: bool = False,
) -> SessionView:
    session_id = uuid.UUID(f"01990000-0000-7000-8000-{100 + number:012d}")
    return SessionView(
        session=SessionDetailResponse(
            id=session_id,
            owner_id=OWNER_ID,
            created=NOW,
            updated=NOW,
            agent_id=agent_id,
            number=number,
            import_id=IMPORT_ID,
            imported_from="langfuse",
            origin=SessionOrigin.IMPORTED,
            status=(SessionStatus.FAILED if number == 1 else SessionStatus.COMPLETED),
            inputs={"message": "THAT IS WRONG!!!" if number == 1 else "thank you"},
            outputs=None,
            metadata={},
            cost=None,
            tokens=None,
            llm_call_count=0,
            tool_call_count=0,
        ),
        nodes=(
            [
                SessionNodeResponse(
                    id=uuid.UUID(f"01990000-0000-7000-8000-{200 + number:012d}"),
                    session_id=session_id,
                    index=0,
                    parent_index=None,
                    secondary_parent_indexes=[],
                    secondary_parent_ids=[],
                    node_type=NodeType.TOOL_CALL,
                    name="lookup_order",
                    status=NodeStatus.FAILED,
                    inputs={"order_id": "123"},
                    outputs=None,
                    tool_name="lookup_order",
                    metadata={},
                )
            ]
            if failed_tool
            else []
        ),
    )


async def test_analyzer_returns_self_contained_insight_inputs() -> None:
    """Return cards that retain evidence and copy-prompt context after persistence."""
    views = [_view(1, failed_tool=True), _view(2)]

    insights = await analyze_post_import_sessions(views, agent_name="returns-agent")

    assert insights
    metadata = [InsightGenerationResult.card_metadata(item) for item in insights]
    assert sum(item.recommended for item in metadata) == 1
    for item in metadata:
        assert item.context.agent_id == AGENT_ID
        assert item.context.agent_name == "returns-agent"
        assert item.context.source_import.import_id == IMPORT_ID
        assert item.context.source_import.provider == "langfuse"
        assert item.contributing_session_ids
        assert item.coverage.sessions_analyzed == 2
        assert str(IMPORT_ID) in item.investigation_prompt
        assert all(
            str(session_id) in item.investigation_prompt
            for session_id in item.contributing_session_ids
        )
    assert any(
        evidence.node_id is not None for item in metadata for evidence in item.evidence
    )


async def test_analyzer_builds_the_optional_model_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forward the selected model and provider-neutral generator to the pipeline."""
    sentinel_generator = object()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "kitaru.insights.openai_generator.OpenAIInsightGenerator",
        lambda: sentinel_generator,
    )

    async def fake_generate_insights(sessions, **kwargs):
        captured["sessions"] = sessions
        captured.update(kwargs)
        return SimpleNamespace(insights=[])

    monkeypatch.setattr(analyzer_module, "generate_insights", fake_generate_insights)

    result = await analyze_post_import_sessions([_view(1)], model="gpt-test")

    assert result == []
    assert captured["generator"] is sentinel_generator
    assert captured["config"].model.model == "gpt-test"
    assert captured["sessions"][0].session.import_id == IMPORT_ID


async def test_analyzer_returns_no_cards_when_no_pattern_is_eligible() -> None:
    """Preserve the pipeline's honest empty result at the plugin boundary."""
    assert await analyze_post_import_sessions([_view(2)]) == []


async def test_analyzer_rejects_empty_input() -> None:
    """Reject input that cannot supply agent or import identity."""
    with pytest.raises(ValueError, match="at least one session"):
        await analyze_post_import_sessions([])


@pytest.mark.parametrize("provider", ["", "p" * 256, "broken-\ud800-provider"])
async def test_analyzer_omits_invalid_optional_provider(provider: str) -> None:
    """Do not fail insight generation because an optional source label is invalid."""
    view = _view(1)
    view.session = view.session.model_copy(update={"imported_from": provider})

    insights = await analyze_post_import_sessions([view])

    assert insights
    metadata = InsightGenerationResult.card_metadata(insights[0])
    assert metadata.context.source_import.provider is None


async def test_analyzer_rejects_sessions_from_multiple_agents() -> None:
    """Reject an analyzer task whose sessions do not have one agent identity."""
    with pytest.raises(ValueError, match="context agent"):
        await analyze_post_import_sessions([_view(1), _view(2, agent_id=uuid.uuid4())])


async def test_analyzer_requires_an_import_identity() -> None:
    """Reject imported sessions that cannot be tied back to an import."""
    view = _view(1)
    view.session = view.session.model_copy(update={"import_id": None})

    with pytest.raises(ValueError, match="import ID"):
        await analyze_post_import_sessions([view])
