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

import json
import uuid
import weakref
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.insight import InsightInput
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionListParams,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.api_models.v1.task import AnalysisTaskDetails, PackagePluginSpec
from kitaru.insights import InsightGenerationResult
from kitaru.insights import analyzer as analyzer_module
from kitaru.insights.analyzer import analyze_post_import_sessions
from kitaru.insights.profiling import SessionProfiler
from kitaru.task import analyzer as task_analyzer
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

    async def fake_generate_insights(profiling, **kwargs):
        captured["profiling"] = profiling
        captured.update(kwargs)
        return SimpleNamespace(insights=[])

    monkeypatch.setattr(
        analyzer_module, "generate_insights_from_profile", fake_generate_insights
    )

    result = await analyze_post_import_sessions([_view(1)], model="gpt-test")

    assert result == []
    assert captured["generator"] is sentinel_generator
    assert captured["config"].model.model == "gpt-test"
    assert captured["profiling"].coverage.sessions_analyzed == 1
    assert captured["context"].source_import.import_id == IMPORT_ID


async def test_analyzer_returns_no_cards_when_no_pattern_is_eligible() -> None:
    """Preserve the pipeline's honest empty result at the plugin boundary."""
    assert await analyze_post_import_sessions([_view(2)]) == []


async def test_analyzer_forwards_enabled_observer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass the initialized observer to insight generation."""
    sentinel_observer = object()
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        analyzer_module, "LangfuseGenerationObserver", lambda: sentinel_observer
    )

    async def fake_generate_insights(profiling: Any, **kwargs: Any) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(insights=[])

    monkeypatch.setattr(
        analyzer_module, "generate_insights_from_profile", fake_generate_insights
    )

    assert await analyze_post_import_sessions([_view(1)], observe=True) == []
    assert captured["observer"] is sentinel_observer


async def test_analyzer_generates_when_observer_initialization_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Continue generation without telemetry when its setup fails."""
    captured: dict[str, Any] = {}
    initialization_attempts = 0

    def failing_observer() -> None:
        nonlocal initialization_attempts
        initialization_attempts += 1
        raise RuntimeError("telemetry is unavailable")

    monkeypatch.setattr(analyzer_module, "LangfuseGenerationObserver", failing_observer)

    async def fake_generate_insights(profiling: Any, **kwargs: Any) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(insights=[])

    monkeypatch.setattr(
        analyzer_module, "generate_insights_from_profile", fake_generate_insights
    )

    assert await analyze_post_import_sessions([_view(1)], observe=True) == []
    assert initialization_attempts == 1
    assert captured["observer"] is None


async def test_analyzer_preserves_source_session_count_in_card_coverage() -> None:
    """Distinguish the eligible source total from sessions actually analyzed."""
    insights = await analyze_post_import_sessions(
        [_view(1, failed_tool=True), _view(2)], source_session_count=5
    )

    assert insights
    for insight in insights:
        coverage = InsightGenerationResult.card_metadata(insight).coverage
        assert coverage.sessions_available == 5
        assert coverage.sessions_analyzed == 2


async def test_analyzer_returns_no_cards_for_empty_input() -> None:
    """An import without eligible sessions completes without findings."""
    assert await analyze_post_import_sessions([]) == []


async def test_analyzer_async_and_sync_iterables_match_list_results() -> None:
    views = [_view(number, failed_tool=number == 301) for number in range(1, 302)]
    expected = await analyze_post_import_sessions(views)

    async def stream() -> AsyncIterator[SessionView]:
        for view in reversed(views):
            yield view

    assert await analyze_post_import_sessions(iter(views)) == expected
    assert await analyze_post_import_sessions(stream()) == expected


async def test_empty_async_input_returns_without_model_initialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def stream() -> AsyncIterator[SessionView]:
        for _ in range(0):
            yield _view(1)

    def unexpected_model() -> None:
        raise AssertionError("empty input must not initialize a model")

    monkeypatch.setattr(
        "kitaru.insights.openai_generator.OpenAIInsightGenerator", unexpected_model
    )
    assert await analyze_post_import_sessions(stream(), model="gpt-test") == []


@pytest.mark.parametrize(
    "invalid",
    ["agent", "import", "origin", "duplicate", "node_session", "node_id", "node_index"],
)
async def test_late_invalid_session_prevents_model_initialization(
    monkeypatch: pytest.MonkeyPatch, invalid: str
) -> None:
    late = _view(302, failed_tool=True)
    if invalid == "agent":
        late.session.agent_id = uuid.uuid4()
    elif invalid == "import":
        late.session.import_id = uuid.uuid4()
    elif invalid == "origin":
        late.session.origin = SessionOrigin.RECORDED
    elif invalid == "duplicate":
        late = _view(1)
    elif invalid == "node_session":
        late.nodes[0].session_id = uuid.uuid4()
    else:
        second = late.nodes[0].model_copy(
            update={"index": 1} if invalid == "node_id" else {"id": uuid.uuid4()}
        )
        late.nodes.append(second)

    async def stream() -> AsyncIterator[SessionView]:
        for number in range(1, 302):
            yield _view(number)
        yield late

    initialized = False

    def unexpected_model() -> None:
        nonlocal initialized
        initialized = True
        raise AssertionError("invalid input must not initialize a model")

    monkeypatch.setattr(
        "kitaru.insights.openai_generator.OpenAIInsightGenerator", unexpected_model
    )
    with pytest.raises(ValueError):
        await analyze_post_import_sessions(stream(), model="gpt-test")
    assert not initialized


async def test_provider_must_match_across_the_full_stream() -> None:
    async def stream() -> AsyncIterator[SessionView]:
        yield _view(1, failed_tool=True)
        for number in range(2, 302):
            view = _view(number)
            if number == 301:
                view.session.imported_from = "another-provider"
            yield view

    cards = await analyze_post_import_sessions(stream())
    assert cards
    assert all(
        InsightGenerationResult.card_metadata(card).context.source_import.provider
        is None
        for card in cards
    )


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


async def test_task_runner_loads_analyzer_and_writes_cards(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exercise plugin loading, async invocation, and the JSON task receipt."""
    views = [_view(number, failed_tool=number == 301) for number in range(301, 0, -1)]
    for view in views:
        view.session.status = (
            SessionStatus.FAILED
            if view.session.number == 301
            else SessionStatus.COMPLETED
        )
    views[1].session.status = SessionStatus.IN_PROGRESS
    task_id = uuid.uuid4()
    details = AnalysisTaskDetails(
        analyzer_name="post-import-insights",
        params={"agent_name": "returns-agent"},
        plugin=PackagePluginSpec(
            entrypoint="kitaru.insights.analyzer:analyze_post_import_sessions",
            requirement="kitaru",
        ),
        agent_id=AGENT_ID,
        import_id=IMPORT_ID,
    )
    fetched: list[uuid.UUID] = []
    consumed: list[uuid.UUID] = []
    previous_node: weakref.ReferenceType[SessionNodeResponse] | None = None
    consume = SessionProfiler.consume

    def record_consumption(
        profiler: SessionProfiler, session: SessionWithNodesResponse
    ) -> None:
        consume(profiler, session)
        consumed.append(session.session.id)

    monkeypatch.setattr(SessionProfiler, "consume", record_consumption)

    class Tasks:
        async def get_spec(self, requested_id: uuid.UUID) -> Any:
            assert requested_id == task_id
            return SimpleNamespace(details=details)

    class Sessions:
        async def iter(
            self, params: SessionListParams
        ) -> AsyncIterator[SessionDetailResponse]:
            assert params.filter is not None
            assert params.filter.model_dump(mode="json", by_alias=True) == {
                "field": "import_id",
                "op": "eq",
                "value": str(IMPORT_ID),
            }
            for view in views:
                yield view.session

        async def get_with_nodes(
            self, requested_id: uuid.UUID
        ) -> SessionWithNodesResponse:
            nonlocal previous_node
            assert consumed == fetched
            if previous_node is not None:
                assert previous_node() is None
            fetched.append(requested_id)
            view = next(view for view in views if view.session.id == requested_id)
            nodes = [node.model_copy(deep=True) for node in view.nodes]
            previous_node = weakref.ref(nodes[0]) if nodes else None
            return SessionWithNodesResponse(session=view.session, nodes=nodes)

    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    client: Any = SimpleNamespace(tasks=Tasks(), sessions=Sessions())

    await task_analyzer.run(client, str(task_id))

    cards = [
        InsightInput.model_validate(item)
        for item in json.loads(result_path.read_text())
    ]
    assert fetched == [view.session.id for view in views]
    assert consumed == fetched
    assert cards
    metadata = [InsightGenerationResult.card_metadata(card) for card in cards]
    assert all(item.context.source_import.import_id == IMPORT_ID for item in metadata)
    assert all(item.context.agent_name == "returns-agent" for item in metadata)
    assert any(evidence.node_id for item in metadata for evidence in item.evidence)
    assert all(item.coverage.sessions_available == 301 for item in metadata)
    assert all(item.coverage.sessions_analyzed == 301 for item in metadata)
    assert all(item.coverage.nodes_available == 1 for item in metadata)
    assert all(
        all(
            truncation.dimension != "sessions"
            for truncation in item.coverage.truncations
        )
        for item in metadata
    )
    assert all(
        all("sample" not in caveat for caveat in item.coverage.caveats)
        for item in metadata
    )


async def test_task_runner_accepts_no_eligible_findings() -> None:
    """Allow an honest empty analysis through the plugin contract."""
    assert (
        await task_analyzer.call_analyzer(
            "post-import-insights", analyze_post_import_sessions, [_view(2)], {}
        )
        == []
    )
