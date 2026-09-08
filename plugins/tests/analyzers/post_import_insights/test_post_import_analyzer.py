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
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
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
from kitaru.client.exceptions import AuthenticationError, NotFoundError
from kitaru.task import analyzer as task_analyzer
from kitaru_post_import_insights import InsightGenerationResult
from kitaru_post_import_insights import analyzer as analyzer_module
from kitaru_post_import_insights.analyzer import (
    analyze_openai_post_import_sessions,
    analyze_post_import_sessions,
)
from kitaru_post_import_insights.generation import (
    AnalystPlan,
    AnalystProjection,
    EditorialCardCopy,
    EditorialPlan,
)
from kitaru_post_import_insights.openai_generator import MissingOpenAICredential
from kitaru_post_import_insights.profiling import SessionProfiler

NOW = datetime(2026, 9, 4, tzinfo=UTC)
OWNER_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000003")


def _view(
    number: int,
    *,
    agent_id: uuid.UUID = AGENT_ID,
    failed_tool: bool = False,
) -> SessionWithNodesResponse:
    session_id = uuid.UUID(f"01990000-0000-7000-8000-{100 + number:012d}")
    return SessionWithNodesResponse(
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


class StubClient:
    def __init__(self) -> None:
        self.sessions = self
        self.responses: dict[uuid.UUID, SessionWithNodesResponse] = {}
        self.fetched: list[uuid.UUID] = []
        self.entered = False
        self.closed = False

    def add(self, responses: list[SessionWithNodesResponse]) -> list[uuid.UUID]:
        self.responses.update({response.session.id: response for response in responses})
        return [response.session.id for response in responses]

    async def __aenter__(self) -> "StubClient":
        self.entered = True
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.closed = True

    async def get_with_nodes(self, session_id: uuid.UUID) -> SessionWithNodesResponse:
        self.fetched.append(session_id)
        return self.responses[session_id]


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> StubClient:
    client = StubClient()
    monkeypatch.setattr(analyzer_module, "KitaruAPIClient", lambda: client)
    return client


async def test_analyzer_returns_self_contained_insight_inputs(
    client: StubClient,
) -> None:
    """Return cards that retain evidence and copy-prompt context after persistence."""
    views = [_view(1, failed_tool=True), _view(2)]

    insights = await analyze_post_import_sessions(
        client.add(views), agent_name="returns-agent"
    )

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
    assert client.fetched == [view.session.id for view in views]
    assert client.closed


@pytest.mark.parametrize("last_status", [200, 401, 404])
async def test_analyzer_fetches_full_sessions_using_task_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    last_status: int,
) -> None:
    """Use the real SDK with task credentials and reject incomplete API reads."""
    monkeypatch.setenv("KITARU_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("KITARU_API_URL", "https://task-api.example.test")
    monkeypatch.setenv("KITARU_API_TOKEN", "task-scoped-token")
    monkeypatch.setenv("KITARU_API_KEY", "stale-ambient-key")
    views = [_view(1, failed_tool=True), _view(2)]
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        view = views[len(requests) - 1]
        assert request.method == "GET"
        assert str(request.url) == (
            f"https://task-api.example.test/api/v1/sessions/{view.session.id}/full"
        )
        assert request.headers["Authorization"] == "Bearer task-scoped-token"
        if len(requests) == len(views) and last_status != 200:
            return httpx.Response(last_status, json={"detail": "trace unavailable"})
        return httpx.Response(200, json=view.model_dump(mode="json"))

    def build_mock_http_client(
        base_url: str, headers: dict[str, str], **kwargs: Any
    ) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            transport=httpx.MockTransport(respond),
        )

    monkeypatch.setattr(
        "kitaru.client.api_client.build_async_client", build_mock_http_client
    )
    session_ids = [view.session.id for view in views]
    if last_status != 200:
        error = AuthenticationError if last_status == 401 else NotFoundError
        with pytest.raises(error, match="trace unavailable"):
            await analyze_post_import_sessions(session_ids)
    else:
        insights = await analyze_post_import_sessions(session_ids)
        assert insights
        metadata = [InsightGenerationResult.card_metadata(item) for item in insights]
        assert all(item.coverage.sessions_analyzed == 2 for item in metadata)
        assert all(item.context.agent_id == AGENT_ID for item in metadata)
        assert all(
            item.context.source_import.import_id == IMPORT_ID for item in metadata
        )
        assert any(
            evidence.node_id == views[0].nodes[0].id
            and evidence.session_id == views[0].session.id
            for item in metadata
            for evidence in item.evidence
        )
    assert len(requests) == 2


async def test_openai_analyzer_uses_the_selected_model(
    client: StubClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forward the selected model and provider-neutral generator to the pipeline."""
    sentinel_generator = object()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "kitaru_post_import_insights.openai_generator.OpenAIInsightGenerator",
        lambda: sentinel_generator,
    )

    async def fake_generate_insights(profiling, **kwargs):
        captured["profiling"] = profiling
        captured.update(kwargs)
        return SimpleNamespace(insights=[])

    monkeypatch.setattr(
        analyzer_module, "generate_insights_from_profile", fake_generate_insights
    )

    result = await analyze_openai_post_import_sessions(
        client.add([_view(1)]), model="gpt-test"
    )

    assert result == []
    assert captured["generator"] is sentinel_generator
    assert captured["config"].model.model == "gpt-test"
    assert captured["profiling"].coverage.sessions_analyzed == 1
    assert captured["context"].source_import.import_id == IMPORT_ID


async def test_deterministic_analyzer_does_not_initialize_openai(
    client: StubClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generate deterministic cards even when OpenAI credentials are present."""
    monkeypatch.setenv("OPENAI_API_KEY", "unused-key")

    def unexpected_model() -> None:
        raise AssertionError("the deterministic analyzer must not initialize OpenAI")

    monkeypatch.setattr(
        "kitaru_post_import_insights.openai_generator.OpenAIInsightGenerator",
        unexpected_model,
    )
    assert await analyze_post_import_sessions(client.add([_view(1)]))


async def test_both_analyzers_generate_independent_cards_for_the_same_import(
    client: StubClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep numeric evidence stable while OpenAI selects and edits its own cards."""
    requests: list[dict[str, Any]] = []

    async def parse(**kwargs: Any) -> SimpleNamespace:
        requests.append(kwargs)
        if kwargs["text_format"] is AnalystPlan:
            projection = AnalystProjection.model_validate_json(kwargs["input"])
            selected = projection.candidates[0].id
            value = AnalystPlan(
                selected_candidate_ids=[selected],
                recommended_candidate_id=selected,
                rationale="Specific and actionable.",
            )
        else:
            selected = json.loads(kwargs["input"])["candidates"][0]["id"]
            value = EditorialPlan(
                intro_eyebrow="Worth looking at first",
                intro_title="A pattern deserves attention",
                intro_description="Start with a focused investigation.",
                recommendation_title="Recommended next step",
                recommendation_description="Compare a focused cohort.",
                insights=[
                    EditorialCardCopy(
                        id=selected,
                        eyebrow="Agent behavior",
                        description="This pattern is worth a closer look.",
                    )
                ],
            )
        return SimpleNamespace(
            id="response", model="gpt-test", usage=None, output_parsed=value
        )

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        "openai.AsyncOpenAI",
        lambda **kwargs: SimpleNamespace(responses=SimpleNamespace(parse=parse)),
    )
    session_ids = client.add([_view(1, failed_tool=True), _view(2)])
    deterministic = await analyze_post_import_sessions(session_ids)
    assert not requests
    openai = await analyze_openai_post_import_sessions(session_ids, model="gpt-test")

    assert len(requests) == 2
    assert all(request["model"] == "gpt-test" for request in requests)
    assert len(openai) == 1
    original = next(card for card in deterministic if card.name == openai[0].name)
    assert original is not openai[0]
    assert original.data == openai[0].data
    assert openai[0].description == "This pattern is worth a closer look."
    assert original.description != openai[0].description
    assert InsightGenerationResult.card_metadata(original).evidence == (
        InsightGenerationResult.card_metadata(openai[0]).evidence
    )


async def test_deterministic_analyzer_rejects_model_parameter(
    client: StubClient,
) -> None:
    """Selecting the deterministic plugin cannot enable provider calls."""
    with pytest.raises(task_analyzer.AnalysisError, match="model"):
        await task_analyzer.call_analyzer(
            "post-import-insights",
            analyze_post_import_sessions,
            client.add([_view(1)]),
            {"model": "gpt-test"},
        )


async def test_openai_analyzer_requires_explicit_model(client: StubClient) -> None:
    """Reject an OpenAI task whose model was not selected."""
    with pytest.raises(task_analyzer.AnalysisError, match="model"):
        await task_analyzer.call_analyzer(
            "openai-post-import-insights",
            analyze_openai_post_import_sessions,
            client.add([_view(1)]),
            {},
        )


@pytest.mark.parametrize("credential", [None, "", "   "])
@pytest.mark.parametrize("session_number", [1, 2])
async def test_openai_analyzer_requires_credentials_without_fallback(
    client: StubClient,
    monkeypatch: pytest.MonkeyPatch,
    credential: str | None,
    session_number: int,
) -> None:
    """Missing credentials fail even when profiling finds no eligible pattern."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    if credential is not None:
        monkeypatch.setenv("OPENAI_API_KEY", credential)
    with pytest.raises(MissingOpenAICredential):
        await analyze_openai_post_import_sessions(
            client.add([_view(session_number)]), model="gpt-test"
        )


async def test_analyzer_returns_no_cards_when_no_pattern_is_eligible(
    client: StubClient,
) -> None:
    """Preserve the pipeline's honest empty result at the plugin boundary."""
    assert await analyze_post_import_sessions(client.add([_view(2)])) == []


async def test_analyzer_rejects_caller_controlled_source_session_count(
    client: StubClient,
) -> None:
    """Do not let task parameters falsify coverage for a complete import scan."""
    with pytest.raises(task_analyzer.AnalysisError, match="source_session_count"):
        await task_analyzer.call_analyzer(
            "post-import-insights",
            analyze_post_import_sessions,
            client.add([_view(1, failed_tool=True), _view(2)]),
            {"source_session_count": 5},
        )


async def test_analyzer_returns_no_cards_for_empty_input(
    client: StubClient,
) -> None:
    """An import without eligible sessions completes without findings."""
    assert await analyze_post_import_sessions([]) == []
    assert not client.entered


async def test_analyzer_scans_all_ids_regardless_of_order(
    client: StubClient,
) -> None:
    views = [_view(number, failed_tool=number == 301) for number in range(1, 302)]
    expected = await analyze_post_import_sessions(client.add(views))

    assert (
        await analyze_post_import_sessions(client.add(list(reversed(views))))
        == expected
    )


async def test_empty_ids_return_without_client_or_model_initialization(
    client: StubClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_model() -> None:
        raise AssertionError("empty input must not initialize a model")

    monkeypatch.setattr(
        "kitaru_post_import_insights.openai_generator.OpenAIInsightGenerator",
        unexpected_model,
    )
    monkeypatch.setattr(analyzer_module, "KitaruAPIClient", unexpected_model)
    assert await analyze_openai_post_import_sessions([], model="gpt-test") == []


@pytest.mark.parametrize(
    "invalid",
    ["agent", "import", "origin", "duplicate", "node_session", "node_id", "node_index"],
)
async def test_late_invalid_session_prevents_model_initialization(
    client: StubClient, monkeypatch: pytest.MonkeyPatch, invalid: str
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

    ids = client.add([_view(number) for number in range(1, 302)] + [late])

    initialized = False

    def unexpected_model() -> None:
        nonlocal initialized
        initialized = True
        raise AssertionError("invalid input must not initialize a model")

    monkeypatch.setattr(
        "kitaru_post_import_insights.openai_generator.OpenAIInsightGenerator",
        unexpected_model,
    )
    with pytest.raises(ValueError):
        await analyze_openai_post_import_sessions(ids, model="gpt-test")
    assert not initialized
    assert client.closed


async def test_fetch_failure_closes_client_before_model_generation(
    client: StubClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail an incomplete import scan and release the API connection."""
    ids = client.add([_view(1)])
    missing_id = uuid.uuid4()

    def unexpected_model() -> None:
        raise AssertionError("an incomplete scan must not initialize a model")

    monkeypatch.setattr(
        "kitaru_post_import_insights.openai_generator.OpenAIInsightGenerator",
        unexpected_model,
    )
    with pytest.raises(KeyError):
        await analyze_openai_post_import_sessions([*ids, missing_id], model="gpt-test")

    assert client.fetched == [*ids, missing_id]
    assert client.closed


async def test_provider_must_match_across_the_full_stream(
    client: StubClient,
) -> None:
    views = [_view(number, failed_tool=number == 1) for number in range(1, 302)]
    views[-1].session.imported_from = "another-provider"

    cards = await analyze_post_import_sessions(client.add(views))
    assert cards
    assert all(
        InsightGenerationResult.card_metadata(card).context.source_import.provider
        is None
        for card in cards
    )


@pytest.mark.parametrize("provider", ["", "p" * 256, "broken-\ud800-provider"])
async def test_analyzer_omits_invalid_optional_provider(
    client: StubClient, provider: str
) -> None:
    """Do not fail insight generation because an optional source label is invalid."""
    view = _view(1)
    view.session = view.session.model_copy(update={"imported_from": provider})

    insights = await analyze_post_import_sessions(client.add([view]))

    assert insights
    metadata = InsightGenerationResult.card_metadata(insights[0])
    assert metadata.context.source_import.provider is None


async def test_analyzer_rejects_sessions_from_multiple_agents(
    client: StubClient,
) -> None:
    """Reject an analyzer task whose sessions do not have one agent identity."""
    with pytest.raises(ValueError, match="context agent"):
        await analyze_post_import_sessions(
            client.add([_view(1), _view(2, agent_id=uuid.uuid4())])
        )


async def test_analyzer_requires_an_import_identity(
    client: StubClient,
) -> None:
    """Reject imported sessions that cannot be tied back to an import."""
    view = _view(1)
    view.session = view.session.model_copy(update={"import_id": None})

    with pytest.raises(ValueError, match="import ID"):
        await analyze_post_import_sessions(client.add([view]))


async def test_task_runner_loads_analyzer_and_writes_cards(
    client: StubClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
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
            entrypoint="kitaru_post_import_insights.analyzer:analyze_post_import_sessions",
            requirement="kitaru-post-import-insights==0.1.0",
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
    task_client: Any = SimpleNamespace(tasks=Tasks(), sessions=Sessions())

    @asynccontextmanager
    async def plugin_client():
        yield task_client

    monkeypatch.setattr(analyzer_module, "KitaruAPIClient", plugin_client)
    await task_analyzer.run(task_client, str(task_id))

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


async def test_task_runner_accepts_no_eligible_findings(
    client: StubClient,
) -> None:
    """Allow an honest empty analysis through the plugin contract."""
    assert (
        await task_analyzer.call_analyzer(
            "post-import-insights",
            analyze_post_import_sessions,
            client.add([_view(2)]),
            {},
        )
        == []
    )
