#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Insight CLI behavior over the existing SDK resource."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.insight import (
    InsightInput,
    InsightListParams,
    InsightUpdateRequest,
)
from kitaru.cli import app as app_module
from kitaru.cli import insights
from kitaru.cli.output import CLIError
from kitaru.cli.schema import describe_schema


@dataclass
class StubModel:
    """Small response exposing the Pydantic serialization surface."""

    id: uuid.UUID
    values: dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.values[name]
        except KeyError as error:
            raise AttributeError(name) from error

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {"id": str(self.id), **self.values}


class StubInsightClient:
    """Protocol-shaped client recording insight SDK calls."""

    def __init__(self) -> None:
        self.agent = StubModel(uuid.uuid4(), {"name": "assistant"})
        self.insight = StubModel(
            uuid.uuid4(),
            {
                "agent_id": str(self.agent.id),
                "title": "Latency regressed",
                "description": "p95 latency doubled after the deploy",
                "data": {
                    "type": "text",
                    "content": "Latency doubled after the deploy.",
                },
                "metadata": {},
            },
        )
        self.agent_lookups = 0
        self.create_calls: list[tuple[uuid.UUID, list[InsightInput]]] = []
        self.create_idempotency_keys: list[str | None] = []
        self.list_calls: list[InsightListParams] = []
        self.get_calls: list[uuid.UUID] = []
        self.update_calls: list[tuple[uuid.UUID, InsightUpdateRequest]] = []
        self.deleted: list[uuid.UUID] = []
        self.agents = self._Agents(self)
        self.insights = self._Insights(self)

    class _Agents:
        def __init__(self, owner: "StubInsightClient") -> None:
            self.owner = owner

        async def get(self, agent_id: uuid.UUID) -> StubModel:
            self.owner.agent_lookups += 1
            assert agent_id == self.owner.agent.id
            return self.owner.agent

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            self.owner.agent_lookups += 1
            return SimpleNamespace(items=[self.owner.agent], next_cursor=None)

    class _Insights:
        def __init__(self, owner: "StubInsightClient") -> None:
            self.owner = owner

        async def create(
            self,
            agent_id: uuid.UUID,
            insight_inputs: list[InsightInput],
            idempotency_key: str | None = None,
        ) -> list[StubModel]:
            self.owner.create_calls.append((agent_id, insight_inputs))
            self.owner.create_idempotency_keys.append(idempotency_key)
            return [self.owner.insight]

        async def list(self, params: InsightListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(
                items=[self.owner.insight], next_cursor="next-insight"
            )

        async def get(self, insight_id: uuid.UUID) -> StubModel:
            self.owner.get_calls.append(insight_id)
            assert insight_id == self.owner.insight.id
            return self.owner.insight

        async def update(
            self, insight_id: uuid.UUID, request: InsightUpdateRequest
        ) -> StubModel:
            self.owner.update_calls.append((insight_id, request))
            return self.owner.insight

        async def delete(self, insight_id: uuid.UUID) -> None:
            self.owner.deleted.append(insight_id)


async def test_create_resolves_agent_and_parses_each_insight_json() -> None:
    """Create resolves the agent and validates each --insight JSON object."""
    client = StubInsightClient()

    result = await insights.create_insights(
        client,
        agent="assistant",
        insight=[
            json.dumps(
                {
                    "title": "Latency regressed",
                    "data": {"type": "text", "content": "It got slower."},
                }
            ),
            json.dumps(
                {
                    "title": "Error mix",
                    "data": {
                        "type": "categorical",
                        "values": [{"label": "timeout", "value": 3}],
                    },
                }
            ),
        ],
        idempotency_key="retry-insights",
    )

    [(agent_id, insight_inputs)] = client.create_calls
    assert agent_id == client.agent.id
    assert [item.model_dump(mode="json") for item in insight_inputs] == [
        {
            "title": "Latency regressed",
            "description": None,
            "data": {"type": "text", "content": "It got slower."},
            "metadata": {},
        },
        {
            "title": "Error mix",
            "description": None,
            "data": {
                "type": "categorical",
                "unit": None,
                "values": [{"label": "timeout", "value": 3}],
            },
            "metadata": {},
        },
    ]
    assert client.create_idempotency_keys == ["retry-insights"]
    assert result.items == [client.insight.model_dump(mode="json")]


async def test_create_requires_at_least_one_insight() -> None:
    """Create fails before resolving the agent when no --insight is given."""
    client = StubInsightClient()

    with pytest.raises(CLIError, match="Provide at least one --insight"):
        await insights.create_insights(client, agent="assistant", insight=[])

    assert client.agent_lookups == 0
    assert client.create_calls == []


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("not-json", "not valid JSON"),
        ("[]", "must contain a JSON object"),
    ],
)
async def test_create_rejects_malformed_insight_json(
    payload: str, message: str
) -> None:
    """Malformed --insight values fail before resolving the agent."""
    client = StubInsightClient()

    with pytest.raises(CLIError, match=message):
        await insights.create_insights(client, agent="assistant", insight=[payload])

    assert client.agent_lookups == 0
    assert client.create_calls == []


async def test_list_combines_agent_and_type_filters_with_the_generic_filter() -> None:
    """List folds --agent, --type, and --filter into one AND expression."""
    client = StubInsightClient()

    result = await insights.list_insights(
        client,
        size=10,
        cursor=None,
        sort="created:desc",
        filter=json.dumps({"field": "title", "op": "eq", "value": "Latency"}),
        agent="assistant",
        type="text",
    )

    [params] = client.list_calls
    dumped = params.model_dump(mode="json", exclude_unset=True)
    assert json.loads(dumped["filter"]) == {
        "and": [
            {"field": "title", "op": "eq", "value": "Latency"},
            {"field": "agent_id", "op": "eq", "value": str(client.agent.id)},
            {"field": "type", "op": "eq", "value": "text"},
        ]
    }
    assert client.agent_lookups == 1
    assert result.page is not None
    assert result.page["next_cursor"] == "next-insight"


async def test_list_without_shortcuts_uses_the_generic_filter_only() -> None:
    """List without --agent or --type leaves the generic filter untouched."""
    client = StubInsightClient()

    await insights.list_insights(
        client,
        size=20,
        cursor=None,
        sort="created:desc",
        filter=None,
        agent=None,
        type=None,
    )

    [params] = client.list_calls
    assert params.filter is None
    assert client.agent_lookups == 0


async def test_crud_commands_map_to_sdk() -> None:
    """Reads, sparse updates, and deletion use the corresponding SDK calls."""
    client = StubInsightClient()
    insight_id = client.insight.id

    fetched = await insights.get_insight(client, insight_id)
    assert fetched.item["id"] == str(insight_id)

    await insights.update_insight(
        client,
        insight_id,
        title="renamed",
        description=None,
        clear_description=True,
    )
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "title": "renamed",
        "description": None,
    }

    with pytest.raises(CLIError, match="requires --force"):
        await insights.delete_insight(client, insight_id, force=False)
    assert client.deleted == []
    deleted = await insights.delete_insight(client, insight_id, force=True)
    assert deleted.item == {"id": str(insight_id), "deleted": True}


async def test_sparse_update_rejects_conflicts_and_empty_changes() -> None:
    """Sparse update validation happens before an SDK request."""
    client = StubInsightClient()

    with pytest.raises(CLIError, match="cannot be used together"):
        await insights.update_insight(
            client,
            client.insight.id,
            title=None,
            description="set",
            clear_description=True,
        )
    with pytest.raises(CLIError, match="Select at least one"):
        await insights.update_insight(
            client,
            client.insight.id,
            title=None,
            description=None,
            clear_description=False,
        )
    assert client.update_calls == []


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubInsightClient:
    """Route public CLI invocations through one recording client."""
    client = StubInsightClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_argv_and_schema_cover_insight_lifecycle(
    argv_client: StubInsightClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every public insight leaf is registered with structured output."""
    client = argv_client
    insight_id = str(client.insight.id)
    payload = json.dumps(
        {"title": "Latency regressed", "data": {"type": "text", "content": "c"}}
    )

    commands = [
        (
            ["insight", "create", "--agent", "assistant", "--insight", payload],
            "create",
        ),
        (["insight", "list"], "list"),
        (["insight", "get", insight_id], "get"),
        (["insight", "update", insight_id, "--title", "renamed"], "update"),
        (["insight", "delete", insight_id, "--force"], "delete"),
    ]
    for argv, command in commands:
        assert app_module.main(argv) == 0
        response = json.loads(capsys.readouterr().out)
        assert response["command"] == f"insight.{command}"

    specs = {item["command"]: item for item in describe_schema(("insight",))}
    assert set(specs) == {
        "insight.create",
        "insight.delete",
        "insight.get",
        "insight.list",
        "insight.update",
    }
    assert specs["insight.delete"]["side_effects"]["deletes_remote_state"]
    assert specs["insight.create"]["side_effects"]["creates_remote_state"]
