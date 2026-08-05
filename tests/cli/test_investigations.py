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
"""Investigation CLI behavior over the existing SDK resource."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationListParams,
    InvestigationSessionsListParams,
    InvestigationSessionStatus,
    InvestigationSessionUpdateRequest,
    InvestigationUpdateRequest,
)
from kitaru.cli import app as app_module
from kitaru.cli import investigations
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


class StubInvestigationClient:
    """Protocol-shaped client recording investigation SDK calls."""

    def __init__(self) -> None:
        self.agent = StubModel(uuid.uuid4(), {"name": "assistant"})
        self.session_ids = [uuid.uuid4(), uuid.uuid4()]
        self.investigation = StubModel(
            uuid.uuid4(),
            {
                "agent_id": str(self.agent.id),
                "name": "failure-review",
                "description": "Review failures",
                "status": "pending",
                "questions": [{"key": "cause", "question": "What caused the failure?"}],
                "total_sessions": 2,
                "completed_sessions": 0,
            },
        )
        self.investigation_session = StubModel(
            uuid.uuid4(),
            {
                "investigation_id": str(self.investigation.id),
                "session_id": str(self.session_ids[0]),
                "position": 0,
                "status": "pending",
                "view": None,
            },
        )
        self.agent_lookups = 0
        self.create_calls: list[InvestigationCreateRequest] = []
        self.list_calls: list[InvestigationListParams] = []
        self.get_calls: list[uuid.UUID] = []
        self.update_calls: list[tuple[uuid.UUID, InvestigationUpdateRequest]] = []
        self.deleted: list[uuid.UUID] = []
        self.session_list_calls: list[
            tuple[uuid.UUID, InvestigationSessionsListParams]
        ] = []
        self.session_update_calls: list[
            tuple[uuid.UUID, uuid.UUID, InvestigationSessionUpdateRequest]
        ] = []
        self.agents = self._Agents(self)
        self.investigations = self._Investigations(self)

    class _Agents:
        def __init__(self, owner: "StubInvestigationClient") -> None:
            self.owner = owner

        async def iter(self):
            self.owner.agent_lookups += 1
            yield self.owner.agent

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            self.owner.agent_lookups += 1
            return SimpleNamespace(items=[self.owner.agent], next_cursor=None)

        async def get(self, agent_id: uuid.UUID) -> StubModel:
            self.owner.agent_lookups += 1
            assert agent_id == self.owner.agent.id
            return self.owner.agent

    class _Investigations:
        def __init__(self, owner: "StubInvestigationClient") -> None:
            self.owner = owner

        async def create(self, request: InvestigationCreateRequest) -> StubModel:
            self.owner.create_calls.append(request)
            return self.owner.investigation

        async def list(self, params: InvestigationListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(
                items=[self.owner.investigation], next_cursor="next-investigation"
            )

        async def get(self, investigation_id: uuid.UUID) -> StubModel:
            self.owner.get_calls.append(investigation_id)
            assert investigation_id == self.owner.investigation.id
            return self.owner.investigation

        async def update(
            self, investigation_id: uuid.UUID, request: InvestigationUpdateRequest
        ) -> StubModel:
            self.owner.update_calls.append((investigation_id, request))
            return self.owner.investigation

        async def delete(self, investigation_id: uuid.UUID) -> None:
            self.owner.deleted.append(investigation_id)

        async def list_sessions(
            self,
            investigation_id: uuid.UUID,
            params: InvestigationSessionsListParams,
        ) -> Any:
            self.owner.session_list_calls.append((investigation_id, params))
            return SimpleNamespace(
                items=[self.owner.investigation_session], next_cursor="next-session"
            )

        async def update_session(
            self,
            investigation_id: uuid.UUID,
            session_id: uuid.UUID,
            request: InvestigationSessionUpdateRequest,
        ) -> StubModel:
            self.owner.session_update_calls.append(
                (investigation_id, session_id, request)
            )
            return self.owner.investigation_session


async def test_create_maps_questions_sessions_and_views_to_sdk() -> None:
    """Create preserves question/session order and validates curated views."""
    client = StubInvestigationClient()
    node_id = uuid.uuid4()
    view = json.dumps(
        {
            "summary": "The failed tool call",
            "items": [
                {
                    "label": "Failure",
                    "description": "The tool returned an error.",
                    "selectors": [{"node_id": str(node_id), "part": "error"}],
                }
            ],
        }
    )

    result = await investigations.create_investigation(
        client,
        "triage",
        agent="assistant",
        description="Review selected failures",
        questions=["cause=What caused it?", "fix=How should it be fixed?"],
        session_ids=client.session_ids,
        session_views=[f"{client.session_ids[1]}={view}"],
    )

    [request] = client.create_calls
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "agent_id": str(client.agent.id),
        "name": "triage",
        "description": "Review selected failures",
        "questions": [
            {"key": "cause", "question": "What caused it?"},
            {"key": "fix", "question": "How should it be fixed?"},
        ],
        "sessions": [
            {"session_id": str(client.session_ids[0])},
            {
                "session_id": str(client.session_ids[1]),
                "view": {
                    "summary": "The failed tool call",
                    "items": [
                        {
                            "label": "Failure",
                            "description": "The tool returned an error.",
                            "selectors": [
                                {
                                    "node_id": str(node_id),
                                    "part": "error",
                                }
                            ],
                        }
                    ],
                },
            },
        ],
    }
    assert result.item["id"] == str(client.investigation.id)


@pytest.mark.parametrize(
    ("questions", "sessions", "views", "message"),
    [
        (["missing-separator"], [], [], "KEY=QUESTION"),
        (["cause=one", "cause=two"], [], [], "question key must be unique"),
        (
            [],
            [uuid.UUID(int=1), uuid.UUID(int=1)],
            [],
            "--session value must be unique",
        ),
        ([], [], [f"{uuid.UUID(int=2)}={{}}"], "also be selected with --session"),
    ],
)
async def test_create_validation_precedes_agent_lookup(
    questions: list[str],
    sessions: list[uuid.UUID],
    views: list[str],
    message: str,
) -> None:
    """Malformed local inputs fail before resolving remote state."""
    client = StubInvestigationClient()

    with pytest.raises(CLIError, match=message):
        await investigations.create_investigation(
            client,
            "triage",
            agent="assistant",
            description=None,
            questions=questions,
            session_ids=sessions,
            session_views=views,
        )

    assert client.agent_lookups == 0
    assert client.create_calls == []


async def test_crud_and_session_status_commands_map_to_sdk() -> None:
    """Reads, sparse updates, deletion, and terminal session states use SDK calls."""
    client = StubInvestigationClient()
    investigation_id = client.investigation.id

    listed = await investigations.list_investigations(
        client, size=5, cursor="page", sort="created:asc", filter=None
    )
    assert client.list_calls[-1].model_dump(mode="json", exclude_unset=True) == {
        "cursor": "page",
        "size": 5,
        "sort": "created:asc",
        "filter": None,
    }
    assert listed.page is not None
    assert listed.page["next_cursor"] == "next-investigation"

    fetched = await investigations.get_investigation(client, investigation_id)
    assert fetched.item["id"] == str(investigation_id)

    await investigations.update_investigation(
        client,
        investigation_id,
        name="renamed",
        description=None,
        clear_description=True,
    )
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "name": "renamed",
        "description": None,
    }

    sessions = await investigations.list_investigation_sessions(
        client, investigation_id, size=10, cursor="sessions"
    )
    _, params = client.session_list_calls[-1]
    assert params.model_dump(mode="json", exclude_unset=True) == {
        "cursor": "sessions",
        "size": 10,
    }
    assert sessions.page is not None
    assert sessions.page["next_cursor"] == "next-session"

    for target in (
        InvestigationSessionStatus.COMPLETED,
        InvestigationSessionStatus.SKIPPED,
    ):
        await investigations.update_investigation_session_status(
            client,
            investigation_id,
            client.session_ids[0],
            status=target,
        )
        _, _, request = client.session_update_calls[-1]
        assert request.status is target

    with pytest.raises(CLIError, match="requires --force"):
        await investigations.delete_investigation(client, investigation_id, force=False)
    assert client.deleted == []
    deleted = await investigations.delete_investigation(
        client, investigation_id, force=True
    )
    assert deleted.item == {"id": str(investigation_id), "deleted": True}


async def test_sparse_update_rejects_conflicts_and_empty_changes() -> None:
    """Sparse update validation happens before an SDK request."""
    client = StubInvestigationClient()

    with pytest.raises(CLIError, match="cannot be used together"):
        await investigations.update_investigation(
            client,
            client.investigation.id,
            name=None,
            description="set",
            clear_description=True,
        )
    with pytest.raises(CLIError, match="Select at least one"):
        await investigations.update_investigation(
            client,
            client.investigation.id,
            name=None,
            description=None,
            clear_description=False,
        )
    assert client.update_calls == []


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubInvestigationClient:
    """Route public CLI invocations through one recording client."""
    client = StubInvestigationClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_argv_and_schema_cover_investigation_lifecycle(
    argv_client: StubInvestigationClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every public investigation leaf is registered with structured output."""
    client = argv_client
    investigation_id = str(client.investigation.id)
    session_id = str(client.session_ids[0])

    commands = [
        (["investigation", "create", "triage", "--agent", "assistant"], "create"),
        (["investigation", "list"], "list"),
        (["investigation", "get", investigation_id], "get"),
        (
            ["investigation", "update", investigation_id, "--name", "renamed"],
            "update",
        ),
        (
            ["investigation", "session", "list", investigation_id],
            "session.list",
        ),
        (
            ["investigation", "session", "complete", investigation_id, session_id],
            "session.complete",
        ),
        (
            ["investigation", "session", "skip", investigation_id, session_id],
            "session.skip",
        ),
        (["investigation", "delete", investigation_id, "--force"], "delete"),
    ]
    for argv, command in commands:
        assert app_module.main(argv) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["command"] == f"investigation.{command}"

    specs = {item["command"]: item for item in describe_schema(("investigation",))}
    assert set(specs) == {
        "investigation.create",
        "investigation.delete",
        "investigation.get",
        "investigation.list",
        "investigation.session.complete",
        "investigation.session.list",
        "investigation.session.skip",
        "investigation.update",
    }
    assert specs["investigation.delete"]["side_effects"]["deletes_remote_state"]
    assert specs["investigation.session.complete"]["mutating"] is True
