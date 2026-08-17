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

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationListParams,
    InvestigationSessionsListParams,
    InvestigationSessionUpdateRequest,
    InvestigationSessionVerdict,
    InvestigationStatus,
    InvestigationUpdateRequest,
)
from kitaru.cli import app as app_module
from kitaru.cli import investigations
from kitaru.cli.output import CLIError
from kitaru.cli.schema import describe_schema
from kitaru.client.exceptions import APIError


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
                "questions": [
                    {
                        "key": "root-cause",
                        "question": "What caused the failure?",
                        "highlights": [],
                    }
                ],
                "verdict": None,
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
        self.base_url = "https://api.kitaru.example.com/"
        self.server_info = ServerInfoResponse(
            version="0.0.0",
            auth_scheme=AuthScheme.LOCAL,
            ui_version="1.2.3",
        )
        self.info_error: Exception | None = None
        self.agents = self._Agents(self)
        self.investigations = self._Investigations(self)
        self.info = self._Info(self)

    class _Info:
        def __init__(self, owner: "StubInvestigationClient") -> None:
            self.owner = owner

        async def get(self) -> ServerInfoResponse:
            if self.owner.info_error is not None:
                raise self.owner.info_error
            return self.owner.server_info

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


async def test_create_maps_sessions_questions_and_highlights_to_sdk() -> None:
    """Create groups keyed questions and highlights per session."""
    client = StubInvestigationClient()
    highlight_node_id = uuid.uuid4()
    highlights = json.dumps(
        [{"selector": {"node_id": str(highlight_node_id)}, "description": "Odd retry."}]
    )

    result = await investigations.create_investigation(
        client,
        "triage",
        agent="assistant",
        description="Review selected failures",
        session_ids=client.session_ids,
        session_questions=[
            f"{client.session_ids[0]}:root-cause=What caused it?",
            f"{client.session_ids[1]}:root-cause=What caused it?",
            f"{client.session_ids[1]}:retry=Was the retry appropriate?",
        ],
        session_highlights=[f"{client.session_ids[1]}:retry={highlights}"],
    )

    [request] = client.create_calls
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "agent_id": str(client.agent.id),
        "name": "triage",
        "description": "Review selected failures",
        "sessions": [
            {
                "session_id": str(client.session_ids[0]),
                "questions": [
                    {"key": "root-cause", "question": "What caused it?"},
                ],
            },
            {
                "session_id": str(client.session_ids[1]),
                "questions": [
                    {"key": "root-cause", "question": "What caused it?"},
                    {
                        "key": "retry",
                        "question": "Was the retry appropriate?",
                        "highlights": [
                            {
                                "selector": {"node_id": str(highlight_node_id)},
                                "description": "Odd retry.",
                            }
                        ],
                    },
                ],
            },
        ],
    }
    assert result.item["id"] == str(client.investigation.id)


async def _create_minimal(client: StubInvestigationClient) -> Any:
    return await investigations.create_investigation(
        client,
        "triage",
        agent="assistant",
        description=None,
        session_ids=client.session_ids,
        session_questions=[
            f"{session_id}:root-cause=What caused it?"
            for session_id in client.session_ids
        ],
        session_highlights=[],
    )


async def test_create_links_review_url_from_dashboard_url() -> None:
    """Create links the review page under a server-stated dashboard URL."""
    client = StubInvestigationClient()
    client.server_info = ServerInfoResponse(
        version="0.0.0",
        auth_scheme=AuthScheme.CONTROL_PLANE,
        dashboard_url="https://cloud.example.com/kitaru-workspaces/ws-1/",
    )

    result = await _create_minimal(client)

    assert result.links == {
        "review": (
            "https://cloud.example.com/kitaru-workspaces/ws-1"
            f"/agents/{client.agent.id}"
            f"/investigations/{client.investigation.id}/review"
        )
    }
    assert result.warnings == []


async def test_create_omits_review_link_for_api_only_server() -> None:
    """A server without a dashboard or UI yields no review link."""
    client = StubInvestigationClient()
    client.server_info = ServerInfoResponse(
        version="0.0.0", auth_scheme=AuthScheme.LOCAL
    )

    result = await _create_minimal(client)

    assert result.links == {}
    assert result.warnings == []


async def test_create_warns_when_server_info_is_unavailable() -> None:
    """A failed info request keeps the created investigation and warns."""
    client = StubInvestigationClient()
    client.info_error = APIError(503, "unavailable")

    result = await _create_minimal(client)

    assert result.item["id"] == str(client.investigation.id)
    assert result.links == {}
    assert len(result.warnings) == 1
    assert "review link" in result.warnings[0]


@pytest.mark.parametrize(
    ("sessions", "questions", "highlights", "message"),
    [
        (
            [uuid.UUID(int=1), uuid.UUID(int=1)],
            [],
            [],
            "--session value must be unique",
        ),
        (
            [],
            ["missing-separator"],
            [],
            "--session-question must be SESSION:KEY=QUESTION",
        ),
        (
            [],
            [f"{uuid.UUID(int=2)}=why?"],
            [],
            "--session-question must start with SESSION:KEY",
        ),
        (
            [],
            [f"{uuid.UUID(int=2)}:root-cause=why?"],
            [],
            "also be selected with --session",
        ),
        (
            [uuid.UUID(int=3)],
            [
                f"{uuid.UUID(int=3)}:root-cause=why?",
                f"{uuid.UUID(int=3)}:root-cause=why not?",
            ],
            [],
            "key must be unique per session",
        ),
        (
            [],
            [],
            ["missing-separator"],
            "--session-highlights must be SESSION:KEY=JSON_ARRAY",
        ),
        (
            [],
            [],
            [f"{uuid.UUID(int=2)}:root-cause=why?"],
            "also be selected with --session",
        ),
        (
            [uuid.UUID(int=4)],
            [],
            [f"{uuid.UUID(int=4)}:root-cause=not-json"],
            "--session-highlights is not valid JSON",
        ),
        (
            [uuid.UUID(int=4)],
            [],
            [f"{uuid.UUID(int=4)}:root-cause={{}}"],
            "--session-highlights must contain a JSON array",
        ),
        (
            [uuid.UUID(int=5)],
            [f"{uuid.UUID(int=5)}:root-cause=why?"],
            [f"{uuid.UUID(int=5)}:retry=[]"],
            "has no matching --session-question",
        ),
    ],
)
async def test_create_validation_precedes_agent_lookup(
    sessions: list[uuid.UUID],
    questions: list[str],
    highlights: list[str],
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
            session_ids=sessions,
            session_questions=questions,
            session_highlights=highlights,
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
        status=None,
    )
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "name": "renamed",
        "description": None,
    }

    await investigations.update_investigation(
        client,
        investigation_id,
        name=None,
        description=None,
        clear_description=False,
        status=InvestigationStatus.COMPLETED,
    )
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "status": "completed",
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
        InvestigationSessionVerdict.ACCEPTABLE,
        InvestigationSessionVerdict.PROBLEMATIC,
    ):
        await investigations.update_investigation_session_verdict(
            client,
            investigation_id,
            client.session_ids[0],
            verdict=target,
        )
        _, _, request = client.session_update_calls[-1]
        assert request.verdict is target

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
            status=None,
        )
    with pytest.raises(CLIError, match="Select at least one"):
        await investigations.update_investigation(
            client,
            client.investigation.id,
            name=None,
            description=None,
            clear_description=False,
            status=None,
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
            [
                "investigation",
                "session",
                "verdict",
                investigation_id,
                session_id,
                "acceptable",
            ],
            "session.verdict",
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
        "investigation.session.list",
        "investigation.session.verdict",
        "investigation.update",
    }
    assert specs["investigation.delete"]["side_effects"]["deletes_remote_state"]
    assert specs["investigation.session.verdict"]["mutating"] is True
