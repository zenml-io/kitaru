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
"""Cohort and immutable cohort-version CLI behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.base import ListParams
from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.cohort_version import (
    CohortVersionCreateRequest,
    CohortVersionUpdateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition
from kitaru.api_models.v1.session import SessionListParams
from kitaru.cli import app as app_module
from kitaru.cli import cohorts
from kitaru.cli.output import CLIError


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


class StubCohortClient:
    """Protocol-shaped client recording cohort SDK calls."""

    def __init__(self) -> None:
        self.agent = StubModel(uuid.uuid4(), {"name": "assistant"})
        self.cohort = StubModel(
            uuid.uuid4(),
            {
                "name": "regression",
                "description": "original",
                "agent_id": str(self.agent.id),
                "metadata": {"team": "eval"},
                "latest_version": 2,
            },
        )
        self.version_one = StubModel(
            uuid.uuid4(),
            {
                "cohort_id": self.cohort.id,
                "version": 1,
                "display_version": "baseline",
                "session_count": 1,
            },
        )
        self.version_two = StubModel(
            uuid.uuid4(),
            {
                "cohort_id": self.cohort.id,
                "version": 2,
                "display_version": None,
                "session_count": 2,
            },
        )
        self.agent_lookups = 0
        self.cohort_lookups = 0
        self.created_cohorts: list[CohortCreateRequest] = []
        self.list_calls: list[CohortListParams] = []
        self.update_calls: list[tuple[uuid.UUID, CohortUpdateRequest]] = []
        self.deleted_cohorts: list[uuid.UUID] = []
        self.created_versions: list[tuple[uuid.UUID, CohortVersionCreateRequest]] = []
        self.version_list_calls: list[tuple[uuid.UUID, ListParams]] = []
        self.version_get_calls: list[uuid.UUID] = []
        self.version_update_calls: list[
            tuple[uuid.UUID, CohortVersionUpdateRequest]
        ] = []
        self.deleted_versions: list[uuid.UUID] = []
        self.selected_sessions = [StubModel(uuid.uuid4()), StubModel(uuid.uuid4())]
        self.agents = self._Agents(self)
        self.cohorts = self._Cohorts(self)
        self.cohort_versions = self._CohortVersions(self)
        self.sessions = self._Sessions(self)

    class _Sessions:
        def __init__(self, owner: "StubCohortClient") -> None:
            self.owner = owner
            self.params: SessionListParams | None = None

        async def iter(self, params: SessionListParams):
            self.params = params
            for session in self.owner.selected_sessions:
                yield session

    class _Agents:
        def __init__(self, owner: "StubCohortClient") -> None:
            self.owner = owner

        async def get(self, agent_id: uuid.UUID) -> StubModel:
            self.owner.agent_lookups += 1
            assert agent_id == self.owner.agent.id
            return self.owner.agent

        async def iter(self):
            self.owner.agent_lookups += 1
            yield self.owner.agent

    class _Cohorts:
        def __init__(self, owner: "StubCohortClient") -> None:
            self.owner = owner

        async def create(self, request: CohortCreateRequest) -> StubModel:
            self.owner.created_cohorts.append(request)
            return self.owner.cohort

        async def get(self, cohort_id: uuid.UUID) -> StubModel:
            self.owner.cohort_lookups += 1
            assert cohort_id == self.owner.cohort.id
            return self.owner.cohort

        async def iter(self):
            self.owner.cohort_lookups += 1
            yield self.owner.cohort

        async def list(self, params: CohortListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(items=[self.owner.cohort], next_cursor="next")

        async def update(
            self, cohort_id: uuid.UUID, request: CohortUpdateRequest
        ) -> StubModel:
            self.owner.update_calls.append((cohort_id, request))
            return self.owner.cohort

        async def delete(self, cohort_id: uuid.UUID) -> None:
            self.owner.deleted_cohorts.append(cohort_id)

        async def create_version(
            self, cohort_id: uuid.UUID, request: CohortVersionCreateRequest
        ) -> StubModel:
            self.owner.created_versions.append((cohort_id, request))
            return self.owner.version_two

        async def list_versions(self, cohort_id: uuid.UUID, params: ListParams) -> Any:
            self.owner.version_list_calls.append((cohort_id, params))
            return SimpleNamespace(
                items=[self.owner.version_two], next_cursor="next-version"
            )

        async def iter_versions(self, cohort_id: uuid.UUID):
            assert cohort_id == self.owner.cohort.id
            yield self.owner.version_one
            yield self.owner.version_two

    class _CohortVersions:
        def __init__(self, owner: "StubCohortClient") -> None:
            self.owner = owner

        async def get(self, version_id: uuid.UUID) -> StubModel:
            self.owner.version_get_calls.append(version_id)
            assert version_id == self.owner.version_two.id
            return self.owner.version_two

        async def update(
            self, version_id: uuid.UUID, request: CohortVersionUpdateRequest
        ) -> StubModel:
            self.owner.version_update_calls.append((version_id, request))
            return self.owner.version_two

        async def delete(self, version_id: uuid.UUID) -> None:
            self.owner.deleted_versions.append(version_id)


async def test_cohort_create_list_and_get_map_to_existing_sdk() -> None:
    """Cohort creation and reads preserve exact SDK requests and envelopes."""
    client = StubCohortClient()

    created = await cohorts.create_cohort(
        client,
        "nightly",
        agent=str(client.agent.id),
        description="Nightly cases",
        metadata='{"priority":2}',
    )
    [request] = client.created_cohorts
    assert isinstance(request, CohortCreateRequest)
    assert request.model_dump(mode="json") == {
        "name": "nightly",
        "description": "Nightly cases",
        "agent_id": str(client.agent.id),
        "metadata": {"priority": 2},
    }
    assert created.item["id"] == str(client.cohort.id)
    assert created.next_actions == []

    listed = await cohorts.list_cohorts(
        client,
        size=7,
        cursor="cursor",
        sort="created:asc",
        filter='{"field":"name","op":"eq","value":"regression"}',
    )
    [params] = client.list_calls
    assert isinstance(params, CohortListParams)
    assert params.size == 7
    assert params.cursor == "cursor"
    assert params.sort == "created:asc"
    dumped_params = params.model_dump(mode="json")
    assert json.loads(dumped_params["filter"]) == {
        "field": "name",
        "op": "eq",
        "value": "regression",
    }
    assert listed.page == {
        "limit": 7,
        "next_cursor": "next",
        "truncated": True,
    }

    fetched = await cohorts.get_cohort(client, "regression")
    assert fetched.item["name"] == "regression"


async def test_cohort_create_rejects_metadata_before_lookup() -> None:
    """Malformed metadata cannot trigger resource resolution or mutation."""
    client = StubCohortClient()

    with pytest.raises(CLIError, match="--metadata is not valid JSON"):
        await cohorts.create_cohort(
            client,
            "bad",
            agent="assistant",
            description=None,
            metadata="{",
        )

    assert client.agent_lookups == 0
    assert client.created_cohorts == []


async def test_cohort_create_snapshots_a_tag_selection() -> None:
    """Create the cohort parent and first immutable membership in one command."""
    client = StubCohortClient()

    result = await cohorts.create_cohort(
        client,
        "nightly",
        agent="assistant",
        description="Nightly cases",
        metadata=None,
        tag="baseline",
        display_version="discovery-v1",
    )

    assert client.sessions.params is not None
    session_filter = client.sessions.params.filter
    assert isinstance(session_filter, FilterCondition)
    assert session_filter.field == "tag"
    assert session_filter.value == "baseline"
    _, request = client.created_versions[0]
    assert request.add_session_ids == [
        session.id for session in client.selected_sessions
    ]
    assert request.display_version == "discovery-v1"
    assert result.item["reference"] == "regression@2"
    assert result.item["session_count"] == 2


async def test_cohort_update_is_sparse_and_supports_explicit_clears() -> None:
    """Omitted fields remain unset while explicit null and empty metadata persist."""
    client = StubCohortClient()

    await cohorts.update_cohort(
        client,
        "regression",
        name="renamed",
        description=None,
        clear_description=False,
        metadata=None,
    )
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {"name": "renamed"}

    await cohorts.update_cohort(
        client,
        str(client.cohort.id),
        name=None,
        description=None,
        clear_description=True,
        metadata="{}",
    )
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "description": None,
        "metadata": {},
    }


@pytest.mark.parametrize(
    ("description", "clear_description", "metadata", "message"),
    [
        ("set", True, None, "cannot be used together"),
        (None, False, None, "Select at least one cohort update"),
        (None, False, "[]", "must contain a JSON object"),
    ],
)
async def test_cohort_update_validation_precedes_lookup(
    description: str | None,
    clear_description: bool,
    metadata: str | None,
    message: str,
) -> None:
    """Invalid sparse updates fail before cohort resolution."""
    client = StubCohortClient()

    with pytest.raises(CLIError, match=message):
        await cohorts.update_cohort(
            client,
            "regression",
            name=None,
            description=description,
            clear_description=clear_description,
            metadata=metadata,
        )

    assert client.cohort_lookups == 0
    assert client.update_calls == []


async def test_cohort_deletes_require_force_before_lookup() -> None:
    """Both destructive commands require force before resolving remote state."""
    client = StubCohortClient()

    with pytest.raises(CLIError, match="requires --force"):
        await cohorts.delete_cohort(client, "regression", force=False)
    with pytest.raises(CLIError, match="requires --force"):
        await cohorts.delete_cohort_version(client, "regression@2", force=False)
    assert client.cohort_lookups == 0
    assert client.version_get_calls == []

    cohort_result = await cohorts.delete_cohort(client, "regression", force=True)
    version_result = await cohorts.delete_cohort_version(
        client, "regression@2", force=True
    )
    assert cohort_result.item == {"id": str(client.cohort.id), "deleted": True}
    assert version_result.item == {
        "id": str(client.version_two.id),
        "deleted": True,
    }
    assert client.deleted_cohorts == [client.cohort.id]
    assert client.deleted_versions == [client.version_two.id]


async def test_version_create_preserves_order_and_empty_delta() -> None:
    """Non-overlapping membership deltas stay ordered and may be empty."""
    client = StubCohortClient()
    first = uuid.uuid4()
    second = uuid.uuid4()
    removed = uuid.uuid4()

    result = await cohorts.create_cohort_version(
        client,
        "regression",
        add_session_ids=[first, second],
        remove_session_ids=[removed],
        display_version="candidate",
    )
    _, request = client.created_versions[-1]
    assert request.add_session_ids == [first, second]
    assert request.remove_session_ids == [removed]
    assert request.display_version == "candidate"
    assert result.warnings == []

    empty = await cohorts.create_cohort_version(
        client,
        "regression",
        add_session_ids=None,
        remove_session_ids=None,
        display_version=None,
    )
    assert "membership is unchanged" in empty.warnings[0]
    _, request = client.created_versions[-1]
    assert request.add_session_ids == []
    assert request.remove_session_ids == []


async def test_version_create_rejects_overlap_before_lookup() -> None:
    """The same session cannot appear in both sides of a membership delta."""
    client = StubCohortClient()
    session_id = uuid.uuid4()

    with pytest.raises(CLIError) as error:
        await cohorts.create_cohort_version(
            client,
            "regression",
            add_session_ids=[session_id],
            remove_session_ids=[session_id],
            display_version=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert "both --add-session and --remove-session" in error.value.message
    assert client.cohort_lookups == 0
    assert client.created_versions == []


@pytest.mark.parametrize("option", ["add", "remove"])
async def test_version_create_rejects_duplicates_before_lookup(option: str) -> None:
    """Duplicates within either delta list fail before resource resolution."""
    client = StubCohortClient()
    session_id = uuid.uuid4()

    with pytest.raises(CLIError, match=f"--{option}-session value must be unique"):
        await cohorts.create_cohort_version(
            client,
            "regression",
            add_session_ids=[session_id, session_id] if option == "add" else None,
            remove_session_ids=[session_id, session_id] if option == "remove" else None,
            display_version=None,
        )

    assert client.cohort_lookups == 0
    assert client.created_versions == []


async def test_version_resolution_supports_uuid_number_and_latest() -> None:
    """Version references resolve only stored IDs or server-assigned numbers."""
    client = StubCohortClient()

    parent, by_id = await cohorts.get_cohort_version(client, str(client.version_two.id))
    assert parent.id == client.cohort.id
    assert by_id.id == client.version_two.id
    assert client.version_get_calls == [client.version_two.id]

    _, by_number = await cohorts.get_cohort_version(client, "regression@1")
    _, latest = await cohorts.get_cohort_version(client, "regression@latest")
    assert by_number.id == client.version_one.id
    assert latest.id == client.version_two.id

    with pytest.raises(CLIError, match="has no version 3"):
        await cohorts.get_cohort_version(client, "regression@3")
    with pytest.raises(CLIError, match="must be PARENT@VERSION"):
        await cohorts.get_cohort_version(client, "baseline")


async def test_version_list_and_sparse_update_map_to_sdk() -> None:
    """Nested list and display-version updates use existing bounded SDK calls."""
    client = StubCohortClient()

    listed = await cohorts.list_cohort_versions(
        client,
        "regression",
        size=3,
        cursor="versions",
        sort="created:asc",
    )
    cohort_id, params = client.version_list_calls[-1]
    assert cohort_id == client.cohort.id
    assert params.model_dump(mode="json") == {
        "cursor": "versions",
        "size": 3,
        "sort": "created:asc",
    }
    assert listed.page is not None
    assert listed.page["next_cursor"] == "next-version"

    await cohorts.update_cohort_version(
        client,
        "regression@2",
        display_version="stable",
        clear_display_version=False,
    )
    _, request = client.version_update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "display_version": "stable"
    }

    await cohorts.update_cohort_version(
        client,
        str(client.version_two.id),
        display_version=None,
        clear_display_version=True,
    )
    _, request = client.version_update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "display_version": None
    }

    with pytest.raises(CLIError, match="Select exactly one"):
        await cohorts.update_cohort_version(
            client,
            "regression@2",
            display_version=None,
            clear_display_version=False,
        )


@pytest.fixture
def argv_client(
    monkeypatch: pytest.MonkeyPatch,
) -> StubCohortClient:
    """Route public CLI invocations through one recording client."""
    client = StubCohortClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_cohort_argv_covers_all_parent_commands(
    argv_client: StubCohortClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """The registered parent leaves emit standard JSON and text results."""
    client = argv_client

    assert (
        app_module.main(
            [
                "cohort",
                "create",
                "nightly",
                "--agent",
                "assistant",
                "--metadata",
                '{"priority":1}',
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.create"
    assert payload["item"]["id"] == str(client.cohort.id)

    assert app_module.main(["cohort", "list", "--size", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.list"
    assert payload["count"] == 1

    assert app_module.main(["cohort", "get", "regression", "--output", "text"]) == 0
    assert "regression" in capsys.readouterr().out

    assert (
        app_module.main(["cohort", "update", "regression", "--clear-description"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.update"

    assert app_module.main(["cohort", "delete", "regression", "--force"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.delete"
    assert payload["item"]["deleted"] is True


def test_public_cohort_create_snapshots_a_tag(
    argv_client: StubCohortClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """The public command creates a parent and membership snapshot from a tag."""
    assert (
        app_module.main(
            [
                "cohort",
                "create",
                "nightly",
                "--agent",
                "assistant",
                "--tag",
                "baseline",
                "--display-version",
                "discovery-v1",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.create"
    assert payload["item"]["reference"] == "regression@2"


def test_public_cohort_version_argv_covers_all_commands(
    argv_client: StubCohortClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """The registered nested leaves forward exact references and deltas."""
    client = argv_client
    added_session_id = uuid.uuid4()
    removed_session_id = uuid.uuid4()

    assert (
        app_module.main(
            [
                "cohort",
                "version",
                "create",
                "regression",
                "--add-session",
                str(added_session_id),
                "--remove-session",
                str(removed_session_id),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.version.create"
    _, request = client.created_versions[-1]
    assert request.add_session_ids == [added_session_id]
    assert request.remove_session_ids == [removed_session_id]

    assert (
        app_module.main(["cohort", "version", "list", "regression", "--size", "1"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.version.list"
    assert payload["page"]["limit"] == 1

    assert app_module.main(["cohort", "version", "get", "regression@latest"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"]["version"] == 2

    assert (
        app_module.main(
            [
                "cohort",
                "version",
                "update",
                str(client.version_two.id),
                "--clear-display-version",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.version.update"

    assert (
        app_module.main(["cohort", "version", "delete", "regression@2", "--force"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.version.delete"
    assert payload["item"]["deleted"] is True


def test_public_argv_errors_are_structured_and_do_not_mutate(
    argv_client: StubCohortClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Local force and delta errors use stable structured stderr."""
    client = argv_client

    assert app_module.main(["cohort", "delete", "regression"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["command"] == "cohort.delete"
    assert error["error"]["kind"] == "invalid_arguments"
    assert client.deleted_cohorts == []

    session_id = uuid.uuid4()
    assert (
        app_module.main(
            [
                "cohort",
                "version",
                "create",
                "regression",
                "--add-session",
                str(session_id),
                "--add-session",
                str(session_id),
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)
    assert error["command"] == "cohort.version.create"
    assert error["error"]["kind"] == "invalid_arguments"
