#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Import CLI behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.imports import ImportListParams, ImportResponse
from kitaru.cli import app as app_module
from kitaru.cli import imports


class StubImportClient:
    """Protocol-shaped client recording import calls."""

    def __init__(self) -> None:
        self.import_id = uuid.uuid4()
        self.job_id = uuid.uuid4()
        self.agent_id = uuid.uuid4()
        now = datetime.now(UTC)
        self.import_ = ImportResponse(
            id=self.import_id,
            owner_id=uuid.uuid4(),
            job_id=self.job_id,
            agent_id=self.agent_id,
            agent_version_id=None,
            importer_version_id=None,
            payload_blob_id=uuid.uuid4(),
            params={},
            evaluators=[],
            analyzers=[],
            stats=None,
            error=None,
            created=now,
            updated=now,
        )
        self.list_calls: list[ImportListParams] = []
        self.get_calls: list[uuid.UUID] = []
        self.imports = self._Imports(self)

    class _Imports:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def list(self, params: ImportListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(items=[self.owner.import_], next_cursor="next")

        async def get(self, import_id: uuid.UUID) -> ImportResponse:
            self.owner.get_calls.append(import_id)
            return self.owner.import_


async def test_list_and_get_preserve_sdk_results() -> None:
    """Finite reads forward pagination and do not remap import state."""
    client = StubImportClient()

    listed = await imports.list_imports(
        client,
        size=7,
        cursor="cursor",
        sort="created:asc",
        filter=f'{{"field":"agent_id","op":"eq","value":"{client.agent_id}"}}',
    )
    fetched = await imports.get_import(client, client.import_id)

    [params] = client.list_calls
    assert params.model_dump(mode="json", exclude_unset=True) == {
        "cursor": "cursor",
        "size": 7,
        "sort": "created:asc",
        "filter": (
            f'{{"field": "agent_id", "op": "eq", "value": "{client.agent_id}"}}'
        ),
    }
    assert listed.page == {"limit": 7, "next_cursor": "next", "truncated": True}
    assert listed.items == [client.import_.model_dump(mode="json")]
    assert fetched.item["id"] == str(client.import_id)
    assert fetched.item["job_id"] == str(client.job_id)
    assert client.get_calls == [client.import_id]


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubImportClient:
    """Route public import commands through one recording client."""
    client = StubImportClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_import_argv_covers_all_leaves(
    argv_client: StubImportClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """The public root exposes list and exact get commands."""
    client = argv_client

    assert (
        app_module.main(
            [
                "import",
                "list",
                "--size",
                "7",
                "--filter",
                f'{{"field":"agent_id","op":"eq","value":"{client.agent_id}"}}',
            ]
        )
        == 0
    )
    listed = json.loads(capsys.readouterr().out)
    assert listed["command"] == "import.list"
    assert listed["count"] == 1
    assert listed["items"][0]["id"] == str(client.import_id)
    [params] = client.list_calls
    assert params.size == 7

    assert app_module.main(["import", "get", str(client.import_id)]) == 0
    fetched = json.loads(capsys.readouterr().out)
    assert fetched["command"] == "import.get"
    assert fetched["item"]["job_id"] == str(client.job_id)
