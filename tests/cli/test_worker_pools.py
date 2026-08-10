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
"""Worker pool CLI behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import LabelSelector, WorkerScope
from kitaru.api_models.v1.worker_pool import (
    WorkerPoolCreateRequest,
    WorkerPoolUpdateRequest,
)
from kitaru.cli import app as app_module
from kitaru.cli import workers
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


@dataclass
class StubWorkerPools:
    """Worker pool resource fake for create, list, get, stats, update, and delete."""

    items: list[StubModel]
    created: list[WorkerPoolCreateRequest] = field(default_factory=list)
    list_calls: list[Any] = field(default_factory=list)
    update_calls: list[tuple[uuid.UUID, WorkerPoolUpdateRequest]] = field(
        default_factory=list
    )
    deleted: list[uuid.UUID] = field(default_factory=list)
    stats_calls: list[Any] = field(default_factory=list)
    stats_response: StubModel = field(
        default_factory=lambda: StubModel(
            uuid.uuid4(),
            {
                "pending_tasks": 0,
                "in_flight_tasks": 0,
                "oldest_pending_seconds": None,
                "live_workers": 0,
                "capacity": 0,
            },
        )
    )

    async def create(self, request: WorkerPoolCreateRequest) -> StubModel:
        self.created.append(request)
        return self.items[0]

    async def get(self, pool_id: uuid.UUID) -> StubModel:
        return next(item for item in self.items if item.id == pool_id)

    async def stats(self, pool: Any) -> StubModel:
        self.stats_calls.append(pool)
        return self.stats_response

    async def list(self, params: Any = None) -> Any:
        self.list_calls.append(params)
        return SimpleNamespace(items=self.items, next_cursor=None)

    async def iter(self):
        for item in self.items:
            yield item

    async def update(
        self, pool_id: uuid.UUID, request: WorkerPoolUpdateRequest
    ) -> StubModel:
        self.update_calls.append((pool_id, request))
        return self.items[0]

    async def delete(self, pool_id: uuid.UUID) -> None:
        self.deleted.append(pool_id)


def _pool_response(name: str, *, owner_id: uuid.UUID | None = None) -> StubModel:
    """Build a worker pool response with only behavior-relevant fields."""
    return StubModel(
        uuid.uuid4(),
        {
            "name": name,
            "owner_id": str(owner_id or uuid.uuid4()),
            "scope": {},
        },
    )


async def test_worker_pool_create_list_and_get_map_to_existing_sdk() -> None:
    """Worker pool creation and reads preserve exact SDK requests and envelopes."""
    pool = _pool_response("pool-1")
    client = SimpleNamespace(worker_pools=StubWorkerPools([pool]))

    created = await workers.create_worker_pool(
        client, name="pool-1", kinds=[TaskKind.AGENT], selectors=["team=core"]
    )
    [request] = client.worker_pools.created
    assert isinstance(request, WorkerPoolCreateRequest)
    assert request.name == "pool-1"
    assert request.scope == WorkerScope(
        kinds=[TaskKind.AGENT],
        selectors=[LabelSelector(key="team", values=["core"])],
    )
    assert created.item["id"] == str(pool.id)

    listed = await workers.list_worker_pools(
        client, size=20, cursor=None, sort="created:desc", filter=None
    )
    assert [item["name"] for item in listed.items or []] == ["pool-1"]

    fetched = await workers.get_worker_pool(client, "pool-1")
    assert fetched.item["id"] == str(pool.id)


async def test_worker_pool_create_rejects_invalid_selector_before_sdk_call() -> None:
    """A malformed --selector cannot reach the SDK."""
    client = SimpleNamespace(worker_pools=StubWorkerPools([_pool_response("pool-1")]))

    with pytest.raises(CLIError, match="Invalid --selector"):
        await workers.create_worker_pool(
            client, name="pool-1", kinds=None, selectors=["missing-values="]
        )

    assert client.worker_pools.created == []


async def test_get_by_uuid_skips_list_lookup() -> None:
    """A UUID reference resolves through get without a list call."""
    pool = _pool_response("pool-1")
    client = SimpleNamespace(worker_pools=StubWorkerPools([pool]))

    fetched = await workers.get_worker_pool(client, str(pool.id))

    assert fetched.item["id"] == str(pool.id)
    assert client.worker_pools.list_calls == []


async def test_get_by_name_conflicts_before_returning_a_record() -> None:
    """Duplicate exact names remain a stable conflict instead of guessing."""
    client = SimpleNamespace(
        worker_pools=StubWorkerPools(
            [_pool_response("duplicate"), _pool_response("duplicate")]
        )
    )

    with pytest.raises(CLIError) as error:
        await workers.get_worker_pool(client, "duplicate")

    assert error.value.kind == "conflict"


async def test_get_by_name_not_found() -> None:
    """An unmatched name reference reports a stable not-found error."""
    client = SimpleNamespace(worker_pools=StubWorkerPools([]))

    with pytest.raises(CLIError) as error:
        await workers.get_worker_pool(client, "missing")

    assert error.value.kind == "not_found"


async def test_worker_pool_stats_passes_the_raw_reference() -> None:
    """The reference passes straight to the SDK, with no client-side resolution."""
    client = SimpleNamespace(worker_pools=StubWorkerPools([_pool_response("pool-1")]))

    result = await workers.get_worker_pool_stats(client, "pool-1")

    assert client.worker_pools.stats_calls == ["pool-1"]
    assert client.worker_pools.list_calls == []
    assert result.item["pending_tasks"] == 0
    assert result.item["live_workers"] == 0


async def test_worker_pool_update_is_sparse() -> None:
    """Omitted fields stay unset, and touching kinds or selectors sends the scope."""
    pool = _pool_response("pool-1")
    client = SimpleNamespace(worker_pools=StubWorkerPools([pool]))

    await workers.update_worker_pool(
        client, "pool-1", name="renamed", kinds=None, selectors=None
    )
    _, request = client.worker_pools.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {"name": "renamed"}

    await workers.update_worker_pool(
        client, str(pool.id), name=None, kinds=[TaskKind.IMPORTER], selectors=None
    )
    _, request = client.worker_pools.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "scope": {"kinds": ["importer"], "selectors": None}
    }


async def test_worker_pool_update_rejects_an_empty_selection() -> None:
    """An update naming no field fails before resolving the worker pool."""
    client = SimpleNamespace(worker_pools=StubWorkerPools([_pool_response("pool-1")]))

    with pytest.raises(CLIError, match="Select at least one worker pool update"):
        await workers.update_worker_pool(
            client, "pool-1", name=None, kinds=None, selectors=None
        )

    assert client.worker_pools.list_calls == []
    assert client.worker_pools.update_calls == []


async def test_worker_pool_delete_requires_force_before_lookup() -> None:
    """Deletion requires force before resolving remote state."""
    pool = _pool_response("pool-1")
    client = SimpleNamespace(worker_pools=StubWorkerPools([pool]))

    with pytest.raises(CLIError, match="requires --force"):
        await workers.delete_worker_pool(client, "pool-1", force=False)
    assert client.worker_pools.list_calls == []
    assert client.worker_pools.deleted == []

    result = await workers.delete_worker_pool(client, "pool-1", force=True)
    assert result.item == {"id": str(pool.id), "deleted": True}
    assert client.worker_pools.deleted == [pool.id]


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Route public CLI invocations through one recording client."""
    client = SimpleNamespace(worker_pools=StubWorkerPools([_pool_response("pool-1")]))

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_worker_pool_argv_covers_all_commands(
    argv_client: SimpleNamespace, capsys: pytest.CaptureFixture[str]
) -> None:
    """The registered leaves emit standard JSON results for every command."""
    client = argv_client
    pool = client.worker_pools.items[0]

    assert (
        app_module.main(
            [
                "worker",
                "pool",
                "create",
                "--name",
                "pool-1",
                "--kinds",
                "agent",
                "--selector",
                "team=core",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "worker.pool.create"
    assert payload["item"]["id"] == str(pool.id)
    [request] = client.worker_pools.created
    assert request.name == "pool-1"
    assert request.scope.kinds == [TaskKind.AGENT]

    assert app_module.main(["worker", "pool", "list", "--size", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "worker.pool.list"
    assert payload["count"] == 1

    assert app_module.main(["worker", "pool", "get", "pool-1", "--output", "text"]) == 0
    assert "pool-1" in capsys.readouterr().out

    assert app_module.main(["worker", "pool", "stats", "pool-1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "worker.pool.stats"
    assert payload["item"]["live_workers"] == 0

    assert (
        app_module.main(["worker", "pool", "update", "pool-1", "--name", "renamed"])
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "worker.pool.update"

    assert app_module.main(["worker", "pool", "delete", "pool-1", "--force"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "worker.pool.delete"
    assert payload["item"]["deleted"] is True


def test_public_worker_pool_delete_without_force_is_a_structured_error(
    argv_client: SimpleNamespace, capsys: pytest.CaptureFixture[str]
) -> None:
    """A missing --force stays a stable structured error and does not mutate."""
    client = argv_client

    assert app_module.main(["worker", "pool", "delete", "pool-1"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["command"] == "worker.pool.delete"
    assert error["error"]["kind"] == "invalid_arguments"
    assert client.worker_pools.deleted == []
