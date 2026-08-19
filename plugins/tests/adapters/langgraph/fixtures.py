"""Shared deterministic fixtures for the LangGraph adapter."""

import uuid
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

import kitaru_langgraph.recording as recording_module


class FakeSessions:
    """Record session calls and inject bounded failures."""

    def __init__(self, client: "FakeClient") -> None:
        self.client = client
        self.created: list[Any] = []
        self.updated: list[tuple[uuid.UUID, Any]] = []
        self.node_batches: list[tuple[uuid.UUID, Any]] = []

    async def create(self, request: Any) -> Any:
        if self.client.create_error is not None:
            raise self.client.create_error
        self.created.append(request)
        return SimpleNamespace(id=self.client.session_id)

    async def update(self, session_id: uuid.UUID, request: Any) -> Any:
        if self.client.update_error is not None:
            raise self.client.update_error
        self.updated.append((session_id, request))
        return None

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        self.client.ingest_count += 1
        if (
            self.client.ingest_error_at is not None
            and self.client.ingest_count == self.client.ingest_error_at
        ):
            raise RuntimeError("sentinel recording failure")
        self.node_batches.append((session_id, request))
        return []


class FakeClient:
    """Small resource-shaped Kitaru API client."""

    instances: ClassVar[list["FakeClient"]] = []
    next_create_error: ClassVar[BaseException | None] = None
    next_update_error: ClassVar[BaseException | None] = None
    next_ingest_error_at: ClassVar[int | None] = None
    next_replay: ClassVar[Any | None] = None
    next_lookup: ClassVar[Any | None] = None

    def __init__(self, **_: Any) -> None:
        self.session_id = uuid.uuid4()
        self.create_error = type(self).next_create_error
        self.update_error = type(self).next_update_error
        self.ingest_error_at = type(self).next_ingest_error_at
        self.ingest_count = 0
        self.sessions = FakeSessions(self)
        self.replay = type(self).next_replay
        self.lookup = type(self).next_lookup
        self.replays = SimpleNamespace(get=self._get_replay, tool_lookup=self._lookup)
        self.tasks = SimpleNamespace(get_spec=self._get_task)
        self.closed = False
        type(self).instances.append(self)

    async def _get_replay(self, _: uuid.UUID) -> Any:
        assert self.replay is not None
        return self.replay

    async def _lookup(self, *_: Any) -> Any:
        assert self.lookup is not None
        return self.lookup

    async def _get_task(self, _: uuid.UUID) -> Any:
        raise AssertionError("unexpected task lookup")

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def fake_client(monkeypatch: pytest.MonkeyPatch) -> type[FakeClient]:
    """Install a clean fake client for every adapter test."""
    for name in (
        "KITARU_API_KEY",
        "KITARU_API_TOKEN",
        "KITARU_API_URL",
        "KITARU_TASK_ID",
        "KITARU_TASK_INPUTS",
        "KITARU_SESSION_NAME",
        "KITARU_REPLAY_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    FakeClient.instances.clear()
    FakeClient.next_create_error = None
    FakeClient.next_update_error = None
    FakeClient.next_ingest_error_at = None
    FakeClient.next_replay = None
    FakeClient.next_lookup = None
    monkeypatch.setattr(recording_module, "KitaruAPIClient", FakeClient)
    return FakeClient
