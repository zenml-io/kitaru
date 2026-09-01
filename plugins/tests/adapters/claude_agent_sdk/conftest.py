#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Provider-free Kitaru client fixtures for Claude adapter tests."""

import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest

import kitaru_claude_agent_sdk.recording as recording_module
from kitaru.api_models.v1.task import AgentTaskDetails


class FakeSessions:
    """Capture session writes and inject bounded persistence failures."""

    def __init__(self, client: "FakeClient") -> None:
        self.client = client
        self.created: list[Any] = []
        self.updated: list[tuple[uuid.UUID, Any]] = []
        self.node_batches: list[tuple[uuid.UUID, Any]] = []

    async def create(self, request: Any) -> Any:
        self.created.append(request)
        return SimpleNamespace(id=self.client.session_id)

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        if self.client.ingest_error is not None:
            error = self.client.ingest_error
            self.client.ingest_error = None
            raise error
        self.node_batches.append((session_id, request))
        return []

    async def update(self, session_id: uuid.UUID, request: Any) -> None:
        self.updated.append((session_id, request))
        if self.client.update_error is not None:
            raise self.client.update_error


class FakeClient:
    """Small resource-shaped Kitaru API client."""

    def __init__(self) -> None:
        self.session_id = uuid.uuid4()
        self.sessions = FakeSessions(self)
        self.ingest_error: BaseException | None = None
        self.update_error: BaseException | None = None
        self.close_count = 0
        self.task_inputs: Any = "task input"
        self.replay: Any = None
        self.tasks = SimpleNamespace(get_spec=self._get_task)
        self.replays = SimpleNamespace(get=self._get_replay)

    async def _get_task(self, _: uuid.UUID) -> Any:
        return SimpleNamespace(details=AgentTaskDetails(inputs=self.task_inputs))

    async def _get_replay(self, _: uuid.UUID) -> Any:
        return self.replay

    async def close(self) -> None:
        self.close_count += 1


@pytest.fixture
def fake_client() -> FakeClient:
    """Create one isolated fake Kitaru client."""
    return FakeClient()


@pytest.fixture(autouse=True)
def isolate_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep task/replay process inputs out of provider-free tests."""
    for name in ("KITARU_TASK_ID", "KITARU_TASK_INPUTS", "KITARU_REPLAY_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(recording_module, "KitaruAPIClient", FakeClient)
    yield


def nodes(client: FakeClient) -> list[Any]:
    """Flatten all persisted node batches."""
    return [node for _, batch in client.sessions.node_batches for node in batch.nodes]
