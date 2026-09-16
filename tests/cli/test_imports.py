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

from kitaru.api_models.v1.imports import (
    BlobImportSource,
    ImportAnalyzeRequest,
    ImportListParams,
    ImportResponse,
)
from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.api_models.v1.task import TaskKind, TaskOnFailure, TaskResponse, TaskStatus
from kitaru.cli import app as app_module
from kitaru.cli import imports
from kitaru.cli.output import CLIError


def _job(status: JobStatus = JobStatus.PENDING) -> JobResponse:
    """Build one analysis job response."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return JobResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        created=now,
        updated=now,
        kind=JobKind.ANALYSIS,
        status=status,
        started_at=now if status is not JobStatus.PENDING else None,
        ended_at=now
        if status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}
        else None,
        error="analysis failed" if status is JobStatus.FAILED else None,
    )


def _task(job: JobResponse, status: TaskStatus = TaskStatus.COMPLETED) -> TaskResponse:
    """Build one terminal analysis task response."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return TaskResponse(
        id=uuid.uuid4(),
        job_id=job.id,
        kind=TaskKind.ANALYZER,
        status=status,
        on_failure=TaskOnFailure.CONTINUE,
        attempt=1,
        labels={},
        error="analyzer crashed" if status is TaskStatus.FAILED else None,
        result=None,
        created=now,
        updated=now,
    )


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
            source=BlobImportSource(blob_id=uuid.uuid4()),
            params={},
            evaluators=[],
            analyzers=[],
            stats=None,
            error=None,
            created=now,
            updated=now,
        )
        self.analyzer = SimpleNamespace(
            id=uuid.uuid4(), name="clustering", latest_version=2
        )
        self.analyzer_version = SimpleNamespace(
            id=uuid.uuid4(), analyzer_id=self.analyzer.id, version=2
        )
        self.connection = SimpleNamespace(id=uuid.uuid4(), name="openai-prod")
        self.job = _job()
        self.list_calls: list[ImportListParams] = []
        self.get_calls: list[uuid.UUID] = []
        self.analyze_calls: list[
            tuple[uuid.UUID, ImportAnalyzeRequest, str | None]
        ] = []
        self.imports = self._Imports(self)
        self.analyzers = self._Analyzers(self)
        self.connections = self._Connections(self)

    class _Imports:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def list(self, params: ImportListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(items=[self.owner.import_], next_cursor="next")

        async def get(self, import_id: uuid.UUID) -> ImportResponse:
            self.owner.get_calls.append(import_id)
            return self.owner.import_

        async def analyze(
            self,
            import_id: uuid.UUID,
            request: ImportAnalyzeRequest,
            idempotency_key: str | None = None,
        ) -> JobResponse:
            self.owner.analyze_calls.append((import_id, request, idempotency_key))
            return self.owner.job

    class _Analyzers:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            return SimpleNamespace(items=[self.owner.analyzer], next_cursor=None)

        async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
            assert parent_id == self.owner.analyzer.id
            assert version == self.owner.analyzer_version.version
            return self.owner.analyzer_version

    class _Connections:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            return SimpleNamespace(items=[self.owner.connection], next_cursor=None)


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

    assert (
        app_module.main(
            [
                "import",
                "analyze",
                str(client.import_id),
                "--analyzer",
                "clustering@2",
                "--idempotency-key",
                "rerun-1",
            ]
        )
        == 0
    )
    analyzed = json.loads(capsys.readouterr().out)
    assert analyzed["command"] == "import.analyze"
    assert analyzed["item"]["job"]["id"] == str(client.job.id)
    [(import_id, _, idempotency_key)] = client.analyze_calls
    assert import_id == client.import_id
    assert idempotency_key == "rerun-1"


async def test_analyze_forwards_analyzer_configs() -> None:
    """Analyze resolves exact analyzer versions and sends their configs."""
    client = StubImportClient()

    result = await imports.analyze_import(
        client,
        client.import_id,
        analyzers=["clustering@2"],
        analyzer_params=['clustering@2={"min_size": 5}'],
        analyzer_connections=["clustering@2=openai-prod"],
        wait=False,
        interval=None,
        timeout=None,
    )

    [(import_id, request, idempotency_key)] = client.analyze_calls
    assert import_id == client.import_id
    assert idempotency_key is None
    assert request.model_dump(mode="json") == {
        "analyzers": [
            {
                "analyzer": "clustering",
                "min_sessions": None,
                "version": 2,
                "params": {"min_size": 5},
                "connection_id": str(client.connection.id),
            }
        ]
    }
    assert result.event == "created"
    assert result.item["operation"] == "import_analysis"
    assert result.item["terminal"] is False
    assert result.item["import_id"] == str(client.import_id)
    assert result.item["analyzers"][0]["name"] == "clustering"
    assert result.item["analyzers"][0]["version"] == 2
    assert result.item["job"]["id"] == str(client.job.id)
    assert f"kitaru job watch {client.job.id}" in result.next_actions
    assert "kitaru insight list" in result.next_actions


async def test_analyze_requires_an_analyzer() -> None:
    """Analyze rejects an empty analyzer selection before calling the server."""
    client = StubImportClient()

    with pytest.raises(CLIError) as excinfo:
        await imports.analyze_import(
            client,
            client.import_id,
            analyzers=[],
            analyzer_params=None,
            analyzer_connections=None,
            wait=False,
            interval=None,
            timeout=None,
        )

    assert excinfo.value.kind == "invalid_arguments"
    assert client.analyze_calls == []


async def test_analyze_waits_for_terminal_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Analyze with --wait maps the settled tasks to a terminal receipt."""
    client = StubImportClient()
    events: list[tuple[str, Any]] = []
    job = _job(JobStatus.COMPLETED)
    task = _task(job)

    async def wait_for_terminal_tasks(*args: Any, **kwargs: Any) -> Any:
        assert args[1] == client.job.id
        return job, [task]

    monkeypatch.setattr(
        imports.receipts, "wait_for_terminal_tasks", wait_for_terminal_tasks
    )
    monkeypatch.setattr(
        imports, "emit_event", lambda event, item: events.append((event, item))
    )

    result = await imports.analyze_import(
        client,
        client.import_id,
        analyzers=["clustering@2"],
        analyzer_params=None,
        analyzer_connections=None,
        wait=True,
        interval=None,
        timeout=None,
    )

    assert events[0][0] == "created"
    assert result.event == "terminal"
    assert result.item["terminal"] is True
    assert result.item["job"]["status"] == "completed"
    assert result.item["tasks"] == [
        {"id": str(task.id), "status": "completed", "error": None}
    ]
    assert str(task.id) in result.next_actions[0]
    assert result.next_actions[0].startswith("kitaru insight list")


async def test_analyze_reports_a_skipped_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Analyze with --wait reports a skipped analyzer task as skipped."""
    client = StubImportClient()
    events: list[tuple[str, Any]] = []
    job = _job(JobStatus.COMPLETED)
    task = _task(job, status=TaskStatus.SKIPPED)

    async def wait_for_terminal_tasks(*args: Any, **kwargs: Any) -> Any:
        assert args[1] == client.job.id
        return job, [task]

    monkeypatch.setattr(
        imports.receipts, "wait_for_terminal_tasks", wait_for_terminal_tasks
    )
    monkeypatch.setattr(
        imports, "emit_event", lambda event, item: events.append((event, item))
    )

    result = await imports.analyze_import(
        client,
        client.import_id,
        analyzers=["clustering@2"],
        analyzer_params=None,
        analyzer_connections=None,
        wait=True,
        interval=None,
        timeout=None,
    )

    assert result.event == "terminal"
    assert result.item["terminal"] is True
    assert result.item["tasks"] == [
        {"id": str(task.id), "status": "skipped", "error": None}
    ]


async def test_analyze_raises_for_a_failed_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Analyze with --wait raises a remote failure for a failed job."""
    client = StubImportClient()
    job = _job(JobStatus.FAILED)
    task = _task(job, status=TaskStatus.FAILED)

    async def wait_for_terminal_tasks(*args: Any, **kwargs: Any) -> Any:
        return job, [task]

    monkeypatch.setattr(
        imports.receipts, "wait_for_terminal_tasks", wait_for_terminal_tasks
    )
    monkeypatch.setattr(imports, "emit_event", lambda event, item: None)
    monkeypatch.setattr(imports.receipts, "emit_event", lambda event, item: None)

    with pytest.raises(CLIError) as excinfo:
        await imports.analyze_import(
            client,
            client.import_id,
            analyzers=["clustering@2"],
            analyzer_params=None,
            analyzer_connections=None,
            wait=True,
            interval=None,
            timeout=None,
        )

    assert excinfo.value.kind == "remote_failed"
    receipt = excinfo.value.details["receipt"]
    assert receipt["tasks"] == [
        {"id": str(task.id), "status": "failed", "error": "analyzer crashed"}
    ]
