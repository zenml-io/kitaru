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
"""Regression coverage for duplicate classification during session ingestion."""

import json
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import httpx
import pytest
from task_fixtures import TaskAppFixture, build_task_app, start_task

from conftest import imported_node, imported_session
from kitaru.api_models.v1.imports import ImportFailure, ImportStats
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.session_node import (
    SessionIngestRequest,
    SessionNodeListParams,
)
from kitaru.api_models.v1.task import (
    BlobImportSourceSpec,
    ImportTaskDetails,
    ScriptPluginSpec,
    TaskKind,
    TaskSpecResponse,
    TaskStatus,
)
from kitaru.client.exceptions import APIError
from kitaru.server.domain.agent_version import RunSpec
from kitaru.task.importer import ingest_session, run
from kitaru_test_support import create_agent_task, create_agent_version, create_job


@pytest.fixture
async def task_app() -> AsyncGenerator[TaskAppFixture, None]:
    """Route the SDK through the real REST app and fake repositories."""
    async for value in build_task_app():
        yield value


@pytest.mark.parametrize("origin", [SessionOrigin.RECORDED, SessionOrigin.REPLAY])
async def test_terminal_session_without_provider_propagates_ingest_conflict(
    task_app: TaskAppFixture,
    origin: SessionOrigin,
) -> None:
    """Report rejected terminal ingestion rather than returning a duplicate."""
    with pytest.raises(APIError, match="does not accept node ingestion") as caught:
        await ingest_session(
            task_app.client,
            imported_session("recorded", nodes=[imported_node("node")]),
            task_app.agent.id,
            None,
            origin,
        )
    assert caught.value.status_code == httpx.codes.CONFLICT


async def test_terminal_replay_with_provider_ingests_nodes(
    task_app: TaskAppFixture,
) -> None:
    """Ingest a completed replay with its source provider and initial nodes."""
    session = await ingest_session(
        task_app.client,
        imported_session("replayed", nodes=[imported_node("node")]),
        task_app.agent.id,
        "acme",
        SessionOrigin.REPLAY,
    )
    assert session is not None
    assert session.imported_from == "acme"
    assert session.status == SessionStatus.COMPLETED
    assert session.origin == SessionOrigin.REPLAY
    nodes = await task_app.client.sessions.list_nodes(
        session.id, SessionNodeListParams()
    )
    assert len(nodes.items) == 1
    assert nodes.items[0].external_id == "node"


@pytest.mark.parametrize("conflict", ["not-running", "already-linked", "stale-attempt"])
async def test_task_lifecycle_conflicts_propagate(
    task_app: TaskAppFixture,
    conflict: str,
) -> None:
    """Preserve task lifecycle failures across REST, SDK, and import helper."""
    version = await create_agent_version(
        task_app.services.agent_versions,
        task_app.agent.id,
        task_app.agent.owner_id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    job = await create_job(task_app.services.jobs, task_app.agent.owner_id)
    task = await create_agent_task(
        task_app.services.tasks, job.id, agent_version_id=version.id
    )
    task_app.auth.principal = await start_task(task_app, task.id)
    if conflict == "already-linked":
        assert (
            await ingest_session(
                task_app.client, imported_session("first"), None, "acme"
            )
            is not None
        )
        expected = "already links a result session"
    else:
        current = await task_app.services.tasks.get(task.id)
        if conflict == "not-running":
            current.status = TaskStatus.COMPLETED
            expected = "is not running"
        else:
            current.attempt += 1
            expected = "fenced by attempt"
        await task_app.services.tasks.update(current)
    with pytest.raises(APIError, match=expected) as caught:
        await ingest_session(task_app.client, imported_session("second"), None, "acme")
    assert caught.value.status_code == httpx.codes.CONFLICT


@pytest.mark.parametrize(
    ("detail", "duplicate"),
    [
        (
            "Session with imported_from 'acme' and external_id 'trace' "
            "is already registered",
            True,
        ),
        (
            "Session with imported_from 'other' and external_id 'trace' "
            "is already registered",
            False,
        ),
        (
            "Session with imported_from 'acme' and external_id 'other' "
            "is already registered",
            False,
        ),
        (
            "prefix Session with imported_from 'acme' and external_id 'trace' "
            "is already registered",
            False,
        ),
        (
            "Session with imported_from 'acme' and external_id 'trace' "
            "is already registered suffix",
            False,
        ),
    ],
)
async def test_legacy_duplicate_requires_exact_source_identity(
    task_app: TaskAppFixture,
    monkeypatch: pytest.MonkeyPatch,
    detail: str,
    duplicate: bool,
) -> None:
    """Skip only the canonical legacy error for the requested source identity."""
    error = APIError(httpx.codes.CONFLICT, detail)

    async def reject(request: SessionIngestRequest) -> None:
        raise error

    monkeypatch.setattr(task_app.client.sessions, "ingest", reject)
    if duplicate:
        assert (
            await ingest_session(
                task_app.client, imported_session("trace"), task_app.agent.id, "acme"
            )
            is None
        )
    else:
        with pytest.raises(APIError) as caught:
            await ingest_session(
                task_app.client, imported_session("trace"), task_app.agent.id, "acme"
            )
        assert caught.value is error


@pytest.mark.parametrize(
    ("status", "code", "duplicate"),
    [
        (409, "duplicate_session_external_id", True),
        (409, "future_conflict", False),
        (503, "duplicate_session_external_id", False),
    ],
)
async def test_duplicate_code_requires_conflict_and_known_code(
    task_app: TaskAppFixture,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    code: str,
    duplicate: bool,
) -> None:
    """Prefer the code even when a legacy message would suggest a duplicate."""
    detail = (
        "unrelated detail"
        if duplicate
        else "Session with imported_from 'acme' and external_id 'trace' "
        "is already registered"
    )
    error = APIError(status, detail, code=code)

    async def reject(request: SessionIngestRequest) -> None:
        raise error

    monkeypatch.setattr(task_app.client.sessions, "ingest", reject)
    if duplicate:
        assert (
            await ingest_session(
                task_app.client, imported_session("trace"), task_app.agent.id, "acme"
            )
            is None
        )
    else:
        with pytest.raises(APIError) as caught:
            await ingest_session(
                task_app.client, imported_session("trace"), task_app.agent.id, "acme"
            )
        assert caught.value is error


_SINGLE_SESSION_PARSER_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession


def parse(payload: bytes, params: dict):
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=[],
    )
"""


@pytest.mark.parametrize(
    "status", [httpx.codes.CONFLICT, httpx.codes.SERVICE_UNAVAILABLE]
)
async def test_importer_flow_records_nonduplicate_ingestion_error(
    task_app: TaskAppFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    """Count rejected ingestion as a failure rather than a duplicate skip."""
    task_id = uuid.uuid4()
    spec = TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.IMPORTER,
        timeout_seconds=30,
        env={},
        secret_env={},
        details=ImportTaskDetails(
            plugin=ScriptPluginSpec(
                entrypoint="parse", blob_id=uuid.uuid4(), sha256="x"
            ),
            source=BlobImportSourceSpec(blob_id=uuid.uuid4(), sha256="x"),
            agent_id=task_app.agent.id,
            provider="acme",
            params={},
        ),
    )
    plugin_path = tmp_path / "importer.py"
    plugin_path.write_text(_SINGLE_SESSION_PARSER_SCRIPT)
    payload_path = tmp_path / "payload.json"
    payload_path.write_text("{}")
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    async def get_spec(requested_task_id: uuid.UUID) -> TaskSpecResponse:
        assert requested_task_id == task_id
        return spec

    async def reject(request: SessionIngestRequest) -> None:
        raise APIError(status, "backend unavailable")

    monkeypatch.setattr(task_app.client.tasks, "get_spec", get_spec)
    monkeypatch.setattr(task_app.client.sessions, "ingest", reject)

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert (written.created, written.skipped, written.failed) == (0, 0, 1)
    assert written.failures == [
        ImportFailure(
            line=1, external_id="session-1", error=f"{status}: backend unavailable"
        )
    ]
