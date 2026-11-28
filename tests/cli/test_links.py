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
"""Dashboard link resolution across CLI commands."""

import io
import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from tests.cli.experiment_run_fakes import StubRunsClient, build_run
from tests.cli.task_diagnostic_fakes import EmptyTaskDiagnostics

from kitaru.api_models.v1.cohort import CohortResponse
from kitaru.api_models.v1.cohort_version import CohortVersionResponse
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.imports import (
    BlobImportSource,
    ImportResponse,
    ImportStats,
)
from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.api_models.v1.replay import BaselineEvaluationMode
from kitaru.api_models.v1.task import TaskKind, TaskOnFailure, TaskResponse, TaskStatus
from kitaru.cli import app as app_module
from kitaru.cli import (
    cohorts,
    experiment_runs,
    experiments,
    imports,
    investigations,
    invocation_context,
    jobs,
    links,
    sessions,
)
from kitaru.cli.output import OutputContext, reset_output_context, set_output_context
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError

DASHBOARD_BASE = "https://cloud.example.com/kitaru-workspaces/ws-1"


def _dashboard_info(
    *,
    dashboard_url: str | None = f"{DASHBOARD_BASE}/",
    ui_version: str | None = None,
    server_url: str | None = None,
    max_blob_size_bytes: int | None = None,
) -> ServerInfoResponse:
    return ServerInfoResponse(
        version="0.0.0",
        auth_scheme=AuthScheme.CONTROL_PLANE,
        dashboard_url=dashboard_url,
        ui_version=ui_version,
        server_url=server_url,
        max_blob_size_bytes=max_blob_size_bytes,
    )


class _InfoStub:
    """Server info resource stub with call counting and optional error."""

    def __init__(
        self,
        response: Any = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.response = response if response is not None else _dashboard_info()
        self.error = error
        self.calls = 0

    async def get(self) -> Any:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.response


def _attach_info(
    client: Any,
    response: Any = None,
    *,
    base_url: str = "https://api.example.com",
    error: Exception | None = None,
) -> _InfoStub:
    stub = _InfoStub(response, error=error)
    client.base_url = base_url
    client.info = stub
    return stub


async def test_real_api_client_resolves_dashboard_links(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production API clients resolve links through the shared CLI helper."""
    client = KitaruAPIClient(base_url="https://api.example.com", api_key="test-key")
    monkeypatch.setattr(client.info, "get", lambda: _async_return(_dashboard_info()))

    resolved, warnings = await links.resolve_dashboard_links(
        client,
        lambda base: {"dashboard": f"{base}/sessions/1"},
        warning="link lookup failed",
    )

    assert resolved == {"dashboard": f"{DASHBOARD_BASE}/sessions/1"}
    assert warnings == []


async def test_resolve_dashboard_links_reuses_prefetched_info() -> None:
    """Passing server info skips an extra info network call."""
    info = _dashboard_info()
    client = SimpleNamespace()
    stub = _attach_info(client, info)

    resolved, warnings = await links.resolve_dashboard_links(
        client,
        lambda base: {"dashboard": f"{base}/sessions/1"},
        warning="link lookup failed",
        info=info,
    )

    assert resolved == {"dashboard": f"{DASHBOARD_BASE}/sessions/1"}
    assert warnings == []
    assert stub.calls == 0


async def test_resolve_dashboard_links_supports_bundled_ui() -> None:
    """A bundled-UI server resolves links against its own origin."""
    client = SimpleNamespace()
    _attach_info(
        client,
        _dashboard_info(dashboard_url=None, ui_version="1.0.0"),
        base_url="http://localhost:8000/",
    )

    resolved, warnings = await links.resolve_dashboard_links(
        client,
        lambda base: {"dashboard": f"{base}/jobs/1"},
        warning="link lookup failed",
    )

    assert resolved == {"dashboard": "http://localhost:8000/jobs/1"}
    assert warnings == []


async def test_resolve_dashboard_links_returns_empty_for_api_only_server() -> None:
    """A server with no dashboard or UI produces no links and no warnings."""
    client = SimpleNamespace()
    _attach_info(client, _dashboard_info(dashboard_url=None, ui_version=None))

    resolved, warnings = await links.resolve_dashboard_links(
        client,
        lambda base: {"dashboard": f"{base}/jobs/1"},
        warning="link lookup failed",
    )

    assert resolved == {}
    assert warnings == []


@pytest.mark.parametrize(
    "error_or_url",
    [
        APIError(503, "unavailable"),
        httpx.ConnectError("refused"),
        TimeoutError("timed out"),
        "javascript:alert(1)",
        "https://user:pass@cloud.example.com",
        "https://cloud.example.com?token=secret",
    ],
    ids=[
        "api_error",
        "connect_error",
        "timeout",
        "javascript_scheme",
        "embedded_credentials",
        "query_string",
    ],
)
async def test_resolve_dashboard_links_warns_without_failing(
    error_or_url: Exception | str,
) -> None:
    """Failed lookups and unsafe dashboard URLs warn and leave links empty."""
    client = SimpleNamespace()
    if isinstance(error_or_url, Exception):
        _attach_info(client, error=error_or_url)
    else:
        _attach_info(client, _dashboard_info(dashboard_url=error_or_url))

    resolved, warnings = await links.resolve_dashboard_links(
        client,
        lambda base: {"dashboard": f"{base}/jobs/1"},
        warning="link lookup failed",
    )

    assert resolved == {}
    assert warnings == ["link lookup failed"]


async def test_session_get_returns_dashboard_link() -> None:
    """Session get links to the session page."""
    session_id = uuid.uuid4()
    session_item = SimpleNamespace(
        id=session_id,
        model_dump=lambda *, mode: {"id": str(session_id)},
    )
    client = SimpleNamespace(
        sessions=SimpleNamespace(get=lambda _id: _async_return(session_item))
    )
    _attach_info(client)

    result = await sessions.get_session(client, session_id)

    assert result.links == {"dashboard": f"{DASHBOARD_BASE}/sessions/{session_id}"}
    assert result.warnings == []


async def test_session_import_file_links_sessions_and_job_with_one_info_lookup(
    tmp_path: Path,
) -> None:
    """File import returns sessions and job links and reuses its server info read."""
    from tests.cli.test_sessions import StubImportClient

    payload = tmp_path / "traces.jsonl"
    payload.write_bytes(b"{}\n")
    client = StubImportClient()
    info_stub = _attach_info(client, _dashboard_info(max_blob_size_bytes=1024))

    result = await sessions.import_sessions(
        client,
        payload,
        importer="jsonl@latest",
        agent="assistant@latest",
        params=None,
        media_type=None,
        wait=False,
        interval=None,
        timeout=None,
    )

    assert info_stub.calls == 1
    assert result.links == {
        "sessions": (
            f"{DASHBOARD_BASE}/agents/{client.agent.id}/sessions"
            f"?version={client.agent_version.id}"
        ),
        "job": f"{DASHBOARD_BASE}/jobs/{client.job.id}",
    }
    assert result.warnings == []


async def test_session_import_api_fetches_info_for_links() -> None:
    """API import without a local file fetches server info once for links."""
    from tests.cli.test_sessions import StubImportClient

    client = StubImportClient()
    info_stub = _attach_info(client)

    result = await sessions.import_sessions(
        client,
        None,
        importer="jsonl@latest",
        agent="assistant@latest",
        params=None,
        media_type=None,
        since="7d",
        wait=False,
        interval=None,
        timeout=None,
    )

    assert info_stub.calls == 1
    assert result.links == {
        "sessions": (
            f"{DASHBOARD_BASE}/agents/{client.agent.id}/sessions"
            f"?version={client.agent_version.id}"
        ),
        "job": f"{DASHBOARD_BASE}/jobs/{client.job.id}",
    }
    assert result.warnings == []


async def test_session_import_wait_returns_sessions_and_job_links(
    tmp_path: Path,
) -> None:
    """Waited session import keeps dashboard links on its terminal receipt."""
    from tests.cli.test_sessions import StubImportClient

    payload = tmp_path / "traces.jsonl"
    payload.write_bytes(b"{}\n")
    now = datetime(2026, 8, 3, tzinfo=UTC)

    class _WaitedImportClient(StubImportClient):
        def __init__(self) -> None:
            super().__init__()
            self.job = JobResponse(
                id=self.job.id,
                owner_id=self.job.owner_id,
                created=now,
                updated=now,
                kind=JobKind.IMPORT,
                status=JobStatus.COMPLETED,
                started_at=now,
                ended_at=now,
                error=None,
            )
            self.import_response = self.import_response.model_copy(
                update={"job_id": self.job.id}
            )
            importer_task = TaskResponse(
                id=uuid.uuid4(),
                job_id=self.job.id,
                kind=TaskKind.IMPORTER,
                status=TaskStatus.COMPLETED,
                on_failure=TaskOnFailure.ABORT,
                attempt=1,
                labels={},
                error=None,
                result=ImportStats(created=2, skipped=0, failed=0).model_dump(
                    mode="json"
                ),
                created=now,
                updated=now,
            )

            async def _get_job(_job_id: uuid.UUID) -> JobResponse:
                return self.job

            async def _iter_tasks(_job_id: uuid.UUID):
                yield importer_task

            self.jobs: Any = SimpleNamespace(get=_get_job, iter_tasks=_iter_tasks)
            self.tasks = EmptyTaskDiagnostics()

    client = _WaitedImportClient()
    info_stub = _attach_info(client)

    token = set_output_context(
        OutputContext(
            command="session.import",
            mode="json",
            debug=False,
            traceback=False,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            rich=False,
        )
    )
    try:
        result = await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@latest",
            agent="assistant@latest",
            params=None,
            media_type=None,
            wait=True,
            interval=0.01,
            timeout=5.0,
        )
    finally:
        reset_output_context(token)

    assert info_stub.calls == 1
    assert result.links == {
        "sessions": (
            f"{DASHBOARD_BASE}/agents/{client.agent.id}/sessions"
            f"?version={client.agent_version.id}"
        ),
        "job": f"{DASHBOARD_BASE}/jobs/{client.job.id}",
    }


async def test_import_get_links_sessions_and_optional_job() -> None:
    """Import get links the agent sessions page and includes job when present."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    agent_id = uuid.uuid4()
    agent_version_id = uuid.uuid4()
    job_id = uuid.uuid4()
    with_job = ImportResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        job_id=job_id,
        agent_id=agent_id,
        agent_version_id=agent_version_id,
        importer_version_id=None,
        source=BlobImportSource(blob_id=uuid.uuid4()),
        params={},
        evaluators=[],
        analyzers=[],
        created=now,
        updated=now,
    )
    without_job = with_job.model_copy(update={"job_id": None, "agent_version_id": None})

    client = SimpleNamespace(
        imports=SimpleNamespace(get=lambda _id: _async_return(with_job))
    )
    _attach_info(client)
    first = await imports.get_import(client, with_job.id)
    assert first.links == {
        "sessions": (
            f"{DASHBOARD_BASE}/agents/{agent_id}/sessions?version={agent_version_id}"
        ),
        "job": f"{DASHBOARD_BASE}/jobs/{job_id}",
    }

    client.imports = SimpleNamespace(get=lambda _id: _async_return(without_job))
    second = await imports.get_import(client, without_job.id)
    assert second.links == {"sessions": f"{DASHBOARD_BASE}/agents/{agent_id}/sessions"}


async def test_experiment_commands_link_to_experiment_page() -> None:
    """Experiment create, get, and update all link to the experiment page."""
    from tests.cli.test_experiments import StubExperimentClient

    client = StubExperimentClient()
    _attach_info(client)
    expected = {"dashboard": f"{DASHBOARD_BASE}/experiments/{client.experiment.id}"}

    created = await experiments.create_experiment(
        client,
        "regression",
        agent="assistant",
        description=None,
        override=None,
        tool_policy=None,
        evaluators=["quality@2"],
        evaluator_params=None,
    )
    fetched = await experiments.get_experiment(client, str(client.experiment.id))
    updated = await experiments.update_experiment(
        client,
        str(client.experiment.id),
        name="renamed",
        description=None,
        clear_description=False,
        override=None,
        clear_override=False,
        tool_policy=None,
        evaluators=None,
        evaluator_params=None,
    )

    assert created.links == expected
    assert fetched.links == expected
    assert updated.links == expected


async def test_experiment_run_commands_link_to_selected_run_number() -> None:
    """Experiment run start, get, watch, and cancel link by run number."""
    client = StubRunsClient()
    _attach_info(client)
    expected = {
        "dashboard": (
            f"{DASHBOARD_BASE}/experiments/{client.experiment.id}"
            f"?run={client.created_run.number}"
        )
    }

    started = await experiment_runs.start_run(
        client,
        "regression",
        cohort_version_id=client.cohort_version.id,
        agent_reference="candidate@latest",
        baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
        wait=False,
        interval=None,
        timeout=None,
    )
    fetched = await experiment_runs.get_run(client, client.run_id)
    canceled = await experiment_runs.cancel_run(client, client.run_id)

    completed = build_run(
        client.run_id,
        ExperimentRunStatus.COMPLETED,
        experiment_id=client.experiment.id,
        cohort_version_id=client.cohort_version.id,
        agent_version_id=client.agent_version.id,
        completed=1,
    )
    client.get_responses = [completed]
    client.get_calls = 0

    token = set_output_context(
        OutputContext(
            command="experiment.run.watch",
            mode="json",
            debug=False,
            traceback=False,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            rich=False,
        )
    )
    try:
        watched = await experiment_runs.watch_run(
            client, client.run_id, interval=0.01, timeout=5.0
        )
        client.get_calls = 0
        waited = await experiment_runs.start_run(
            client,
            "regression",
            cohort_version_id=client.cohort_version.id,
            agent_reference="candidate@latest",
            baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
            wait=True,
            interval=0.01,
            timeout=5.0,
        )
    finally:
        reset_output_context(token)

    assert started.links == expected
    assert fetched.links == expected
    assert canceled.links == expected
    assert watched.links == expected
    assert waited.links == expected


async def test_cohort_and_cohort_version_commands_link_to_cohort_page() -> None:
    """Cohort and cohort-version commands link to the agent-scoped cohort page."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    agent_id = uuid.uuid4()
    cohort_id = uuid.uuid4()
    version_id = uuid.uuid4()
    cohort = CohortResponse(
        id=cohort_id,
        owner_id=uuid.uuid4(),
        name="regression",
        description=None,
        agent_id=agent_id,
        metadata={},
        latest_version=1,
        created=now,
        updated=now,
    )
    version = CohortVersionResponse(
        id=version_id,
        owner_id=cohort.owner_id,
        cohort_id=cohort_id,
        version=1,
        display_version=None,
        session_count=1,
        created=now,
        updated=now,
    )

    class _Client:
        def __init__(self) -> None:
            self.agents = SimpleNamespace(
                get=lambda _id: _async_return(SimpleNamespace(id=agent_id, name="a")),
            )
            self.cohorts = SimpleNamespace(
                create=lambda _req, idempotency_key=None: _async_return(cohort),
                get=lambda _id: _async_return(cohort),
                update=lambda _id, _req: _async_return(cohort),
                create_version=lambda _id, _req, idempotency_key=None: _async_return(
                    version
                ),
            )
            self.cohort_versions = SimpleNamespace(
                get=lambda _id: _async_return(version),
                update=lambda _id, _req: _async_return(version),
            )

    client = _Client()
    _attach_info(client)
    expected = {"dashboard": f"{DASHBOARD_BASE}/agents/{agent_id}/cohorts/{cohort_id}"}

    created_empty = await cohorts.create_cohort(
        client,
        "regression",
        agent=str(agent_id),
        description=None,
        metadata=None,
    )
    created_with_sessions = await cohorts.create_cohort(
        client,
        "regression",
        agent=str(agent_id),
        description=None,
        metadata=None,
        sessions=[str(uuid.uuid4())],
    )
    fetched = await cohorts.get_cohort(client, str(cohort_id))
    updated = await cohorts.update_cohort(
        client,
        str(cohort_id),
        name="renamed",
        description=None,
        clear_description=False,
        metadata=None,
    )
    created_version = await cohorts.create_cohort_version(
        client,
        str(cohort_id),
        add_session_ids=[uuid.uuid4()],
        remove_session_ids=None,
        display_version=None,
    )
    fetched_version = await cohorts.get_cohort_version_result(client, str(version_id))
    updated_version = await cohorts.update_cohort_version(
        client,
        str(version_id),
        display_version="v1",
        clear_display_version=False,
    )

    assert created_empty.links == expected
    assert created_with_sessions.links == expected
    assert fetched.links == expected
    assert updated.links == expected
    assert created_version.links == expected
    assert fetched_version.links == expected
    assert updated_version.links == expected


async def test_investigation_get_and_update_return_review_link() -> None:
    """Investigation get and update preserve the review link key."""
    from tests.cli.test_investigations import StubInvestigationClient

    client = StubInvestigationClient()
    client.server_info = _dashboard_info()
    expected = {
        "review": (
            f"{DASHBOARD_BASE}/agents/{client.agent.id}"
            f"/investigations/{client.investigation.id}/review"
        )
    }

    fetched = await investigations.get_investigation(client, client.investigation.id)
    updated = await investigations.update_investigation(
        client,
        client.investigation.id,
        name="renamed",
        description=None,
        clear_description=False,
        status=None,
    )

    assert fetched.links == expected
    assert updated.links == expected


async def test_job_commands_return_dashboard_link() -> None:
    """Job get, watch, and cancel return the job dashboard link."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    job_id = uuid.uuid4()
    completed_job = JobResponse(
        id=job_id,
        owner_id=uuid.uuid4(),
        created=now,
        updated=now,
        kind=JobKind.IMPORT,
        status=JobStatus.COMPLETED,
        started_at=now,
        ended_at=now,
        error=None,
    )
    client = SimpleNamespace(
        tasks=EmptyTaskDiagnostics(),
        jobs=SimpleNamespace(
            get=lambda _id: _async_return(completed_job),
            cancel=lambda _id: _async_return(completed_job),
        ),
    )
    _attach_info(client)
    expected = {"dashboard": f"{DASHBOARD_BASE}/jobs/{job_id}"}

    fetched = await jobs.get_job(client, job_id, tasks=False)
    canceled = await jobs.cancel_job(client, job_id)

    token = set_output_context(
        OutputContext(
            command="job.watch",
            mode="json",
            debug=False,
            traceback=False,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            rich=False,
        )
    )
    try:
        watched = await jobs.watch_job(client, job_id, interval=0.01, timeout=5.0)
    finally:
        reset_output_context(token)

    assert fetched.links == expected
    assert canceled.links == expected
    assert watched.links == expected


def test_cli_json_output_emits_dashboard_links(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Running a command with -o json includes the populated links object."""
    client = StubRunsClient()
    _attach_info(client)

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)

    code = app_module.main(
        ["experiment", "run", "get", str(client.run_id), "-o", "json"]
    )

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["links"] == {
        "dashboard": (
            f"{DASHBOARD_BASE}/experiments/{client.experiment.id}"
            f"?run={client.created_run.number}"
        )
    }


async def _async_return(value: Any) -> Any:
    return value
