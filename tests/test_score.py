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
"""Tests for the score job harness."""

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from kitaru.api_models.v1.jobs import (
    JobKind,
    JobSpecPlugin,
    JobSpecResponse,
    JobSpecScorer,
    JobUpdateRequest,
    RegistryScorerConfig,
    SourceScorerConfig,
)
from kitaru.api_models.v1.plugins import PluginFormat
from kitaru.api_models.v1.session_nodes import SessionNodeResponse
from kitaru.api_models.v1.sessions import (
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.plugin_loader import required_env
from kitaru.score import (
    load_plugin_scorer,
    resolve_scorer,
    score_job,
)
from kitaru.scoring import ScoringError, SessionView

NOW = datetime.now(UTC)

PLUGIN_CODE = """
def score(session, factor=1.0):
    return 0.5 * factor


NOT_CALLABLE = "text"
"""


def constant_scorer(session: SessionView, value: float = 1.0) -> float:
    """Return a configured constant score."""
    return value


def make_session(session_id: uuid.UUID) -> SessionResponse:
    """Build a completed session."""
    return SessionResponse(
        id=session_id,
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        name=None,
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id=None,
        metadata={},
        provider=None,
        framework=None,
        adapter_version=None,
        log_uri=None,
        scores={},
        cost=None,
        tokens=None,
        llm_call_count=0,
        tool_call_count=0,
        created=NOW,
        updated=NOW,
    )


def make_spec(job_id: uuid.UUID, scorer: JobSpecScorer | None) -> JobSpecResponse:
    """Build a score job spec."""
    return JobSpecResponse(
        job_id=job_id,
        kind=JobKind.SCORE,
        inputs=None,
        override=None,
        tool_policy=None,
        scorer=scorer,
        importer=None,
        run=None,
        secret_env={},
        input_session_id=scorer.input_session_id if scorer else None,
        name=None,
    )


def source_scorer(session_id: uuid.UUID, params: dict[str, Any]) -> JobSpecScorer:
    """Build the scorer of a source arm score job."""
    return JobSpecScorer(
        config=SourceScorerConfig(
            name="quality", source="test_score:constant_scorer", params=params
        ),
        plugin=None,
        input_session_id=session_id,
    )


def plugin_scorer(session_id: uuid.UUID, entrypoint: str = "score") -> JobSpecScorer:
    """Build the scorer of a registry arm score job."""
    return JobSpecScorer(
        config=RegistryScorerConfig(name="quality", version=1),
        plugin=JobSpecPlugin(
            format=PluginFormat.INLINE,
            entrypoint=entrypoint,
            blob_id=uuid.uuid4(),
            sha256="0" * 64,
        ),
        input_session_id=session_id,
    )


class FakeJobsResource:
    """Fake jobs resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get_spec(self, job_id: uuid.UUID) -> JobSpecResponse:
        """Return the configured spec."""
        return self._client.spec

    async def update(self, job_id: uuid.UUID, request: JobUpdateRequest) -> None:
        """Record the update."""
        self._client.updates.append(request)


class FakeSessionsResource:
    """Fake sessions resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get(self, session_id: uuid.UUID) -> SessionResponse:
        """Return a completed session."""
        self._client.session_requests.append(session_id)
        return make_session(session_id)


class FakeSessionNodesResource:
    """Fake session nodes resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def list(
        self, session_id: uuid.UUID, include_payloads: bool = False
    ) -> list[SessionNodeResponse]:
        """Record the request and return no nodes."""
        self._client.node_requests.append((session_id, include_payloads))
        return []


class FakeClient:
    """Fake API client implementing the resource methods the harness uses."""

    def __init__(self, spec: JobSpecResponse) -> None:
        """Initialize the client."""
        self.spec = spec
        self.updates: list[JobUpdateRequest] = []
        self.session_requests: list[uuid.UUID] = []
        self.node_requests: list[tuple[uuid.UUID, bool]] = []
        self.jobs = FakeJobsResource(self)
        self.sessions = FakeSessionsResource(self)
        self.session_nodes = FakeSessionNodesResource(self)


def write_plugin(tmp_path: Path, code: str = PLUGIN_CODE) -> Path:
    """Write scorer code to a file without a suffix, as the cache does."""
    path = tmp_path / ("a" * 64)
    path.write_text(code)
    return path


def test_required_env_reads_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return the value of a set variable."""
    monkeypatch.setenv("KITARU_JOB_ID", "job")
    assert required_env("KITARU_JOB_ID", ScoringError) == "job"


def test_required_env_rejects_a_missing_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a variable that is not set."""
    monkeypatch.delenv("KITARU_JOB_PLUGIN_PATH", raising=False)
    with pytest.raises(ScoringError, match="KITARU_JOB_PLUGIN_PATH is not set"):
        required_env("KITARU_JOB_PLUGIN_PATH", ScoringError)


def test_load_plugin_scorer_imports_a_suffixless_file(tmp_path: Path) -> None:
    """Import the entrypoint of a cached code file."""
    scorer = load_plugin_scorer(write_plugin(tmp_path), "score")
    assert scorer(None, factor=2.0) == 1.0


def test_load_plugin_scorer_missing_attribute(tmp_path: Path) -> None:
    """Reject an entrypoint the code does not define."""
    with pytest.raises(ScoringError, match="has no attribute 'missing'"):
        load_plugin_scorer(write_plugin(tmp_path), "missing")


def test_load_plugin_scorer_not_callable(tmp_path: Path) -> None:
    """Reject an entrypoint that is not callable."""
    with pytest.raises(ScoringError, match="is not callable"):
        load_plugin_scorer(write_plugin(tmp_path), "NOT_CALLABLE")


def test_load_plugin_scorer_import_error(tmp_path: Path) -> None:
    """Reject code that raises while importing."""
    path = write_plugin(tmp_path, "raise RuntimeError('boom')\n")
    with pytest.raises(ScoringError, match="RuntimeError: boom"):
        load_plugin_scorer(path, "score")


def test_load_plugin_scorer_missing_file(tmp_path: Path) -> None:
    """Reject a code file that does not exist."""
    with pytest.raises(ScoringError, match="Failed to import scorer code"):
        load_plugin_scorer(tmp_path / "absent", "score")


def test_resolve_scorer_registry_arm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Load registered code from the path the worker materialized."""
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", str(write_plugin(tmp_path)))
    scorer = resolve_scorer(plugin_scorer(uuid.uuid4()))
    assert scorer(None) == 0.5


def test_resolve_scorer_registry_arm_without_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a registry scorer without a materialized code path."""
    monkeypatch.delenv("KITARU_JOB_PLUGIN_PATH", raising=False)
    with pytest.raises(ScoringError, match="KITARU_JOB_PLUGIN_PATH is not set"):
        resolve_scorer(plugin_scorer(uuid.uuid4()))


def test_resolve_scorer_source_arm() -> None:
    """Resolve a source reference against the ambient environment."""
    scorer = resolve_scorer(source_scorer(uuid.uuid4(), {}))
    assert scorer is constant_scorer


async def test_score_job_records_the_score() -> None:
    """Score the input session with payloads and patch the score."""
    job_id = uuid.uuid4()
    session_id = uuid.uuid4()
    fake = FakeClient(make_spec(job_id, source_scorer(session_id, {"value": 0.25})))

    score = await score_job(cast(KitaruAPIClient, fake), job_id)

    assert score == 0.25
    assert fake.session_requests == [session_id]
    assert fake.node_requests == [(session_id, True)]
    assert [update.score for update in fake.updates] == [0.25]
    assert fake.updates[0].status is None


async def test_score_job_rejects_another_kind() -> None:
    """Reject a job spec without a scorer."""
    job_id = uuid.uuid4()
    fake = FakeClient(make_spec(job_id, None))
    with pytest.raises(ScoringError, match="is not a score job"):
        await score_job(cast(KitaruAPIClient, fake), job_id)


async def test_score_job_propagates_a_scorer_error() -> None:
    """Propagate an invalid score without patching the job."""
    job_id = uuid.uuid4()
    fake = FakeClient(
        make_spec(job_id, source_scorer(uuid.uuid4(), {"value": 1.5})),
    )
    with pytest.raises(ScoringError, match=r"expected a value in 0\.\.1"):
        await score_job(cast(KitaruAPIClient, fake), job_id)
    assert fake.updates == []
