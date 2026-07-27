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
"""Tests for the scorer contract and the score flow."""

import json
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
    RegistryScorerConfig,
    SourceScorerConfig,
)
from kitaru.api_models.v1.plugins import PluginFormat
from kitaru.api_models.v1.session_nodes import SessionNodeResponse
from kitaru.api_models.v1.sessions import SessionOrigin, SessionResponse, SessionStatus
from kitaru.client.api_client import KitaruAPIClient
from kitaru.job.scorer import ScoringError, SessionView, call_scorer, load_scorer, run

NOW = datetime.now(UTC)

PLUGIN_CODE = """
def score(session, factor=1.0):
    return 0.5 * factor


NOT_CALLABLE = "text"
"""


def constant_scorer(session: SessionView, value: float = 1.0) -> float:
    """Return a configured constant score."""
    return value


def raising_scorer(session: SessionView) -> float:
    """Raise an error."""
    raise RuntimeError("boom")


def string_scorer(session: SessionView) -> Any:
    """Return a non-numeric score."""
    return "high"


def boolean_scorer(session: SessionView) -> Any:
    """Return a boolean score."""
    return True


NOT_CALLABLE = "not callable"


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


def make_view() -> SessionView:
    """Build a session view around a minimal completed session."""
    return SessionView(session=make_session(uuid.uuid4()), nodes=[])


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
            name="quality", source="test_job_scorer:constant_scorer", params=params
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


class FakeSessionsResource:
    """Fake sessions resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get(self, session_id: uuid.UUID) -> SessionResponse:
        """Record the request and return a completed session."""
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
    """Fake API client implementing the resource methods the flow uses."""

    def __init__(self, spec: JobSpecResponse) -> None:
        """Initialize the client."""
        self.spec = spec
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


def test_load_scorer() -> None:
    """Import the referenced function."""
    assert load_scorer("test_job_scorer:constant_scorer") is constant_scorer


@pytest.mark.parametrize("source", ["noseparator", ":attribute", "module:"])
def test_load_scorer_malformed_source(source: str) -> None:
    """Reject sources that are not 'module:attribute'."""
    with pytest.raises(ScoringError, match="expected 'module:attribute'"):
        load_scorer(source)


def test_load_scorer_missing_module() -> None:
    """Reject a module that does not import."""
    with pytest.raises(ScoringError, match="Failed to import scorer module"):
        load_scorer("kitaru_missing_module:scorer")


def test_load_scorer_missing_attribute() -> None:
    """Reject a missing attribute."""
    with pytest.raises(ScoringError, match="has no attribute"):
        load_scorer("test_job_scorer:missing_scorer")


def test_load_scorer_not_callable() -> None:
    """Reject a non-callable attribute."""
    with pytest.raises(ScoringError, match="is not callable"):
        load_scorer("test_job_scorer:NOT_CALLABLE")


def test_call_scorer_passes_params() -> None:
    """Call the scorer with the session view and configured params."""
    assert call_scorer("quality", constant_scorer, make_view(), {"value": 0.3}) == 0.3


@pytest.mark.parametrize("value", [-0.1, 1.5])
def test_call_scorer_out_of_range(value: float) -> None:
    """Reject scores outside 0..1."""
    with pytest.raises(ScoringError, match=r"expected a value in 0\.\.1"):
        call_scorer("quality", constant_scorer, make_view(), {"value": value})


def test_call_scorer_non_numeric() -> None:
    """Reject a non-numeric score."""
    with pytest.raises(ScoringError, match=r"expected a float in 0\.\.1"):
        call_scorer("quality", string_scorer, make_view(), {})


def test_call_scorer_boolean() -> None:
    """Reject a boolean score."""
    with pytest.raises(ScoringError, match=r"expected a float in 0\.\.1"):
        call_scorer("quality", boolean_scorer, make_view(), {})


def test_call_scorer_exception_propagates() -> None:
    """Wrap a raising scorer in a ScoringError naming the scorer."""
    with pytest.raises(ScoringError, match="'quality' raised RuntimeError: boom"):
        call_scorer("quality", raising_scorer, make_view(), {})


async def test_run_source_arm_writes_the_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Score the input session with payloads and write the score, making no writes."""
    job_id = uuid.uuid4()
    session_id = uuid.uuid4()
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_JOB_RESULT_PATH", str(result_path))
    fake = FakeClient(make_spec(job_id, source_scorer(session_id, {"value": 0.25})))

    await run(cast(KitaruAPIClient, fake), job_id)

    assert fake.session_requests == [session_id]
    assert fake.node_requests == [(session_id, True)]
    assert json.loads(result_path.read_text()) == 0.25


async def test_run_registry_arm_loads_the_materialized_plugin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Load registered code from the path the worker materialized."""
    job_id = uuid.uuid4()
    session_id = uuid.uuid4()
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", str(write_plugin(tmp_path)))
    monkeypatch.setenv("KITARU_JOB_RESULT_PATH", str(result_path))
    fake = FakeClient(make_spec(job_id, plugin_scorer(session_id)))

    await run(cast(KitaruAPIClient, fake), job_id)

    assert json.loads(result_path.read_text()) == 0.5


async def test_run_registry_arm_without_plugin_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a registry scorer without a materialized code path."""
    job_id = uuid.uuid4()
    monkeypatch.delenv("KITARU_JOB_PLUGIN_PATH", raising=False)
    fake = FakeClient(make_spec(job_id, plugin_scorer(uuid.uuid4())))
    with pytest.raises(ScoringError, match="KITARU_JOB_PLUGIN_PATH is not set"):
        await run(cast(KitaruAPIClient, fake), job_id)


async def test_run_rejects_another_kind() -> None:
    """Reject a job spec without a scorer."""
    job_id = uuid.uuid4()
    fake = FakeClient(make_spec(job_id, None))
    with pytest.raises(ScoringError, match="is not a score job"):
        await run(cast(KitaruAPIClient, fake), job_id)


async def test_run_propagates_a_scorer_error_without_writing_a_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Propagate an invalid score without writing the result file."""
    job_id = uuid.uuid4()
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_JOB_RESULT_PATH", str(result_path))
    fake = FakeClient(make_spec(job_id, source_scorer(uuid.uuid4(), {"value": 1.5})))

    with pytest.raises(ScoringError, match=r"expected a value in 0\.\.1"):
        await run(cast(KitaruAPIClient, fake), job_id)
    assert not result_path.exists()
