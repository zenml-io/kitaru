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
"""Tests for the replay runner against a fake API client."""

import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any

import pytest

from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunProgress,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.replays import (
    PassthroughPolicy,
    ReplayClaimRequest,
    ReplayClaimResponse,
    ReplayHeartbeatResponse,
    ReplayResponse,
    ReplaySpecResponse,
    ReplaySpecRun,
    ReplayStatus,
    ReplayUpdateRequest,
    ScorerConfig,
    ScoringPolicy,
    StandaloneReplayClaimRequest,
    ToolPolicyConfig,
)
from kitaru.api_models.v1.session_nodes import SessionNodeResponse
from kitaru.api_models.v1.sessions import (
    SessionOrigin,
    SessionResponse,
    SessionScoresRequest,
    SessionStatus,
)
from kitaru.runner import Runner
from kitaru.scoring import SessionView

NOW = datetime.now(UTC)

SCORING_POLICY = ScoringPolicy(
    scorers=[
        ScorerConfig(
            name="quality",
            source="test_runner:constant_scorer",
            params={"value": 0.8},
        )
    ],
    pass_threshold=0.5,
)


def constant_scorer(session: SessionView, value: float = 1.0) -> float:
    """Return a configured constant score."""
    return value


def raising_scorer(session: SessionView) -> float:
    """Raise an error."""
    raise RuntimeError("boom")


def make_spec(
    replay_id: uuid.UUID,
    command: str = "true",
    inputs: Any = None,
    run_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
    timeout_seconds: int = 60,
    score_baselines: bool = False,
    scoring_policy: ScoringPolicy = SCORING_POLICY,
    original_session_id: uuid.UUID | None = None,
) -> ReplaySpecResponse:
    """Build a replay spec."""
    return ReplaySpecResponse(
        replay_id=replay_id,
        inputs=inputs,
        override=None,
        tool_policy=ToolPolicyConfig(default=PassthroughPolicy()),
        scoring_policy=scoring_policy,
        score_baselines=score_baselines,
        run=ReplaySpecRun(
            command=command,
            working_dir=None,
            env=run_env or {},
            timeout_seconds=timeout_seconds,
        ),
        secret_env=secret_env or {},
        original_session_id=original_session_id or uuid.uuid4(),
    )


def make_replay(
    replay_id: uuid.UUID,
    status: ReplayStatus = ReplayStatus.RUNNING,
    result_session_id: uuid.UUID | None = None,
) -> ReplayResponse:
    """Build a replay."""
    return ReplayResponse(
        id=replay_id,
        experiment_run_id=None,
        agent_version_id=uuid.uuid4(),
        original_session_id=uuid.uuid4(),
        result_session_id=result_session_id,
        status=status,
        attempt=1,
        worker_id=None,
        claimed_at=None,
        heartbeat_at=None,
        started_at=None,
        ended_at=None,
        error=None,
        passed=None,
        score=None,
        scores=None,
        diff=None,
        override=None,
        tool_policy=ToolPolicyConfig(default=PassthroughPolicy()),
        scoring_policy=SCORING_POLICY,
        created=NOW,
        updated=NOW,
    )


def make_session(
    session_id: uuid.UUID,
    status: SessionStatus = SessionStatus.COMPLETED,
    scores: dict[str, float] | None = None,
) -> SessionResponse:
    """Build a session."""
    return SessionResponse(
        id=session_id,
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        origin=SessionOrigin.REPLAY,
        status=status,
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
        scores=scores or {},
        cost=None,
        tokens=None,
        llm_call_count=0,
        tool_call_count=0,
        created=NOW,
        updated=NOW,
    )


def make_run(status: ExperimentRunStatus) -> ExperimentRunResponse:
    """Build an experiment run."""
    return ExperimentRunResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        experiment_id=uuid.uuid4(),
        number=1,
        status=status,
        agent_version_id=uuid.uuid4(),
        score_baselines=False,
        started_at=None,
        ended_at=None,
        summary=None,
        error=None,
        progress=ExperimentRunProgress(
            pending=0,
            claimed=0,
            running=0,
            completed=1,
            failed=0,
            timed_out=0,
            canceled=0,
            total=1,
        ),
        created=NOW,
        updated=NOW,
    )


class FakeReplaysResource:
    """Fake replays resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get_spec(self, replay_id: uuid.UUID) -> ReplaySpecResponse:
        """Return the configured spec."""
        return self._client.spec

    async def get(self, replay_id: uuid.UUID) -> ReplayResponse:
        """Return the configured replay."""
        return self._client.replay

    async def update(
        self, replay_id: uuid.UUID, request: ReplayUpdateRequest
    ) -> ReplayResponse:
        """Record the update and return the replay in the target status."""
        self._client.updates.append(request)
        return self._client.replay.model_copy(update={"status": request.status})

    async def claim(
        self, replay_id: uuid.UUID, request: StandaloneReplayClaimRequest
    ) -> ReplayResponse:
        """Record the claim and return the configured replay."""
        self._client.standalone_claims.append(request)
        return self._client.replay.model_copy(update={"status": ReplayStatus.CLAIMED})

    async def heartbeat(self, replay_id: uuid.UUID) -> ReplayHeartbeatResponse:
        """Count the heartbeat and report the configured cancellation."""
        self._client.heartbeat_count += 1
        return ReplayHeartbeatResponse(
            status=self._client.replay.status,
            canceled=self._client.cancel_on_heartbeat,
        )


class FakeSessionsResource:
    """Fake sessions resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get(self, session_id: uuid.UUID) -> SessionResponse:
        """Return the configured session."""
        return self._client.sessions_by_id[session_id]

    async def merge_scores(
        self, session_id: uuid.UUID, request: SessionScoresRequest
    ) -> SessionResponse:
        """Record the merged scores."""
        self._client.merged_scores.append((session_id, request.scores))
        return self._client.sessions_by_id[session_id]


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


class FakeExperimentRunsResource:
    """Fake experiment runs resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def claim(
        self, run_id: uuid.UUID, request: ReplayClaimRequest
    ) -> ReplayClaimResponse:
        """Record the claim and pop the next configured batch."""
        self._client.claim_requests.append(request)
        batches = self._client.claim_batches
        return ReplayClaimResponse(replays=batches.pop(0) if batches else [])

    async def get(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        """Return the configured run."""
        return self._client.run


class FakeClient:
    """Fake API client implementing the resource methods the runner uses."""

    def __init__(
        self,
        spec: ReplaySpecResponse,
        replay: ReplayResponse,
        sessions_by_id: dict[uuid.UUID, SessionResponse] | None = None,
        claim_batches: list[list[ReplayResponse]] | None = None,
        run: ExperimentRunResponse | None = None,
        cancel_on_heartbeat: bool = False,
    ) -> None:
        """Initialize the client."""
        self.spec = spec
        self.replay = replay
        self.sessions_by_id = sessions_by_id or {}
        self.claim_batches = claim_batches or []
        self.run = run or make_run(ExperimentRunStatus.COMPLETED)
        self.cancel_on_heartbeat = cancel_on_heartbeat
        self.updates: list[ReplayUpdateRequest] = []
        self.merged_scores: list[tuple[uuid.UUID, dict[str, float]]] = []
        self.node_requests: list[tuple[uuid.UUID, bool]] = []
        self.claim_requests: list[ReplayClaimRequest] = []
        self.standalone_claims: list[StandaloneReplayClaimRequest] = []
        self.heartbeat_count = 0
        self.replays = FakeReplaysResource(self)
        self.sessions = FakeSessionsResource(self)
        self.session_nodes = FakeSessionNodesResource(self)
        self.experiment_runs = FakeExperimentRunsResource(self)

    async def __aenter__(self) -> "FakeClient":
        """Enter the context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager."""


def make_runner(
    monkeypatch: pytest.MonkeyPatch, fake: FakeClient, **kwargs: Any
) -> Runner:
    """Build a runner backed by the fake client."""
    monkeypatch.setattr("kitaru.runner.KitaruAPIClient", lambda base_url, api_key: fake)
    return Runner(api_url="http://server", api_key="key", **kwargs)


async def test_success_flow_completes_and_scores_baselines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Complete a replay with scores and post missing baseline scores."""
    replay_id = uuid.uuid4()
    result_id = uuid.uuid4()
    original_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(
            replay_id,
            command="true",
            score_baselines=True,
            original_session_id=original_id,
        ),
        replay=make_replay(replay_id, result_session_id=result_id),
        sessions_by_id={
            result_id: make_session(result_id),
            original_id: make_session(original_id),
        },
    )
    runner = make_runner(monkeypatch, fake, worker_id="worker-1")
    final = await runner.run_replay(replay_id)

    assert len(fake.standalone_claims) == 1
    assert fake.standalone_claims[0].worker_id == "worker-1"
    assert [update.status for update in fake.updates] == [
        ReplayStatus.RUNNING,
        ReplayStatus.COMPLETED,
    ]
    completed = fake.updates[-1]
    assert completed.passed is True
    assert completed.score == pytest.approx(0.8)
    assert completed.scores == {"quality": 0.8}
    assert final.status is ReplayStatus.COMPLETED
    assert fake.merged_scores == [(original_id, {"quality": 0.8})]
    assert (result_id, True) in fake.node_requests
    assert (original_id, True) in fake.node_requests


async def test_baselines_skip_present_scores(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip baseline scoring when the original already has all scores."""
    replay_id = uuid.uuid4()
    result_id = uuid.uuid4()
    original_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(
            replay_id,
            command="true",
            score_baselines=True,
            original_session_id=original_id,
        ),
        replay=make_replay(replay_id, result_session_id=result_id),
        sessions_by_id={
            result_id: make_session(result_id),
            original_id: make_session(original_id, scores={"quality": 0.4}),
        },
    )
    runner = make_runner(monkeypatch, fake)
    await runner.run_replay(replay_id)

    assert fake.merged_scores == []
    assert fake.updates[-1].status is ReplayStatus.COMPLETED


async def test_nonzero_exit_fails_with_log_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail a replay whose agent process exits non-zero."""
    replay_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(replay_id, command="echo boom >&2 && exit 3"),
        replay=make_replay(replay_id),
    )
    runner = make_runner(monkeypatch, fake)
    final = await runner.run_replay(replay_id)

    assert [update.status for update in fake.updates] == [
        ReplayStatus.RUNNING,
        ReplayStatus.FAILED,
    ]
    failed = fake.updates[-1]
    assert failed.error is not None
    assert "exited with code 3" in failed.error
    assert "boom" in failed.error
    assert final.status is ReplayStatus.FAILED


async def test_timeout_kills_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Kill the agent process and time the replay out on expiry."""
    replay_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(replay_id, command="sleep 30", timeout_seconds=1),
        replay=make_replay(replay_id),
    )
    runner = make_runner(monkeypatch, fake)
    started = time.monotonic()
    await runner.run_replay(replay_id)

    assert time.monotonic() - started < 10
    assert [update.status for update in fake.updates] == [
        ReplayStatus.RUNNING,
        ReplayStatus.TIMED_OUT,
    ]
    error = fake.updates[-1].error
    assert error is not None
    assert "timed out after 1 seconds" in error


async def test_heartbeat_cancel_kills_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kill the agent process and cancel the replay on a canceled heartbeat."""
    replay_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(replay_id, command="sleep 30", timeout_seconds=30),
        replay=make_replay(replay_id),
        cancel_on_heartbeat=True,
    )
    runner = make_runner(monkeypatch, fake, heartbeat_interval=0.05)
    started = time.monotonic()
    await runner.run_replay(replay_id)

    assert time.monotonic() - started < 10
    assert fake.heartbeat_count >= 1
    assert [update.status for update in fake.updates] == [
        ReplayStatus.RUNNING,
        ReplayStatus.CANCELED,
    ]


async def test_missing_result_session_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail a replay whose agent recorded no result session."""
    replay_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(replay_id, command="true"),
        replay=make_replay(replay_id, result_session_id=None),
    )
    runner = make_runner(monkeypatch, fake)
    await runner.run_replay(replay_id)

    failed = fake.updates[-1]
    assert failed.status is ReplayStatus.FAILED
    assert failed.error is not None
    assert "without recording a result session" in failed.error


async def test_scorer_error_fails_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail a replay when a scorer raises."""
    replay_id = uuid.uuid4()
    result_id = uuid.uuid4()
    policy = ScoringPolicy(
        scorers=[ScorerConfig(name="quality", source="test_runner:raising_scorer")],
        pass_threshold=0.5,
    )
    fake = FakeClient(
        spec=make_spec(replay_id, command="true", scoring_policy=policy),
        replay=make_replay(replay_id, result_session_id=result_id),
        sessions_by_id={result_id: make_session(result_id)},
    )
    runner = make_runner(monkeypatch, fake)
    await runner.run_replay(replay_id)

    failed = fake.updates[-1]
    assert failed.status is ReplayStatus.FAILED
    assert failed.error is not None
    assert "'quality' raised RuntimeError: boom" in failed.error


async def run_env_dump(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    inputs: Any,
    run_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
) -> tuple[uuid.UUID, dict[str, str]]:
    """Execute a replay dumping its environment to a file."""
    monkeypatch.delenv("KITARU_INPUTS", raising=False)
    replay_id = uuid.uuid4()
    result_id = uuid.uuid4()
    out_file = tmp_path / "env.txt"
    fake = FakeClient(
        spec=make_spec(
            replay_id,
            command='env > "$KITARU_TEST_ENV_FILE"',
            inputs=inputs,
            run_env={**(run_env or {}), "KITARU_TEST_ENV_FILE": str(out_file)},
            secret_env=secret_env,
        ),
        replay=make_replay(replay_id, result_session_id=result_id),
        sessions_by_id={result_id: make_session(result_id)},
    )
    runner = make_runner(monkeypatch, fake)
    await runner.run_replay(replay_id)
    env = dict(
        line.split("=", 1) for line in out_file.read_text().splitlines() if "=" in line
    )
    return replay_id, env


async def test_env_contract_and_merge_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Layer run env over the process env, secrets over both, contract on top."""
    monkeypatch.setenv("KITARU_TEST_OS_VAR", "os")
    replay_id, env = await run_env_dump(
        monkeypatch,
        tmp_path,
        inputs={"question": "hi"},
        run_env={"KITARU_TEST_OS_VAR": "run", "KITARU_TEST_SHARED": "run"},
        secret_env={"KITARU_TEST_SHARED": "secret", "KITARU_API_URL": "secret"},
    )
    assert env["KITARU_TEST_OS_VAR"] == "run"
    assert env["KITARU_TEST_SHARED"] == "secret"
    assert env["KITARU_API_URL"] == "http://server"
    assert env["KITARU_API_KEY"] == "key"
    assert env["KITARU_REPLAY_ID"] == str(replay_id)
    assert json.loads(env["KITARU_INPUTS"]) == {"question": "hi"}


async def test_inputs_env_omitted_over_threshold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Omit KITARU_INPUTS when the encoded inputs exceed the threshold."""
    _, env = await run_env_dump(monkeypatch, tmp_path, inputs="x" * 40_000)
    assert "KITARU_INPUTS" not in env
    assert "KITARU_REPLAY_ID" in env


async def test_run_experiment_run_claims_until_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claim and execute replays until the claim drains and the run ends."""
    replay_id = uuid.uuid4()
    result_id = uuid.uuid4()
    run_id = uuid.uuid4()
    replay = make_replay(replay_id, result_session_id=result_id)
    fake = FakeClient(
        spec=make_spec(replay_id, command="true"),
        replay=replay,
        sessions_by_id={result_id: make_session(result_id)},
        claim_batches=[[replay]],
        run=make_run(ExperimentRunStatus.COMPLETED),
    )
    runner = make_runner(
        monkeypatch, fake, worker_id="worker-1", concurrency=2, claim_batch_size=5
    )
    final = await runner.run_experiment_run(run_id)

    assert final.status is ExperimentRunStatus.COMPLETED
    assert len(fake.claim_requests) == 2
    assert fake.claim_requests[0].worker_id == "worker-1"
    assert fake.claim_requests[0].max_replays == 5
    assert [update.status for update in fake.updates] == [
        ReplayStatus.RUNNING,
        ReplayStatus.COMPLETED,
    ]
