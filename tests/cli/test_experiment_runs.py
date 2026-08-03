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
"""Experiment-run lifecycle CLI behavior."""

import asyncio
import io
import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Literal

import pytest

from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunJobsListParams,
    ExperimentRunListParams,
    ExperimentRunProgress,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.cli import app as app_module
from kitaru.cli import experiment_runs
from kitaru.cli.output import (
    CLIError,
    OutputContext,
    emit_result,
    reset_output_context,
    set_output_context,
)


def _run(
    run_id: uuid.UUID,
    status: ExperimentRunStatus,
    *,
    experiment_id: uuid.UUID,
    cohort_version_id: uuid.UUID,
    agent_version_id: uuid.UUID,
    updated_offset: int = 0,
    completed: int = 0,
) -> ExperimentRunResponse:
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return ExperimentRunResponse(
        id=run_id,
        owner_id=uuid.uuid5(uuid.NAMESPACE_URL, str(run_id)),
        created=now,
        updated=now + timedelta(seconds=updated_offset),
        experiment_id=experiment_id,
        number=3,
        status=status,
        cohort_version_id=cohort_version_id,
        agent_version_id=agent_version_id,
        evaluate_baselines=True,
        started_at=now,
        ended_at=now
        if status
        in {
            ExperimentRunStatus.COMPLETED,
            ExperimentRunStatus.FAILED,
            ExperimentRunStatus.CANCELED,
        }
        else None,
        error="remote failure" if status is ExperimentRunStatus.FAILED else None,
        progress=ExperimentRunProgress(
            pending=0 if completed else 1,
            evaluating=0,
            completed=completed,
            failed=1 if status is ExperimentRunStatus.FAILED else 0,
            canceled=1 if status is ExperimentRunStatus.CANCELED else 0,
            total=1,
        ),
    )


class StubRunsClient:
    """Protocol-shaped client recording all experiment-run SDK calls."""

    def __init__(self) -> None:
        self.experiment = SimpleNamespace(id=uuid.uuid4(), name="regression")
        self.cohort_version = SimpleNamespace(
            id=uuid.uuid4(), cohort_id=uuid.uuid4(), version=4
        )
        self.agent = SimpleNamespace(
            id=uuid.uuid4(), name="candidate", latest_version=2
        )
        self.agent_version = SimpleNamespace(
            id=uuid.uuid4(), agent_id=self.agent.id, version=2
        )
        self.run_id = uuid.uuid4()
        self.created_run = _run(
            self.run_id,
            ExperimentRunStatus.RUNNING,
            experiment_id=self.experiment.id,
            cohort_version_id=self.cohort_version.id,
            agent_version_id=self.agent_version.id,
        )
        self.get_responses = [self.created_run]
        self.start_calls: list[tuple[uuid.UUID, ExperimentRunCreateRequest]] = []
        self.list_calls: list[ExperimentRunListParams] = []
        self.jobs_calls: list[tuple[uuid.UUID, ExperimentRunJobsListParams]] = []
        self.get_calls = 0
        self.cancel_calls = 0
        self.delete_calls = 0
        self.experiment_lookups = 0
        self.experiments = self._Experiments(self)
        self.experiment_runs = self._Runs(self)
        self.cohort_versions = self._CohortVersions(self)
        self.agents = self._Agents(self)
        self.agent_versions = self._AgentVersions(self)

    class _Experiments:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def iter(self):
            self.owner.experiment_lookups += 1
            yield self.owner.experiment

        async def get(self, experiment_id: uuid.UUID) -> Any:
            self.owner.experiment_lookups += 1
            assert experiment_id == self.owner.experiment.id
            return self.owner.experiment

        async def start_run(
            self, experiment_id: uuid.UUID, request: ExperimentRunCreateRequest
        ) -> ExperimentRunResponse:
            self.owner.start_calls.append((experiment_id, request))
            return self.owner.created_run

    class _Runs:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def get(self, run_id: uuid.UUID) -> ExperimentRunResponse:
            assert run_id == self.owner.run_id
            index = min(self.owner.get_calls, len(self.owner.get_responses) - 1)
            self.owner.get_calls += 1
            return self.owner.get_responses[index]

        async def list(self, params: ExperimentRunListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(items=[self.owner.created_run], next_cursor="next")

        async def list_jobs(
            self, run_id: uuid.UUID, params: ExperimentRunJobsListParams
        ) -> Any:
            self.owner.jobs_calls.append((run_id, params))
            job = SimpleNamespace(model_dump=lambda **_: {"id": "job-1"})
            return SimpleNamespace(items=[job], next_cursor=None)

        async def cancel(self, run_id: uuid.UUID) -> ExperimentRunResponse:
            assert run_id == self.owner.run_id
            self.owner.cancel_calls += 1
            return self.owner.get_responses[-1]

        async def delete(self, run_id: uuid.UUID) -> None:
            assert run_id == self.owner.run_id
            self.owner.delete_calls += 1

    class _CohortVersions:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def get(self, version_id: uuid.UUID) -> Any:
            assert version_id == self.owner.cohort_version.id
            return self.owner.cohort_version

    class _Agents:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def iter(self):
            yield self.owner.agent

        async def get(self, agent_id: uuid.UUID) -> Any:
            assert agent_id == self.owner.agent.id
            return self.owner.agent

        async def iter_versions(self, agent_id: uuid.UUID):
            assert agent_id == self.owner.agent.id
            yield self.owner.agent_version

    class _AgentVersions:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def get(self, version_id: uuid.UUID) -> Any:
            assert version_id == self.owner.agent_version.id
            return self.owner.agent_version


def _output_context(
    mode: Literal["text", "json", "jsonl"], stdout: io.StringIO
) -> OutputContext:
    return OutputContext(
        command="experiment.run.watch",
        mode=mode,
        machine=True,
        non_interactive=True,
        debug=False,
        traceback=False,
        stdout=stdout,
        stderr=io.StringIO(),
        rich=False,
    )


async def test_start_returns_exact_created_receipt() -> None:
    """Immediate start resolves exact inputs and submits one current SDK request."""
    client = StubRunsClient()

    result = await experiment_runs.start_run(
        client,
        "regression",
        cohort_version_id=client.cohort_version.id,
        agent_reference="candidate@latest",
        evaluate_baselines=True,
        wait=False,
        interval=None,
        timeout=None,
    )

    [(experiment_id, request)] = client.start_calls
    assert experiment_id == client.experiment.id
    assert request == ExperimentRunCreateRequest(
        cohort_version_id=client.cohort_version.id,
        agent_version_id=client.agent_version.id,
        evaluate_baselines=True,
    )
    assert result.event == "created"
    assert result.item["operation"] == "experiment_run"
    assert result.item["terminal"] is False
    assert result.item["experiment"] == {
        "id": str(client.experiment.id),
        "name": "regression",
    }
    assert result.item["agent_version"]["version_id"] == str(client.agent_version.id)
    assert result.item["run"]["status"] == "running"
    assert result.next_actions == [
        f"kitaru experiment run watch {client.run_id}",
        f"kitaru experiment run get {client.run_id}",
        f"kitaru experiment run jobs {client.run_id}",
        f"kitaru experiment run cancel {client.run_id}",
    ]
    assert all("<" not in action for action in result.next_actions)


async def test_waited_start_emits_executable_created_and_terminal_actions() -> None:
    """Waited starts preserve immediate actions and terminal inspection actions."""
    client = StubRunsClient()
    completed = _run(
        client.run_id,
        ExperimentRunStatus.COMPLETED,
        experiment_id=client.experiment.id,
        cohort_version_id=client.cohort_version.id,
        agent_version_id=client.agent_version.id,
        completed=1,
    )
    client.get_responses = [completed]

    stdout = io.StringIO()
    token = set_output_context(_output_context("jsonl", stdout))
    try:
        result = await experiment_runs.start_run(
            client,
            "regression",
            cohort_version_id=client.cohort_version.id,
            agent_reference="candidate@2",
            evaluate_baselines=False,
            wait=True,
            interval=1,
            timeout=2,
        )
        assert emit_result(result) == 0
    finally:
        reset_output_context(token)

    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["event"] for event in events] == [
        "created",
        "snapshot",
        "terminal",
    ]
    created_actions = events[0]["item"]["next_actions"]
    assert created_actions == [
        f"kitaru experiment run watch {client.run_id}",
        f"kitaru experiment run get {client.run_id}",
        f"kitaru experiment run jobs {client.run_id}",
        f"kitaru experiment run cancel {client.run_id}",
    ]
    assert events[-1]["next_actions"] == [
        f"kitaru experiment run get {client.run_id}",
        f"kitaru experiment run jobs {client.run_id}",
    ]
    assert all(
        "<" not in action
        for event in (events[0], events[-1])
        for action in event.get("next_actions", event["item"].get("next_actions", []))
    )


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (ExperimentRunStatus.FAILED, "remote_failed"),
        (ExperimentRunStatus.CANCELED, "remote_canceled"),
    ],
)
async def test_waited_start_preserves_actions_on_terminal_errors(
    status: ExperimentRunStatus, kind: str
) -> None:
    """Waited terminal errors retain created and recovery inspection actions."""
    client = StubRunsClient()
    client.get_responses = [
        _run(
            client.run_id,
            status,
            experiment_id=client.experiment.id,
            cohort_version_id=client.cohort_version.id,
            agent_version_id=client.agent_version.id,
        )
    ]

    stdout = io.StringIO()
    token = set_output_context(_output_context("jsonl", stdout))
    try:
        with pytest.raises(CLIError) as error:
            await experiment_runs.start_run(
                client,
                "regression",
                cohort_version_id=client.cohort_version.id,
                agent_reference="candidate@2",
                evaluate_baselines=False,
                wait=True,
                interval=1,
                timeout=2,
            )
    finally:
        reset_output_context(token)

    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["event"] for event in events] == [
        "created",
        "snapshot",
        "terminal",
    ]
    assert events[0]["item"]["next_actions"] == [
        f"kitaru experiment run watch {client.run_id}",
        f"kitaru experiment run get {client.run_id}",
        f"kitaru experiment run jobs {client.run_id}",
        f"kitaru experiment run cancel {client.run_id}",
    ]
    assert error.value.kind == kind
    assert error.value.details["next_actions"] == [
        f"kitaru experiment run get {client.run_id}",
        f"kitaru experiment run jobs {client.run_id}",
    ]


async def test_start_validates_wait_settings_before_network_access() -> None:
    """Run wait flags fail before reference resolution or mutation."""
    client = StubRunsClient()

    with pytest.raises(CLIError, match="require --wait"):
        await experiment_runs.start_run(
            client,
            "regression",
            cohort_version_id=client.cohort_version.id,
            agent_reference="candidate@2",
            evaluate_baselines=False,
            wait=False,
            interval=1,
            timeout=None,
        )

    assert client.experiment_lookups == 0
    assert client.start_calls == []


async def test_poll_suppresses_updated_only_changes_and_created_snapshot() -> None:
    """Run polling fingerprints the complete response except its updated timestamp."""
    client = StubRunsClient()
    client.get_responses = [
        _run(
            client.run_id,
            ExperimentRunStatus.RUNNING,
            experiment_id=client.experiment.id,
            cohort_version_id=client.cohort_version.id,
            agent_version_id=client.agent_version.id,
            updated_offset=1,
        ),
        _run(
            client.run_id,
            ExperimentRunStatus.RUNNING,
            experiment_id=client.experiment.id,
            cohort_version_id=client.cohort_version.id,
            agent_version_id=client.agent_version.id,
            updated_offset=2,
            completed=1,
        ),
        _run(
            client.run_id,
            ExperimentRunStatus.COMPLETED,
            experiment_id=client.experiment.id,
            cohort_version_id=client.cohort_version.id,
            agent_version_id=client.agent_version.id,
            updated_offset=3,
            completed=1,
        ),
    ]

    async def sleep(_: float) -> None:
        return None

    stdout = io.StringIO()
    token = set_output_context(_output_context("jsonl", stdout))
    try:
        settled = await experiment_runs.poll_run(
            client,
            client.run_id,
            interval=1,
            timeout=None,
            sleep=sleep,
            initial_run=client.created_run,
        )
    finally:
        reset_output_context(token)

    assert settled.status is ExperimentRunStatus.COMPLETED
    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["item"]["status"] for event in events] == [
        "running",
        "completed",
    ]


@pytest.mark.parametrize(
    ("interval", "timeout", "message"),
    [
        (0, None, "--interval must be positive and finite"),
        (float("inf"), None, "--interval must be positive and finite"),
        (1, 0, "--timeout must be positive and finite"),
        (1, float("nan"), "--timeout must be positive and finite"),
    ],
)
async def test_poll_validates_positive_finite_timing(
    interval: float, timeout: float | None, message: str
) -> None:
    """Run polling rejects invalid timing without a GET."""
    client = StubRunsClient()
    with pytest.raises(CLIError, match=message):
        await experiment_runs.poll_run(
            client, client.run_id, interval=interval, timeout=timeout
        )
    assert client.get_calls == 0


async def test_poll_timeout_is_recoverable_and_does_not_cancel() -> None:
    """A local run timeout identifies recovery while remote work continues."""
    client = StubRunsClient()
    now = 0.0

    def clock() -> float:
        return now

    async def sleep(delay: float) -> None:
        nonlocal now
        now += delay

    stdout = io.StringIO()
    token = set_output_context(_output_context("json", stdout))
    try:
        with pytest.raises(CLIError) as error:
            await experiment_runs.poll_run(
                client,
                client.run_id,
                interval=2,
                timeout=1,
                clock=clock,
                sleep=sleep,
            )
    finally:
        reset_output_context(token)

    assert error.value.kind == "timeout"
    assert error.value.exit_code == 7
    assert error.value.details == {
        "run_id": str(client.run_id),
        "last_status": "running",
        "remote_continues": True,
    }
    assert f"experiment run watch {client.run_id}" in str(error.value.hint)
    assert client.cancel_calls == 0


async def test_poll_timeout_bounds_an_in_flight_sleep() -> None:
    """A stalled polling delay cannot exceed the local run deadline."""
    client = StubRunsClient()

    async def stalled_sleep(_: float) -> None:
        await asyncio.Event().wait()

    stdout = io.StringIO()
    token = set_output_context(_output_context("json", stdout))
    try:
        with pytest.raises(CLIError) as error:
            await experiment_runs.poll_run(
                client,
                client.run_id,
                interval=1,
                timeout=0.01,
                sleep=stalled_sleep,
            )
    finally:
        reset_output_context(token)

    assert error.value.kind == "timeout"
    assert error.value.details["last_status"] == "running"
    assert client.cancel_calls == 0


@pytest.mark.parametrize(
    ("status", "kind", "exit_code"),
    [
        (ExperimentRunStatus.FAILED, "remote_failed", 8),
        (ExperimentRunStatus.CANCELED, "remote_canceled", 9),
    ],
)
async def test_watch_maps_terminal_errors_with_receipts(
    status: ExperimentRunStatus, kind: str, exit_code: int
) -> None:
    """Failed and canceled watches carry terminal run evidence in stable errors."""
    client = StubRunsClient()
    client.get_responses = [
        _run(
            client.run_id,
            status,
            experiment_id=client.experiment.id,
            cohort_version_id=client.cohort_version.id,
            agent_version_id=client.agent_version.id,
        )
    ]
    stdout = io.StringIO()
    token = set_output_context(_output_context("json", stdout))
    try:
        with pytest.raises(CLIError) as error:
            await experiment_runs.watch_run(
                client, client.run_id, interval=1, timeout=None
            )
    finally:
        reset_output_context(token)

    assert error.value.kind == kind
    assert error.value.exit_code == exit_code
    assert error.value.details["receipt"]["terminal"] is True
    assert error.value.details["receipt"]["run"]["status"] == status.value
    assert error.value.details["next_actions"] == [
        f"kitaru experiment run get {client.run_id}",
        f"kitaru experiment run jobs {client.run_id}",
    ]
    assert stdout.getvalue() == ""


async def test_completed_watch_jsonl_emits_snapshot_then_terminal_receipt() -> None:
    """JSONL watch output is append-only and ends with one receipt."""
    client = StubRunsClient()
    completed = _run(
        client.run_id,
        ExperimentRunStatus.COMPLETED,
        experiment_id=client.experiment.id,
        cohort_version_id=client.cohort_version.id,
        agent_version_id=client.agent_version.id,
        completed=1,
    )
    client.get_responses = [client.created_run, completed]

    async def sleep(_: float) -> None:
        return None

    stdout = io.StringIO()
    token = set_output_context(_output_context("jsonl", stdout))
    try:
        result = await experiment_runs.watch_run(
            client, client.run_id, interval=1, timeout=None, sleep=sleep
        )
        assert emit_result(result) == 0
    finally:
        reset_output_context(token)

    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["event"] for event in events] == [
        "snapshot",
        "snapshot",
        "terminal",
    ]
    assert events[-1]["item"]["operation"] == "experiment_run"
    assert events[-1]["item"]["run"]["status"] == "completed"
    assert events[-1]["next_actions"] == [
        f"kitaru experiment run get {client.run_id}",
        f"kitaru experiment run jobs {client.run_id}",
    ]


async def test_inspection_cancel_and_delete_map_directly_to_sdk() -> None:
    """List/get/jobs/cancel/delete preserve their direct SDK resource semantics."""
    client = StubRunsClient()

    listed = await experiment_runs.list_runs(
        client,
        size=2,
        cursor="cursor",
        sort="created:asc",
        filter='{"field":"status","op":"eq","value":"running"}',
    )
    assert listed.page == {"limit": 2, "next_cursor": "next", "truncated": True}
    [list_params] = client.list_calls
    assert isinstance(list_params, ExperimentRunListParams)
    assert list_params.size == 2

    fetched = await experiment_runs.get_run(client, client.run_id)
    assert fetched.item["status"] == "running"

    jobs = await experiment_runs.list_run_jobs(
        client,
        client.run_id,
        size=3,
        cursor=None,
        sort="created:desc",
        filter='{"field":"status","op":"eq","value":"pending"}',
    )
    assert jobs.items == [{"id": "job-1"}]
    assert isinstance(client.jobs_calls[-1][1], ExperimentRunJobsListParams)

    cancel = await experiment_runs.cancel_run(client, client.run_id)
    assert client.cancel_calls == 1
    assert cancel.item["cancellation_requested"] is True
    assert cancel.next_actions == [f"kitaru experiment run watch {client.run_id}"]

    with pytest.raises(CLIError, match="requires --force"):
        await experiment_runs.delete_run(client, client.run_id, force=False)
    assert client.delete_calls == 0
    deleted = await experiment_runs.delete_run(client, client.run_id, force=True)
    assert deleted.item == {
        "id": str(client.run_id),
        "deleted": True,
        "status": "running",
    }
    assert client.delete_calls == 1


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubRunsClient:
    """Route public run commands through one recording client."""
    client = StubRunsClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_argv_covers_every_run_leaf(
    argv_client: StubRunsClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The nested experiment-run command surface is fully registered."""
    client = argv_client

    assert (
        app_module.main(
            [
                "experiment",
                "run",
                "start",
                "regression",
                "--cohort-version",
                str(client.cohort_version.id),
                "--agent",
                "candidate@2",
                "--output",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "experiment.run.start"
    assert payload["item"]["operation"] == "experiment_run"

    assert app_module.main(["experiment", "run", "list", "--size", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["command"] == "experiment.run.list"

    assert app_module.main(["experiment", "run", "get", str(client.run_id)]) == 0
    assert json.loads(capsys.readouterr().out)["item"]["status"] == "running"

    assert app_module.main(["experiment", "run", "jobs", str(client.run_id)]) == 0
    assert json.loads(capsys.readouterr().out)["items"] == [{"id": "job-1"}]

    completed = _run(
        client.run_id,
        ExperimentRunStatus.COMPLETED,
        experiment_id=client.experiment.id,
        cohort_version_id=client.cohort_version.id,
        agent_version_id=client.agent_version.id,
        completed=1,
    )

    async def completed_watch(*args: Any, **kwargs: Any):
        del args, kwargs
        return experiment_runs._terminal_result(
            completed,
            {
                "operation": "experiment_run",
                "terminal": True,
                "experiment": {"id": str(client.experiment.id), "name": "regression"},
                "cohort_version": {"id": str(client.cohort_version.id)},
                "agent_version": {"version_id": str(client.agent_version.id)},
                "run": completed.model_dump(mode="json"),
            },
        )

    monkeypatch.setattr(experiment_runs, "watch_run", completed_watch)
    assert (
        app_module.main(
            ["experiment", "run", "watch", str(client.run_id), "--output", "json"]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["item"]["terminal"] is True

    assert app_module.main(["experiment", "run", "cancel", str(client.run_id)]) == 0
    assert json.loads(capsys.readouterr().out)["item"]["cancellation_requested"] is True

    assert (
        app_module.main(["experiment", "run", "delete", str(client.run_id), "--force"])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["item"]["deleted"] is True


def test_public_start_wait_validation_is_structured_and_non_mutating(
    argv_client: StubRunsClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invalid create-time wait options use structured stderr before mutation."""
    client = argv_client
    assert (
        app_module.main(
            [
                "experiment",
                "run",
                "start",
                "regression",
                "--cohort-version",
                str(client.cohort_version.id),
                "--agent",
                "candidate@2",
                "--timeout",
                "1",
                "--output",
                "json",
            ]
        )
        == 2
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    error = json.loads(captured.err)
    assert error["command"] == "experiment.run.start"
    assert error["error"]["kind"] == "invalid_arguments"
    assert client.start_calls == []


def test_watch_ctrl_c_does_not_cancel_remote_run(
    argv_client: StubRunsClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Interrupting local observation preserves the existing 130 behavior."""
    client = argv_client

    async def interrupt(*args: Any, **kwargs: Any):
        del args, kwargs
        raise KeyboardInterrupt

    monkeypatch.setattr(experiment_runs, "watch_run", interrupt)
    assert (
        app_module.main(["experiment", "run", "watch", str(client.run_id), "--machine"])
        == 130
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err)["error"]["message"] == "Interrupted."
    assert client.cancel_calls == 0
