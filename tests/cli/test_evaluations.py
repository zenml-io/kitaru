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
"""Session evaluation and stored evaluation inspection CLI behavior."""

import json
import traceback
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.evaluation import (
    EvaluationBatchCreateRequest,
    EvaluationListParams,
)
from kitaru.api_models.v1.filter import FilterCondition
from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskOnFailure,
    TaskResponse,
    TaskStatus,
)
from kitaru.cli import app as app_module
from kitaru.cli import evaluations, registration, session_selection
from kitaru.cli.output import CLIError
from kitaru.client.exceptions import APIError


@dataclass
class StubEvaluation:
    """Small evaluation response exposing JSON serialization."""

    id: uuid.UUID
    passed: bool | None
    values: dict[str, Any] = field(default_factory=dict)

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {"id": str(self.id), "passed": self.passed, **self.values}


class StubEvaluations:
    """Evaluation resource fake that records list and get requests."""

    def __init__(self) -> None:
        self.evaluation = StubEvaluation(
            uuid.uuid4(), False, {"name": "quality", "score": 0.25}
        )
        self.list_calls: list[EvaluationListParams] = []
        self.get_calls: list[uuid.UUID] = []

    async def list(self, params: EvaluationListParams) -> Any:
        self.list_calls.append(params)
        return SimpleNamespace(items=[self.evaluation], next_cursor=None)

    async def get(self, evaluation_id: uuid.UUID) -> StubEvaluation:
        self.get_calls.append(evaluation_id)
        return self.evaluation


async def test_evaluation_list_and_get_preserve_stored_passed_value() -> None:
    """Evaluation inspection returns complete rows without interpreting passed."""
    resource = StubEvaluations()
    client = SimpleNamespace(evaluations=resource)
    filter_value = f'{{"field":"session_id","op":"eq","value":"{uuid.uuid4()}"}}'

    listed = await evaluations.list_evaluations(
        client,
        size=9,
        cursor="cursor",
        sort="created:asc",
        filter=filter_value,
    )
    params = resource.list_calls[0]
    assert isinstance(params, EvaluationListParams)
    dumped_params = params.model_dump(mode="json")
    assert {key: dumped_params[key] for key in ("cursor", "size", "sort")} == {
        "cursor": "cursor",
        "size": 9,
        "sort": "created:asc",
    }
    assert json.loads(dumped_params["filter"]) == json.loads(filter_value)
    assert listed.items == [
        {
            "id": str(resource.evaluation.id),
            "passed": False,
            "name": "quality",
            "score": 0.25,
        }
    ]
    assert listed.page == {"limit": 9, "next_cursor": None, "truncated": False}

    fetched = await evaluations.get_evaluation(client, resource.evaluation.id)
    assert resource.get_calls == [resource.evaluation.id]
    assert fetched.item is not None
    assert fetched.item["passed"] is False


def test_evaluation_list_argv_uses_shared_list_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The registered leaf passes one bounded list request to the SDK."""
    resource = StubEvaluations()
    client = SimpleNamespace(evaluations=resource)

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "evaluation",
                "list",
                "--size",
                "4",
                "--sort",
                "created:asc",
                "--filter",
                '{"field":"name","op":"eq","value":"quality"}',
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "evaluation.list"
    assert payload["count"] == 1
    params = resource.list_calls[0]
    assert params.size == 4
    assert params.sort == "created:asc"


def test_evaluation_get_argv_uses_exact_uuid(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The registered get leaf forwards the parsed UUID unchanged."""
    resource = StubEvaluations()
    client = SimpleNamespace(evaluations=resource)

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert app_module.main(["evaluation", "get", str(resource.evaluation.id)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "evaluation.get"
    assert payload["item"]["passed"] is False
    assert resource.get_calls == [resource.evaluation.id]


def test_invalid_evaluation_uuid_reports_the_full_command(capsys) -> None:
    """UUID parsing failures retain the registered nested command name."""
    assert app_module.main(["evaluation", "get", "not-a-uuid"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["command"] == "evaluation.get"
    assert payload["error"]["kind"] == "invalid_arguments"


def _job(status: JobStatus = JobStatus.PENDING) -> JobResponse:
    """Build one job response for evaluation creation tests."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return JobResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        created=now,
        updated=now,
        kind=JobKind.EVALUATION,
        status=status,
        started_at=now if status is not JobStatus.PENDING else None,
        ended_at=now
        if status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}
        else None,
        error="evaluation failed" if status is JobStatus.FAILED else None,
    )


def _task(
    job: JobResponse,
    session_id: uuid.UUID,
    evaluator_version_id: uuid.UUID,
    *,
    status: TaskStatus = TaskStatus.COMPLETED,
    result: Any = None,
) -> TaskResponse:
    """Build one evaluator task response with exact pair identity."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return TaskResponse(
        id=uuid.uuid4(),
        job_id=job.id,
        kind=TaskKind.EVALUATOR,
        status=status,
        on_failure=TaskOnFailure.CONTINUE,
        attempt=1,
        labels={},
        plugin_version_id=evaluator_version_id,
        input_session_id=session_id,
        error="scoring failed" if status in evaluations._FAILED_TASK_STATUSES else None,
        result=result,
        created=now,
        updated=now,
    )


class StubEvaluationClient:
    """Protocol-shaped client recording exact evaluation requests."""

    def __init__(self, *, create_error: Exception | None = None) -> None:
        self.quality = SimpleNamespace(
            id=uuid.uuid4(), name="quality", latest_version=2
        )
        self.quality_version = SimpleNamespace(
            id=uuid.uuid4(), evaluator_id=self.quality.id, version=2
        )
        self.judge = SimpleNamespace(id=uuid.uuid4(), name="judge", latest_version=1)
        self.judge_version = SimpleNamespace(
            id=uuid.uuid4(), evaluator_id=self.judge.id, version=1
        )
        self.job = _job()
        self.requests: list[EvaluationBatchCreateRequest] = []
        self.create_idempotency_keys: list[str | None] = []
        self.create_error = create_error
        self.evaluators = self._Evaluators(self)
        self.evaluations = self._Evaluations(self)
        self.selected_sessions = [SimpleNamespace(id=uuid.uuid4())]
        self.sessions = self._Sessions(self)

    class _Sessions:
        def __init__(self, owner: "StubEvaluationClient") -> None:
            self.owner = owner
            self.params: SessionListParams | None = None

        async def iter(self, params):
            self.params = params
            for session in self.owner.selected_sessions:
                yield session

    class _Evaluators:
        def __init__(self, owner: "StubEvaluationClient") -> None:
            self.owner = owner

        async def iter(self):
            yield self.owner.quality
            yield self.owner.judge

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            return SimpleNamespace(
                items=[self.owner.quality, self.owner.judge], next_cursor=None
            )

        async def get(self, parent_id: uuid.UUID) -> Any:
            if parent_id == self.owner.quality.id:
                return self.owner.quality
            if parent_id == self.owner.judge.id:
                return self.owner.judge
            raise AssertionError(f"Unexpected evaluator ID: {parent_id}")

        async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
            if (parent_id, version) == (
                self.owner.quality.id,
                self.owner.quality_version.version,
            ):
                return self.owner.quality_version
            if (parent_id, version) == (
                self.owner.judge.id,
                self.owner.judge_version.version,
            ):
                return self.owner.judge_version
            raise AssertionError(f"Unexpected evaluator version: {parent_id}@{version}")

    class _Evaluations:
        def __init__(self, owner: "StubEvaluationClient") -> None:
            self.owner = owner

        async def create(
            self,
            request: EvaluationBatchCreateRequest,
            idempotency_key: str | None = None,
        ) -> JobResponse:
            self.owner.requests.append(request)
            self.owner.create_idempotency_keys.append(idempotency_key)
            if self.owner.create_error is not None:
                raise self.owner.create_error
            return self.owner.job


async def test_resolve_evaluator_configs_pins_latest_and_returns_identities() -> None:
    """Shared evaluator resolution returns concrete configs and typed version IDs."""
    client = StubEvaluationClient()

    configs, identities, version_ids = await registration.resolve_evaluator_configs(
        client,
        ["quality@latest", "judge@1"],
        ['quality@latest={"threshold":0.7}'],
    )

    assert [config.model_dump(mode="json") for config in configs] == [
        {"evaluator": "quality", "version": 2, "params": {"threshold": 0.7}},
        {"evaluator": "judge", "version": 1, "params": {}},
    ]
    assert identities == [
        {
            "id": str(client.quality.id),
            "name": "quality",
            "version_id": str(client.quality_version.id),
            "version": 2,
        },
        {
            "id": str(client.judge.id),
            "name": "judge",
            "version_id": str(client.judge_version.id),
            "version": 1,
        },
    ]
    assert version_ids == [client.quality_version.id, client.judge_version.id]
    assert all(isinstance(version_id, uuid.UUID) for version_id in version_ids)


@pytest.mark.parametrize(
    "content",
    [
        "# comments are not supported\n",
        "not-a-uuid\n",
    ],
)
def test_sessions_file_rejects_non_uuid_nonblank_lines(
    tmp_path: Path, content: str
) -> None:
    """Every nonblank sessions-file line must be a UUID, including comments."""
    sessions_file = tmp_path / "sessions.txt"
    sessions_file.write_text(content, encoding="utf-8")

    with pytest.raises(CLIError) as error:
        session_selection.parse_session_ids([], sessions_file)

    assert error.value.kind == "invalid_arguments"


def test_sessions_file_accepts_utf8_crlf_and_combines_with_positionals(
    tmp_path: Path,
) -> None:
    """CRLF and blank lines are accepted while input order remains stable."""
    positional = uuid.uuid4()
    first = uuid.uuid4()
    second = uuid.uuid4()
    sessions_file = tmp_path / "sessions.txt"
    sessions_file.write_bytes(f"{first}\r\n\r\n  {second}  \r\n".encode())

    assert session_selection.parse_session_ids([str(positional)], sessions_file) == [
        positional,
        first,
        second,
    ]


@pytest.mark.parametrize("file_value", ["-", "missing.txt"])
def test_sessions_file_rejects_stdin_and_missing_files(
    tmp_path: Path, file_value: str
) -> None:
    """Sessions-file input is bounded to one existing regular file."""
    path = Path("-") if file_value == "-" else tmp_path / file_value

    with pytest.raises(CLIError) as error:
        session_selection.parse_session_ids([], path)

    assert error.value.kind == "invalid_arguments"


def test_sessions_file_rejects_invalid_utf8(tmp_path: Path) -> None:
    """Sessions files use strict UTF-8 decoding."""
    sessions_file = tmp_path / "sessions.txt"
    sessions_file.write_bytes(b"\xff\xfe")

    with pytest.raises(CLIError) as error:
        session_selection.parse_session_ids([], sessions_file)

    assert error.value.kind == "invalid_arguments"
    assert error.value.__suppress_context__ is True


def test_sessions_file_read_error_suppresses_private_path_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sanitized sessions-file failures do not chain path-bearing OS errors."""
    sessions_file = tmp_path / "private-sessions.txt"
    sessions_file.write_text("", encoding="utf-8")

    def fail_read(_: Path, *, encoding: str) -> str:
        assert encoding == "utf-8"
        raise OSError(13, "permission denied", str(sessions_file))

    monkeypatch.setattr(Path, "read_text", fail_read)

    with pytest.raises(CLIError) as error:
        session_selection.read_session_file(sessions_file)

    rendered = "".join(
        traceback.format_exception(
            type(error.value), error.value, error.value.__traceback__
        )
    )
    assert error.value.__suppress_context__ is True
    assert str(sessions_file) not in rendered


async def test_evaluate_sessions_builds_one_exact_cartesian_request(
    tmp_path: Path,
) -> None:
    """Selection, exact versions, params, and pair counts produce one mutation."""
    client = StubEvaluationClient()
    positional = uuid.uuid4()
    from_file = uuid.uuid4()
    sessions_file = tmp_path / "sessions.txt"
    sessions_file.write_text(f"{from_file}\n", encoding="utf-8")

    result = await evaluations.evaluate_sessions(
        client,
        [str(positional)],
        sessions_file=sessions_file,
        evaluators=["quality@latest", "judge@1"],
        evaluator_params=['quality@latest={"threshold":0.7}'],
        wait=False,
        interval=None,
        timeout=None,
    )

    assert len(client.requests) == 1
    request = client.requests[0]
    assert request.input_session_ids == [positional, from_file]
    assert [config.model_dump(mode="json") for config in request.evaluators] == [
        {"evaluator": "quality", "version": 2, "params": {"threshold": 0.7}},
        {"evaluator": "judge", "version": 1, "params": {}},
    ]
    assert result.event == "created"
    assert result.item["session_ids"] == [str(positional), str(from_file)]
    assert result.item["session_count"] == 2
    assert result.item["evaluator_count"] == 2
    assert result.item["pair_count"] == 4
    assert "threshold" not in json.dumps(result.item)
    assert result.next_actions == [
        f"kitaru job watch {client.job.id}",
        f"kitaru job get {client.job.id} --tasks",
    ]


async def test_evaluate_sessions_does_not_enforce_or_batch_pair_cap() -> None:
    """The CLI discloses a large pair count and submits exactly one request."""
    client = StubEvaluationClient()
    session_ids = [str(uuid.uuid4()) for _ in range(101)]

    result = await evaluations.evaluate_sessions(
        client,
        session_ids,
        sessions_file=None,
        evaluators=["quality@2"],
        evaluator_params=None,
        wait=False,
        interval=None,
        timeout=None,
    )

    assert result.item["pair_count"] == 101
    assert len(client.requests) == 1
    assert len(client.requests[0].input_session_ids) == 101


async def test_evaluate_sessions_selects_all_sessions_with_tag() -> None:
    """A tag replaces manual session ID collection."""
    client = StubEvaluationClient()

    result = await evaluations.evaluate_sessions(
        client,
        None,
        sessions_file=None,
        tag="baseline",
        all_sessions=False,
        evaluators=["quality@2"],
        evaluator_params=None,
        wait=False,
        interval=None,
        timeout=None,
    )

    assert client.sessions.params is not None
    session_filter = client.sessions.params.filter
    assert isinstance(session_filter, FilterCondition)
    assert session_filter.field == "tag"
    assert session_filter.value == "baseline"
    assert client.requests[0].input_session_ids == [
        session.id for session in client.selected_sessions
    ]
    assert result.item["session_count"] == 1


async def test_evaluate_sessions_all_is_explicit_and_exclusive() -> None:
    """All-session selection is available but cannot be mixed with IDs."""
    client = StubEvaluationClient()

    result = await evaluations.evaluate_sessions(
        client,
        None,
        sessions_file=None,
        tag=None,
        all_sessions=True,
        evaluators=["quality@2"],
        evaluator_params=None,
        wait=False,
        interval=None,
        timeout=None,
    )
    assert client.sessions.params is not None
    assert client.sessions.params.filter is None
    assert result.item["session_count"] == 1

    with pytest.raises(CLIError, match="--filter, or --all"):
        await evaluations.evaluate_sessions(
            client,
            [str(uuid.uuid4())],
            sessions_file=None,
            tag="baseline",
            all_sessions=False,
            evaluators=["quality@2"],
            evaluator_params=None,
            wait=False,
            interval=None,
            timeout=None,
        )


async def test_duplicate_session_across_sources_is_rejected_before_mutation(
    tmp_path: Path,
) -> None:
    """Combined positional/file duplicates fail before evaluator lookup or creation."""
    client = StubEvaluationClient()
    session_id = uuid.uuid4()
    sessions_file = tmp_path / "sessions.txt"
    sessions_file.write_text(f"{session_id}\n", encoding="utf-8")

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(session_id)],
            sessions_file=sessions_file,
            evaluators=["quality@2"],
            evaluator_params=None,
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.requests == []


@pytest.mark.parametrize(
    ("evaluators", "params"),
    [
        ([], None),
        (["quality@2", "quality@2"], None),
        (["quality@2"], ["judge@1={}"]),
        (["quality@2"], ["quality@2={}", "quality@2={}"]),
        (["quality@2"], ["quality@2=[]"]),
    ],
)
async def test_duplicate_or_invalid_evaluator_selection_fails_before_mutation(
    evaluators: list[str], params: list[str] | None
) -> None:
    """Evaluator tokens and parameter associations are deterministic."""
    client = StubEvaluationClient()

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(uuid.uuid4())],
            sessions_file=None,
            evaluators=evaluators,
            evaluator_params=params,
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.requests == []


async def test_different_tokens_resolving_to_same_version_are_rejected() -> None:
    """Aliases cannot select the same evaluator version twice."""
    client = StubEvaluationClient()

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(uuid.uuid4())],
            sessions_file=None,
            evaluators=["quality@latest", f"{client.quality.id}@2"],
            evaluator_params=None,
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.requests == []


async def test_wait_flags_are_validated_before_evaluation_mutation() -> None:
    """Polling overrides require a waited command and fail before creation."""
    client = StubEvaluationClient()

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(uuid.uuid4())],
            sessions_file=None,
            evaluators=["quality@2"],
            evaluator_params=None,
            wait=False,
            interval=1.0,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.requests == []


async def test_waited_evaluation_returns_sorted_task_receipt_without_aggregation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Waited success validates, sorts, and exposes exact task result lookups."""
    client = StubEvaluationClient()
    session_ids = [uuid.uuid4(), uuid.uuid4()]
    terminal = _job(JobStatus.COMPLETED)
    tasks = [
        _task(
            terminal,
            session_ids[1],
            client.quality_version.id,
            result=[{"name": "quality", "score": 0.2, "passed": False}],
        ),
        _task(
            terminal,
            session_ids[0],
            client.quality_version.id,
            result=[{"name": "quality", "score": 0.9, "passed": True}],
        ),
    ]
    events: list[tuple[str, Any]] = []

    async def wait_for_terminal_tasks(*args, **kwargs):
        assert args[1] == client.job.id
        assert kwargs == {
            "interval": 2.0,
            "timeout": 300.0,
            "initial_job": client.job,
        }
        return terminal, tasks

    monkeypatch.setattr(
        evaluations.receipts, "wait_for_terminal_tasks", wait_for_terminal_tasks
    )
    monkeypatch.setattr(
        evaluations, "emit_event", lambda event, item: events.append((event, item))
    )

    result = await evaluations.evaluate_sessions(
        client,
        [str(session_id) for session_id in session_ids],
        sessions_file=None,
        evaluators=["quality@latest"],
        evaluator_params=None,
        wait=True,
        interval=None,
        timeout=None,
    )

    assert events[0][0] == "created"
    assert result.event == "terminal"
    assert [task["input_session_id"] for task in result.item["tasks"]] == sorted(
        str(session_id) for session_id in session_ids
    )
    assert result.item["summary"] == {
        "total_tasks": 2,
        "completed_tasks": 2,
        "failed_tasks": 0,
        "canceled_tasks": 0,
        "result_count": 2,
    }
    assert "passed" not in result.item["summary"]
    assert {task["results"][0]["passed"] for task in result.item["tasks"]} == {
        True,
        False,
    }
    assert all('"field":"task_id"' in action for action in result.next_actions)
    assert all(
        str(task.id) in result.next_actions[index]
        for index, task in enumerate(
            sorted(
                tasks,
                key=lambda task: (
                    str(task.input_session_id),
                    str(task.plugin_version_id),
                    str(task.id),
                ),
            )
        )
    )


@pytest.mark.parametrize(
    ("job_status", "failed_status", "error_kind"),
    [
        (JobStatus.FAILED, TaskStatus.FAILED, "remote_failed"),
        (JobStatus.CANCELED, TaskStatus.CANCELED, "remote_canceled"),
    ],
)
def test_terminal_evaluation_preserves_partial_and_canceled_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    job_status: JobStatus,
    failed_status: TaskStatus,
    error_kind: str,
) -> None:
    """Successful siblings and failed/canceled task diagnostics remain in errors."""
    client = StubEvaluationClient()
    job = _job(job_status)
    session_ids = [uuid.uuid4(), uuid.uuid4()]
    tasks = [
        _task(
            job,
            session_ids[0],
            client.quality_version.id,
            result=[{"name": "quality", "score": 1.0, "passed": True}],
        ),
        _task(
            job,
            session_ids[1],
            client.quality_version.id,
            status=failed_status,
        ),
    ]

    def terminal_job_error(job: JobResponse, receipt: dict[str, Any]) -> CLIError:
        return CLIError(error_kind, "remote outcome", details={"receipt": receipt})

    monkeypatch.setattr(evaluations.receipts, "terminal_job_error", terminal_job_error)

    with pytest.raises(CLIError) as error:
        evaluations._terminal_evaluation_result(
            job,
            tasks,
            identity={"pair_count": 2},
            session_ids=session_ids,
            evaluator_version_ids=[client.quality_version.id],
        )

    assert error.value.kind == error_kind
    receipt = error.value.details["receipt"]
    assert len(receipt["tasks"]) == 2
    assert receipt["summary"]["completed_tasks"] == 1
    expected_failed = 1 if failed_status is TaskStatus.FAILED else 0
    assert receipt["summary"]["failed_tasks"] == expected_failed
    assert receipt["summary"]["canceled_tasks"] == 1 - expected_failed
    assert receipt["summary"]["result_count"] == 1
    assert len(error.value.details["next_actions"]) == 2


@pytest.mark.parametrize("malformation", ["missing", "wrong_pair", "bad_result"])
def test_terminal_evaluation_rejects_malformed_task_contract(
    malformation: str,
) -> None:
    """Wrong pair sets and malformed completed outputs are internal errors."""
    client = StubEvaluationClient()
    job = _job(JobStatus.COMPLETED)
    session_id = uuid.uuid4()
    task = _task(
        job,
        session_id,
        client.quality_version.id,
        result=[{"name": "quality", "score": 1.0}],
    )
    tasks = [task]
    if malformation == "missing":
        tasks = []
    elif malformation == "wrong_pair":
        tasks = [
            _task(
                job,
                uuid.uuid4(),
                client.quality_version.id,
                result=[{"name": "quality", "score": 1.0}],
            )
        ]
    else:
        task.result = []

    with pytest.raises(CLIError) as error:
        evaluations._terminal_evaluation_result(
            job,
            tasks,
            identity={},
            session_ids=[session_id],
            evaluator_version_ids=[client.quality_version.id],
        )

    assert error.value.kind == "internal_error"


def test_session_evaluate_argv_registers_streaming_created_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The registered leaf parses repeated exact evaluators and a sessions file."""
    client = StubEvaluationClient()
    positional = uuid.uuid4()
    from_file = uuid.uuid4()
    sessions_file = tmp_path / "sessions.txt"
    sessions_file.write_text(f"{from_file}\n", encoding="utf-8")

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "session",
                "evaluate",
                str(positional),
                "--sessions-file",
                str(sessions_file),
                "--evaluator",
                "quality@2",
                "--evaluator",
                "judge@1",
                "--evaluator-params",
                'quality@2={"threshold":0.5}',
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert captured.err == ""
    assert document["command"] == "session.evaluate"
    assert document["event"] == "created"
    assert document["item"]["pair_count"] == 4
    assert len(client.requests) == 1


def test_session_evaluate_maps_server_pair_limit_without_batching(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The authoritative server 422 becomes invalid arguments after one request."""
    client = StubEvaluationClient(
        create_error=APIError(422, "Evaluation request holds 101 pairs, the cap is 100")
    )

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "session",
                "evaluate",
                str(uuid.uuid4()),
                "--evaluator",
                "quality@2",
            ]
        )
        == 2
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    document = json.loads(captured.err)
    assert document["command"] == "session.evaluate"
    assert document["error"]["kind"] == "invalid_arguments"
    assert "101 pairs" in document["error"]["message"]
    assert len(client.requests) == 1
