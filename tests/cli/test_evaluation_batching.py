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
"""Batching, cap discovery, and multi-job wait tests for session evaluation."""

import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskOnFailure,
    TaskResponse,
    TaskStatus,
)
from kitaru.cli import app as app_module
from kitaru.cli import evaluations, invocation_context
from kitaru.cli.output import CLIError
from kitaru.client.exceptions import APIError


def _make_job(
    status: JobStatus = JobStatus.PENDING,
    *,
    job_id: uuid.UUID | None = None,
) -> JobResponse:
    """Build one evaluation job response."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return JobResponse(
        id=job_id or uuid.uuid4(),
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


def _make_task(
    job: JobResponse,
    session_id: uuid.UUID,
    evaluator_version_id: uuid.UUID,
    *,
    status: TaskStatus = TaskStatus.COMPLETED,
    result: Any = None,
) -> TaskResponse:
    """Build one evaluator task response for a concrete (session, version) pair."""
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
        error="scoring failed" if status is TaskStatus.FAILED else None,
        result=result,
        created=now,
        updated=now,
    )


class BatchEvaluationClient:
    """Client fake supporting configurable caps, multi-job creation, and failures."""

    def __init__(
        self,
        *,
        info_pair_limit: int | None = 100,
        server_enforced_cap: int | None = None,
        fail_on_request_index: int | None = None,
        failure_error: Exception | None = None,
        evaluator_names: tuple[str, ...] = ("quality", "judge", "tone"),
    ) -> None:
        self.info_pair_limit = info_pair_limit
        self.server_enforced_cap = server_enforced_cap
        self.fail_on_request_index = fail_on_request_index
        self.failure_error = failure_error
        self.evaluator_records = [
            SimpleNamespace(id=uuid.uuid4(), name=name, latest_version=1)
            for name in evaluator_names
        ]
        self.evaluator_versions = {
            record.id: SimpleNamespace(
                id=uuid.uuid4(), evaluator_id=record.id, version=1
            )
            for record in self.evaluator_records
        }
        self.requests: list[EvaluationBatchCreateRequest] = []
        self.create_idempotency_keys: list[str | None] = []
        self.created_jobs: list[JobResponse] = []
        self.selected_sessions: list[SimpleNamespace] = []

        self.info = self._Info(self)
        self.evaluators = self._Evaluators(self)
        self.evaluations = self._Evaluations(self)
        self.sessions = self._Sessions(self)

    class _Info:
        def __init__(self, owner: "BatchEvaluationClient") -> None:
            self.owner = owner
            self.calls = 0

        async def get(self) -> Any:
            self.calls += 1
            return SimpleNamespace(evaluation_pair_limit=self.owner.info_pair_limit)

    class _Sessions:
        def __init__(self, owner: "BatchEvaluationClient") -> None:
            self.owner = owner

        async def iter(self, _params: Any):
            for session in self.owner.selected_sessions:
                yield session

    class _Evaluators:
        def __init__(self, owner: "BatchEvaluationClient") -> None:
            self.owner = owner

        async def list(self, _params: Any) -> Any:
            return SimpleNamespace(items=self.owner.evaluator_records, next_cursor=None)

        async def get(self, parent_id: uuid.UUID) -> Any:
            for record in self.owner.evaluator_records:
                if record.id == parent_id:
                    return record
            raise AssertionError(f"Unexpected evaluator ID: {parent_id}")

        async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
            record = self.owner.evaluator_versions[parent_id]
            assert record.version == version
            return record

    class _Evaluations:
        def __init__(self, owner: "BatchEvaluationClient") -> None:
            self.owner = owner

        async def create(
            self,
            request: EvaluationBatchCreateRequest,
            idempotency_key: str | None = None,
        ) -> JobResponse:
            call_index = len(self.owner.requests)
            self.owner.requests.append(request)
            self.owner.create_idempotency_keys.append(idempotency_key)

            pairs = len(request.input_session_ids) * len(request.evaluators)
            if (
                self.owner.server_enforced_cap is not None
                and pairs > self.owner.server_enforced_cap
            ):
                raise APIError(
                    422,
                    f"Evaluation request holds {pairs} pairs, the cap is "
                    f"{self.owner.server_enforced_cap}",
                )
            if (
                self.owner.fail_on_request_index is not None
                and call_index == self.owner.fail_on_request_index
            ):
                raise self.owner.failure_error or APIError(
                    500, "temporary server failure"
                )
            job = _make_job()
            self.owner.created_jobs.append(job)
            return job


async def test_single_batch_at_exact_cap_preserves_legacy_shape() -> None:
    """Exactly 100 pairs keeps the single-job receipt without multi-batch fields."""
    client = BatchEvaluationClient(info_pair_limit=100)
    session_ids = [uuid.uuid4() for _ in range(50)]

    result = await evaluations.evaluate_sessions(
        client,
        [str(sid) for sid in session_ids],
        sessions_file=None,
        evaluators=["quality@1", "judge@1"],
        evaluator_params=None,
        wait=False,
        interval=None,
        timeout=None,
        idempotency_key="single-key",
    )

    assert len(client.requests) == 1
    assert client.create_idempotency_keys == ["single-key"]
    assert result.event == "created"
    assert "job" in result.item
    assert "jobs" not in result.item
    assert "job_ids" not in result.item
    assert "batch_count" not in result.item
    assert result.item["job"]["id"] == str(client.created_jobs[0].id)
    assert result.item["pair_count"] == 100
    assert result.next_actions == [
        f"kitaru job watch {client.created_jobs[0].id}",
        f"kitaru job get {client.created_jobs[0].id} --tasks",
    ]


async def test_multi_batch_splits_single_and_multiple_evaluators_without_wait() -> None:
    """Multi-evaluator selections cap sessions per batch at floor(cap / evaluators)."""
    client = BatchEvaluationClient(info_pair_limit=100)
    session_ids = [uuid.uuid4() for _ in range(120)]

    result = await evaluations.evaluate_sessions(
        client,
        [str(sid) for sid in session_ids],
        sessions_file=None,
        evaluators=["quality@1", "judge@1"],
        evaluator_params=None,
        wait=False,
        interval=None,
        timeout=None,
        idempotency_key="backfill-key",
    )

    assert [len(req.input_session_ids) for req in client.requests] == [50, 50, 20]
    submitted_sessions = [
        sid for req in client.requests for sid in req.input_session_ids
    ]
    assert submitted_sessions == session_ids
    assert client.create_idempotency_keys == [
        "backfill-key:batch:1",
        "backfill-key:batch:2",
        "backfill-key:batch:3",
    ]
    assert result.event == "created"
    assert result.item["operation"] == "session_evaluation"
    assert result.item["terminal"] is False
    assert result.item["status"] == "pending"
    assert result.item["batch_count"] == 3
    assert result.item["session_count"] == 120
    assert result.item["evaluator_count"] == 2
    assert result.item["pair_count"] == 240
    assert result.item["job_ids"] == [str(job.id) for job in client.created_jobs]
    assert [job["id"] for job in result.item["jobs"]] == result.item["job_ids"]
    assert result.next_actions == [
        action
        for job in client.created_jobs
        for action in (
            f"kitaru job watch {job.id}",
            f"kitaru job get {job.id} --tasks",
        )
    ]


async def test_multi_batch_wait_aggregates_tasks_results_and_job_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Waiting over multiple batches combines all tasks and summary counts."""
    client = BatchEvaluationClient(info_pair_limit=4)
    session_ids = [uuid.uuid4() for _ in range(5)]
    quality_vid = client.evaluator_versions[client.evaluator_records[0].id].id
    judge_vid = client.evaluator_versions[client.evaluator_records[1].id].id

    events: list[tuple[str, Any]] = []
    waited_job_ids: list[uuid.UUID] = []

    async def fake_wait_for_terminal_tasks(
        _client: Any,
        job_id: uuid.UUID,
        *,
        interval: float,
        timeout: float,
        initial_job: JobResponse | None = None,
    ) -> tuple[JobResponse, list[TaskResponse]]:
        assert interval == 2.0
        assert timeout > 0
        assert initial_job is not None and initial_job.id == job_id
        waited_job_ids.append(job_id)
        batch_index = [job.id for job in client.created_jobs].index(job_id)
        batch_sessions = client.requests[batch_index].input_session_ids
        settled_job = _make_job(JobStatus.COMPLETED, job_id=job_id)
        batch_tasks = [
            _make_task(
                settled_job,
                sid,
                vid,
                result=[
                    {
                        "name": "check",
                        "score": 1.0 if vid == quality_vid else 0.0,
                        "passed": vid == quality_vid,
                    }
                ],
            )
            for sid in batch_sessions
            for vid in (quality_vid, judge_vid)
        ]
        return settled_job, batch_tasks

    monkeypatch.setattr(
        evaluations.receipts, "wait_for_terminal_tasks", fake_wait_for_terminal_tasks
    )
    monkeypatch.setattr(
        evaluations, "emit_event", lambda event, item: events.append((event, item))
    )

    result = await evaluations.evaluate_sessions(
        client,
        [str(sid) for sid in session_ids],
        sessions_file=None,
        evaluators=["quality@1", "judge@1"],
        evaluator_params=None,
        wait=True,
        interval=None,
        timeout=None,
    )

    # Cap 4 with 2 evaluators -> 2 sessions per batch -> batches of 2, 2, 1.
    assert [len(req.input_session_ids) for req in client.requests] == [2, 2, 1]
    assert waited_job_ids == [job.id for job in client.created_jobs]
    assert [event for event, _ in events] == ["created"]
    assert events[0][1]["batch_count"] == 3
    assert result.event == "terminal"
    assert result.item["terminal"] is True
    assert result.item["status"] == "completed"
    assert result.item["batch_count"] == 3
    assert result.item["job_ids"] == [str(job.id) for job in client.created_jobs]
    assert [job["status"] for job in result.item["jobs"]] == [
        "completed",
        "completed",
        "completed",
    ]
    assert result.item["summary"] == {
        "total_batches": 3,
        "completed_batches": 3,
        "failed_batches": 0,
        "canceled_batches": 0,
        "total_tasks": 10,
        "completed_tasks": 10,
        "failed_tasks": 0,
        "canceled_tasks": 0,
        "result_count": 10,
    }
    assert len(result.item["tasks"]) == 10
    assert [task["input_session_id"] for task in result.item["tasks"]] == sorted(
        task["input_session_id"] for task in result.item["tasks"]
    )
    assert {task["job_id"] for task in result.item["tasks"]} == set(
        result.item["job_ids"]
    )
    assert result.next_actions == [
        f"kitaru job get {job.id} --tasks" for job in client.created_jobs
    ]


@pytest.mark.parametrize(
    ("second_job_status", "second_task_status", "expected_kind"),
    [
        (JobStatus.FAILED, TaskStatus.FAILED, "remote_failed"),
        (JobStatus.CANCELED, TaskStatus.CANCELED, "remote_canceled"),
    ],
)
async def test_multi_batch_wait_aggregates_failed_and_canceled_batches(
    monkeypatch: pytest.MonkeyPatch,
    second_job_status: JobStatus,
    second_task_status: TaskStatus,
    expected_kind: str,
) -> None:
    """A failing or canceled batch still waits for every batch and reports all jobs."""
    client = BatchEvaluationClient(info_pair_limit=2)
    session_ids = [uuid.uuid4() for _ in range(4)]
    quality_vid = client.evaluator_versions[client.evaluator_records[0].id].id
    events: list[tuple[str, Any]] = []

    async def fake_wait_for_terminal_tasks(
        _client: Any,
        job_id: uuid.UUID,
        **_kwargs: Any,
    ) -> tuple[JobResponse, list[TaskResponse]]:
        batch_index = [job.id for job in client.created_jobs].index(job_id)
        batch_sessions = client.requests[batch_index].input_session_ids
        if batch_index == 0:
            settled = _make_job(JobStatus.COMPLETED, job_id=job_id)
            tasks = [
                _make_task(
                    settled,
                    sid,
                    quality_vid,
                    result=[{"name": "quality", "score": 1.0, "passed": True}],
                )
                for sid in batch_sessions
            ]
            return settled, tasks
        settled = _make_job(second_job_status, job_id=job_id)
        tasks = [
            _make_task(
                settled,
                batch_sessions[0],
                quality_vid,
                result=[{"name": "quality", "score": 1.0, "passed": True}],
            ),
            _make_task(
                settled,
                batch_sessions[1],
                quality_vid,
                status=second_task_status,
            ),
        ]
        return settled, tasks

    monkeypatch.setattr(
        evaluations.receipts, "wait_for_terminal_tasks", fake_wait_for_terminal_tasks
    )
    monkeypatch.setattr(
        evaluations, "emit_event", lambda event, item: events.append((event, item))
    )

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(sid) for sid in session_ids],
            sessions_file=None,
            evaluators=["quality@1"],
            evaluator_params=None,
            wait=True,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == expected_kind
    assert [event for event, _ in events] == ["created", "terminal"]
    receipt = error.value.details["receipt"]
    assert receipt["job_ids"] == [str(job.id) for job in client.created_jobs]
    assert [job["status"] for job in receipt["jobs"]] == [
        "completed",
        second_job_status.value,
    ]
    assert receipt["summary"]["total_tasks"] == 4
    assert receipt["summary"]["completed_tasks"] == 3
    assert receipt["summary"]["result_count"] == 3


async def test_partial_failure_during_batch_submission_preserves_submitted_jobs() -> (
    None
):
    """Later batch submission failures preserve earlier job IDs and remaining IDs."""
    client = BatchEvaluationClient(
        info_pair_limit=2,
        fail_on_request_index=1,
        failure_error=APIError(503, "server overloaded"),
    )
    session_ids = [uuid.uuid4() for _ in range(5)]

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(sid) for sid in session_ids],
            sessions_file=None,
            evaluators=["quality@1"],
            evaluator_params=None,
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "partial_failure"
    assert error.value.exit_code == 8
    details = error.value.details
    assert details["batch_count"] == 3
    assert details["submitted_batches"] == 1
    assert details["failed_batch"] == 2
    assert details["job_ids"] == [str(client.created_jobs[0].id)]
    assert details["remaining_session_ids"] == [str(sid) for sid in session_ids[2:]]
    assert details["error"] == {"status_code": 503, "detail": "server overloaded"}
    assert str(client.created_jobs[0].id) in (error.value.hint or "")


async def test_timeout_during_multi_batch_wait_preserves_all_submitted_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wait timeout on a later batch keeps all submitted, settled, and pending IDs."""
    client = BatchEvaluationClient(info_pair_limit=2)
    session_ids = [uuid.uuid4() for _ in range(4)]
    quality_vid = client.evaluator_versions[client.evaluator_records[0].id].id

    async def fake_wait_for_terminal_tasks(
        _client: Any,
        job_id: uuid.UUID,
        **_kwargs: Any,
    ) -> tuple[JobResponse, list[TaskResponse]]:
        batch_index = [job.id for job in client.created_jobs].index(job_id)
        if batch_index == 0:
            settled = _make_job(JobStatus.COMPLETED, job_id=job_id)
            tasks = [
                _make_task(
                    settled,
                    sid,
                    quality_vid,
                    result=[{"name": "quality", "score": 1.0, "passed": True}],
                )
                for sid in client.requests[0].input_session_ids
            ]
            return settled, tasks
        raise CLIError(
            "timeout",
            f"Timed out waiting for job {job_id}; remote work continues.",
            details={"job_id": str(job_id), "last_status": "running"},
        )

    monkeypatch.setattr(
        evaluations.receipts, "wait_for_terminal_tasks", fake_wait_for_terminal_tasks
    )
    monkeypatch.setattr(evaluations, "emit_event", lambda *_args: None)

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(sid) for sid in session_ids],
            sessions_file=None,
            evaluators=["quality@1"],
            evaluator_params=None,
            wait=True,
            interval=None,
            timeout=10.0,
        )

    assert error.value.kind == "timeout"
    assert error.value.exit_code == 7
    details = error.value.details
    assert details["job_ids"] == [str(job.id) for job in client.created_jobs]
    assert details["settled_job_ids"] == [str(client.created_jobs[0].id)]
    assert details["pending_job_ids"] == [str(client.created_jobs[1].id)]
    assert details["remote_continues"] is True


async def test_wait_transport_error_preserves_submitted_and_settled_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A polling transport error mid-wait raises partial_failure with every job ID."""
    client = BatchEvaluationClient(info_pair_limit=2)
    session_ids = [uuid.uuid4() for _ in range(4)]
    quality_vid = client.evaluator_versions[client.evaluator_records[0].id].id

    async def fake_wait_for_terminal_tasks(
        _client: Any,
        job_id: uuid.UUID,
        **_kwargs: Any,
    ) -> tuple[JobResponse, list[TaskResponse]]:
        batch_index = [job.id for job in client.created_jobs].index(job_id)
        if batch_index == 0:
            settled = _make_job(JobStatus.COMPLETED, job_id=job_id)
            tasks = [
                _make_task(
                    settled,
                    sid,
                    quality_vid,
                    result=[{"name": "quality", "score": 1.0, "passed": True}],
                )
                for sid in client.requests[0].input_session_ids
            ]
            return settled, tasks
        raise httpx.ConnectError("connection reset")

    monkeypatch.setattr(
        evaluations.receipts, "wait_for_terminal_tasks", fake_wait_for_terminal_tasks
    )
    monkeypatch.setattr(evaluations, "emit_event", lambda *_args: None)

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(sid) for sid in session_ids],
            sessions_file=None,
            evaluators=["quality@1"],
            evaluator_params=None,
            wait=True,
            interval=None,
            timeout=10.0,
        )

    assert error.value.kind == "partial_failure"
    details = error.value.details
    assert details["job_ids"] == [str(job.id) for job in client.created_jobs]
    assert details["settled_job_ids"] == [str(client.created_jobs[0].id)]
    assert details["pending_job_ids"] == [str(client.created_jobs[1].id)]


async def test_evaluator_count_exceeding_pair_limit_fails_before_submission() -> None:
    """When even 1 session exceeds the pair limit, fail before creating any job."""
    client = BatchEvaluationClient(info_pair_limit=2)

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(uuid.uuid4())],
            sessions_file=None,
            evaluators=["quality@1", "judge@1", "tone@1"],
            evaluator_params=None,
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert error.value.exit_code == 2
    assert error.value.details == {
        "evaluator_count": 3,
        "evaluation_pair_limit": 2,
    }
    assert client.requests == []


async def test_custom_smaller_server_pair_limit_from_info_is_respected() -> None:
    """A server advertising a smaller evaluation_pair_limit splits accordingly."""
    client = BatchEvaluationClient(info_pair_limit=10, server_enforced_cap=10)
    session_ids = [uuid.uuid4() for _ in range(10)]

    result = await evaluations.evaluate_sessions(
        client,
        [str(sid) for sid in session_ids],
        sessions_file=None,
        evaluators=["quality@1", "judge@1", "tone@1"],
        evaluator_params=None,
        wait=False,
        interval=None,
        timeout=None,
    )

    # Cap 10 with 3 evaluators -> 3 sessions/batch (9 pairs max) -> 3, 3, 3, 1.
    assert [len(req.input_session_ids) for req in client.requests] == [3, 3, 3, 1]
    assert result.item["batch_count"] == 4
    assert len(result.item["job_ids"]) == 4


async def test_older_server_without_info_limit_falls_back_on_422_cap_error() -> None:
    """Older servers omitting evaluation_pair_limit re-chunk after a 422 cap error."""
    client = BatchEvaluationClient(info_pair_limit=None, server_enforced_cap=10)
    session_ids = [uuid.uuid4() for _ in range(15)]

    result = await evaluations.evaluate_sessions(
        client,
        [str(sid) for sid in session_ids],
        sessions_file=None,
        evaluators=["quality@1"],
        evaluator_params=None,
        wait=False,
        interval=None,
        timeout=None,
        idempotency_key="legacy-server",
    )

    # First attempt sent 15 pairs (rejected with cap=10), then retried as 10 + 5.
    assert [len(req.input_session_ids) for req in client.requests] == [15, 10, 5]
    assert client.create_idempotency_keys == [
        "legacy-server",
        "legacy-server:batch:1",
        "legacy-server:batch:2",
    ]
    assert result.item["batch_count"] == 2
    assert len(result.item["job_ids"]) == 2


async def test_older_server_422_cap_below_evaluator_count_fails_cleanly() -> None:
    """An older server cap below len(evaluators) fails with invalid_arguments."""
    client = BatchEvaluationClient(info_pair_limit=None, server_enforced_cap=2)

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            [str(uuid.uuid4())],
            sessions_file=None,
            evaluators=["quality@1", "judge@1", "tone@1"],
            evaluator_params=None,
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert error.value.details == {
        "evaluator_count": 3,
        "evaluation_pair_limit": 2,
    }
    assert len(client.requests) == 1


async def test_overlong_idempotency_key_fails_before_first_batch_submission() -> None:
    """Idempotency key length for derived batch keys is checked before any mutation."""
    client = BatchEvaluationClient(info_pair_limit=2)
    session_ids = [str(uuid.uuid4()) for _ in range(4)]

    with pytest.raises(CLIError) as error:
        await evaluations.evaluate_sessions(
            client,
            session_ids,
            sessions_file=None,
            evaluators=["quality@1"],
            evaluator_params=None,
            wait=False,
            interval=None,
            timeout=None,
            idempotency_key="k" * 250,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.requests == []


def test_session_evaluate_cli_multi_batch_json_and_text_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI argv flow emits multi-batch JSON and renders human text cleanly."""
    client = BatchEvaluationClient(info_pair_limit=2)
    client.selected_sessions = [SimpleNamespace(id=uuid.uuid4()) for _ in range(5)]

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "--output",
                "json",
                "session",
                "evaluate",
                "--tag",
                "imported-baseline",
                "--evaluator",
                "quality@1",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["command"] == "session.evaluate"
    assert payload["ok"] is True
    assert payload["item"]["batch_count"] == 3
    assert payload["item"]["session_count"] == 5
    assert len(payload["item"]["job_ids"]) == 3

    assert (
        app_module.main(
            [
                "--output",
                "text",
                "session",
                "evaluate",
                "--tag",
                "imported-baseline",
                "--evaluator",
                "quality@1",
            ]
        )
        == 0
    )
    text_out = capsys.readouterr().out
    assert "batch_count: 3" in text_out
    assert "job_ids:" in text_out
