"""Task runner status and result protocol tests."""

import asyncio
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import (
    AgentTaskDetails,
    TaskKind,
    TaskResponse,
    TaskSpecResponse,
    TaskStatus,
    TaskWithSpec,
)
from kitaru.client.exceptions import APIError
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import ProcessResult, TaskProcess
from kitaru.worker.task_runner import TaskRunner


def make_task(
    *,
    kind: TaskKind = TaskKind.AGENT,
    status: TaskStatus = TaskStatus.CLAIMED,
    attempt: int = 2,
    result_session_id: uuid.UUID | None = None,
) -> TaskResponse:
    return TaskResponse.model_construct(
        id=uuid.uuid4(),
        kind=kind,
        status=status,
        attempt=attempt,
        result_session_id=result_session_id,
    )


def make_claim(task: TaskResponse) -> TaskWithSpec:
    details = AgentTaskDetails(kind="agent", inputs={})
    return TaskWithSpec.model_construct(
        task=task,
        spec=TaskSpecResponse.model_construct(
            task_id=task.id,
            kind=task.kind,
            timeout_seconds=9,
            run=None,
            env={},
            secret_env={},
            details=details,
        ),
    )


class FakeTasks:
    def __init__(self, task: TaskResponse) -> None:
        self.task = task
        self.requests = []
        self.conflict_on_completed = False
        self.fail_all = False

    async def update(self, task_id, request):
        assert task_id == self.task.id
        self.requests.append(request)
        if self.fail_all:
            raise APIError(500, "unavailable")
        if self.conflict_on_completed and request.status == TaskStatus.COMPLETED:
            raise APIError(409, "rejected")
        self.task = self.task.model_copy(
            update={
                "status": request.status,
                "error": request.error,
                "result": request.result,
            }
        )
        return self.task

    async def get(self, task_id):
        assert task_id == self.task.id
        return self.task


class FakeHandler:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def prepare(self, ctx, task_id, spec):
        if self.error is not None:
            raise self.error
        return TaskProcess("unused", None, {}, spec.timeout_seconds)


def make_runner(
    tmp_path: Path, task: TaskResponse, sessions: Any | None = None
) -> tuple[TaskRunner, FakeTasks]:
    tasks = FakeTasks(task)
    client = cast(
        Any,
        SimpleNamespace(
            tasks=tasks,
            sessions=sessions or SimpleNamespace(),
        ),
    )
    ctx = ExecutionContext(
        client=client,
        blob_cache=BlobCache(tmp_path / "code"),
        payload_cache=BlobCache(tmp_path / "payload"),
    )
    return TaskRunner(ctx), tasks


async def test_prepare_failure_marks_task_failed(tmp_path, monkeypatch) -> None:
    task = make_task()
    runner, tasks = make_runner(tmp_path, task)
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.AGENT,
        FakeHandler(ValueError("bad command")),
    )

    result = await runner.execute(make_claim(task), asyncio.Event())

    assert result.status == TaskStatus.FAILED
    assert tasks.requests[-1].attempt == task.attempt
    assert (
        tasks.requests[-1].error == "Failed to prepare the Agent process: bad command"
    )


async def test_success_reads_json_result_and_completes(tmp_path, monkeypatch) -> None:
    task = make_task()
    runner, tasks = make_runner(tmp_path, task)
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.AGENT,
        FakeHandler(),
    )

    async def run(process, canceled):
        Path(process.env["KITARU_TASK_RESULT_PATH"]).write_text(
            '{"value": 3}', encoding="utf-8"
        )
        return ProcessResult(0, "")

    monkeypatch.setattr("kitaru.worker.task_runner.run_task_process", run)

    result = await runner.execute(make_claim(task), asyncio.Event())

    assert result.status == TaskStatus.COMPLETED
    assert tasks.requests[-1].result == {"value": 3}
    assert [request.attempt for request in tasks.requests] == [2, 2]


async def test_invalid_success_result_marks_task_failed(tmp_path, monkeypatch) -> None:
    task = make_task(kind=TaskKind.EVALUATOR)
    claim = make_claim(task)
    claim.spec.kind = TaskKind.EVALUATOR
    runner, tasks = make_runner(tmp_path, task)
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.EVALUATOR,
        FakeHandler(),
    )

    async def run(process, canceled):
        Path(process.env["KITARU_TASK_RESULT_PATH"]).write_text(
            "not-json", encoding="utf-8"
        )
        return ProcessResult(0, "")

    monkeypatch.setattr("kitaru.worker.task_runner.run_task_process", run)

    result = await runner.execute(claim, asyncio.Event())

    assert result.status == TaskStatus.FAILED
    assert tasks.requests[-1].error == (
        "Evaluator process wrote an invalid JSON result."
    )


async def test_oversized_success_result_marks_task_failed(
    tmp_path, monkeypatch
) -> None:
    task = make_task(kind=TaskKind.IMPORTER)
    claim = make_claim(task)
    claim.spec.kind = TaskKind.IMPORTER
    runner, tasks = make_runner(tmp_path, task)
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.IMPORTER,
        FakeHandler(),
    )
    monkeypatch.setattr("kitaru.worker.task_runner.MAX_RESULT_BYTES", 2)

    async def run(process, canceled):
        Path(process.env["KITARU_TASK_RESULT_PATH"]).write_text(
            '{"created": 3}', encoding="utf-8"
        )
        return ProcessResult(0, "")

    monkeypatch.setattr("kitaru.worker.task_runner.run_task_process", run)

    result = await runner.execute(claim, asyncio.Event())

    assert result.status == TaskStatus.FAILED
    assert tasks.requests[-1].error == (
        "Importer process wrote a result larger than 2 bytes."
    )


async def test_nonzero_exit_includes_tail_and_partial_result(
    tmp_path, monkeypatch
) -> None:
    task = make_task(kind=TaskKind.IMPORTER)
    claim = make_claim(task)
    claim.spec.kind = TaskKind.IMPORTER
    runner, tasks = make_runner(tmp_path, task)
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.IMPORTER,
        FakeHandler(),
    )

    async def run(process, canceled):
        Path(process.env["KITARU_TASK_RESULT_PATH"]).write_text(
            json.dumps({"created": 2, "failed": 1}), encoding="utf-8"
        )
        return ProcessResult(4, "stderr tail:\nparse failed")

    monkeypatch.setattr("kitaru.worker.task_runner.run_task_process", run)

    result = await runner.execute(claim, asyncio.Event())

    assert result.status == TaskStatus.FAILED
    assert tasks.requests[-1].result == {"created": 2, "failed": 1}
    assert tasks.requests[-1].error == (
        "Importer process exited with code 4.\nstderr tail:\nparse failed"
    )


async def test_cancel_event_wins_for_killed_process(tmp_path, monkeypatch) -> None:
    task = make_task()
    runner, tasks = make_runner(tmp_path, task)
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.AGENT,
        FakeHandler(),
    )
    monkeypatch.setattr(
        "kitaru.worker.task_runner.run_task_process",
        lambda process, canceled: _async_result(ProcessResult(None, "")),
    )
    canceled = asyncio.Event()
    canceled.set()

    result = await runner.execute(make_claim(task), canceled)

    assert result.status == TaskStatus.CANCELED
    assert tasks.requests[-1].attempt == task.attempt


async def test_killed_process_without_cancel_times_out(tmp_path, monkeypatch) -> None:
    task = make_task()
    runner, tasks = make_runner(tmp_path, task)
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.AGENT,
        FakeHandler(),
    )
    monkeypatch.setattr(
        "kitaru.worker.task_runner.run_task_process",
        lambda process, canceled: _async_result(
            ProcessResult(None, "stdout tail:\nworking")
        ),
    )

    result = await runner.execute(make_claim(task), asyncio.Event())

    assert result.status == TaskStatus.TIMED_OUT
    assert tasks.requests[-1].error == (
        "Task timed out after 9 seconds.\nstdout tail:\nworking"
    )


async def test_completion_conflict_with_missing_agent_session_fails_precisely(
    tmp_path, monkeypatch
) -> None:
    task = make_task()
    runner, tasks = make_runner(tmp_path, task)
    tasks.conflict_on_completed = True
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.AGENT,
        FakeHandler(),
    )
    monkeypatch.setattr(
        "kitaru.worker.task_runner.run_task_process",
        lambda process, canceled: _async_result(ProcessResult(0, "")),
    )

    result = await runner.execute(make_claim(task), asyncio.Event())

    assert result.status == TaskStatus.FAILED
    assert tasks.requests[-1].error == (
        "Agent process exited successfully without recording a result session."
    )


async def test_completion_conflict_reports_incomplete_result_session(
    tmp_path, monkeypatch
) -> None:
    session_id = uuid.uuid4()
    task = make_task(result_session_id=session_id)

    class Sessions:
        async def get(self, requested):
            assert requested == session_id
            return SimpleNamespace(id=session_id, status=SessionStatus.IN_PROGRESS)

    runner, tasks = make_runner(tmp_path, task, Sessions())
    tasks.conflict_on_completed = True
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.AGENT,
        FakeHandler(),
    )
    monkeypatch.setattr(
        "kitaru.worker.task_runner.run_task_process",
        lambda process, canceled: _async_result(ProcessResult(0, "")),
    )

    result = await runner.execute(make_claim(task), asyncio.Event())

    assert result.status == TaskStatus.FAILED
    assert tasks.requests[-1].error == (
        f"Result session {session_id} is in_progress, not completed."
    )


async def test_lost_completion_fence_does_not_write_failure(
    tmp_path, monkeypatch
) -> None:
    task = make_task()
    runner, tasks = make_runner(tmp_path, task)
    tasks.conflict_on_completed = True
    monkeypatch.setitem(
        __import__("kitaru.worker.task_runner", fromlist=["HANDLERS"]).HANDLERS,
        TaskKind.AGENT,
        FakeHandler(),
    )

    async def run(process, canceled):
        tasks.task = tasks.task.model_copy(update={"attempt": 3})
        return ProcessResult(0, "")

    monkeypatch.setattr("kitaru.worker.task_runner.run_task_process", run)

    result = await runner.execute(make_claim(task), asyncio.Event())

    assert result.attempt == 3
    assert [request.status for request in tasks.requests] == [
        TaskStatus.RUNNING,
        TaskStatus.COMPLETED,
    ]


async def test_running_write_failure_abandons_attempt(tmp_path, monkeypatch) -> None:
    task = make_task()
    runner, tasks = make_runner(tmp_path, task)
    tasks.fail_all = True
    called = False

    async def run(process, canceled):
        nonlocal called
        called = True
        return ProcessResult(0, "")

    monkeypatch.setattr("kitaru.worker.task_runner.run_task_process", run)

    result = await runner.execute(make_claim(task), asyncio.Event())

    assert result is task
    assert not called
    assert len(tasks.requests) == 1


async def _async_result(result: ProcessResult) -> ProcessResult:
    return result
