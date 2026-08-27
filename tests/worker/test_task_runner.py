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
"""Tests for the TaskRunner status protocol and outcome ranking."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path

import httpx
import pytest
from fakes import (
    FakeKitaruAPIClient,
    as_client,
    make_agent_spec,
    make_claimed,
    make_evaluator_spec,
    make_session_response,
    make_task,
)

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.hook import CommandHook, CopyWorkdirHook
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import PackagePluginSpec, TaskKind, TaskStatus
from kitaru.client.exceptions import (
    APIError,
    AuthenticationError,
    AuthorizationError,
)
from kitaru.worker import task_runner as task_runner_module
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import ProcessResult, TaskProcess
from kitaru.worker.task_runner import MAX_RESULT_BYTES, TaskRunner


def _ctx(tmp_path: Path, client: FakeKitaruAPIClient) -> ExecutionContext:
    return ExecutionContext(
        client=as_client(client),
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )


_NO_RESULT = object()

_FakeRunTaskProcess = Callable[[TaskProcess, asyncio.Event], Awaitable[ProcessResult]]


def _fake_run_task_process(
    returncode: int | None,
    tail: str = "",
    result_content: object = _NO_RESULT,
    result_bytes: bytes | None = None,
) -> _FakeRunTaskProcess:
    """Build a fake run_task_process that writes a scripted result file."""

    async def _fake(process: TaskProcess, canceled: asyncio.Event) -> ProcessResult:
        result_path = Path(process.env["KITARU_TASK_RESULT_PATH"])
        if result_bytes is not None:
            result_path.write_bytes(result_bytes)
        elif result_content is not _NO_RESULT:
            result_path.write_text(json.dumps(result_content))
        return ProcessResult(returncode=returncode, tail=tail)

    return _fake


def _patch_run_task_process(
    monkeypatch: pytest.MonkeyPatch, fake: _FakeRunTaskProcess
) -> None:
    monkeypatch.setattr(task_runner_module, "run_task_process", fake)


async def test_exit_zero_without_a_result_file_completes_with_no_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean exit with no result file completes with a null result."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, status=TaskStatus.CLAIMED, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = make_task(
        kind=TaskKind.AGENT, status=TaskStatus.RUNNING, attempt=1
    ).model_copy(update={"id": task.id})
    completed_task = running_task.model_copy(update={"status": TaskStatus.COMPLETED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(completed_task)

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result.status == TaskStatus.COMPLETED
    _, completed_request = client.tasks.update_calls[-1]
    assert completed_request.status == TaskStatus.COMPLETED
    assert completed_request.result is None


async def test_exit_zero_with_a_result_file_completes_with_the_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean exit with a result file rides the completion as the result."""
    _patch_run_task_process(
        monkeypatch, _fake_run_task_process(0, result_content=[{"name": "quality"}])
    )
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.EVALUATOR, status=TaskStatus.CLAIMED, attempt=1)
    spec = make_agent_spec(task.id)  # process construction is faked, kind unused here
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    completed_task = running_task.model_copy(update={"status": TaskStatus.COMPLETED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(completed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, completed_request = client.tasks.update_calls[-1]
    assert completed_request.result == [{"name": "quality"}]


async def test_oversized_result_fails_the_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A result file over the size cap fails the task instead of completing."""
    _patch_run_task_process(
        monkeypatch,
        _fake_run_task_process(0, result_bytes=b"x" * (MAX_RESULT_BYTES + 1)),
    )
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    failed_task = running_task.model_copy(update={"status": TaskStatus.FAILED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(failed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.FAILED
    assert f"larger than {MAX_RESULT_BYTES} bytes" in request.error


async def test_invalid_json_result_fails_the_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A result file that is not valid JSON fails the task."""
    _patch_run_task_process(
        monkeypatch, _fake_run_task_process(0, result_bytes=b"not json{{")
    )
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    failed_task = running_task.model_copy(update={"status": TaskStatus.FAILED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(failed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.FAILED
    assert "invalid JSON result" in request.error


async def test_nonzero_exit_fails_with_code_and_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A nonzero exit fails the task with its code and the log tail."""
    _patch_run_task_process(
        monkeypatch, _fake_run_task_process(2, tail="stdout tail:\nboom")
    )
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    failed_task = running_task.model_copy(update={"status": TaskStatus.FAILED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(failed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.FAILED
    assert "Agent process exited with code 2." in request.error
    assert "boom" in request.error


async def test_nonzero_exit_attaches_a_readable_result_as_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A nonzero exit that still wrote a readable result attaches it."""
    _patch_run_task_process(
        monkeypatch,
        _fake_run_task_process(1, result_content={"created": 3, "failed": 1}),
    )
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.IMPORTER, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    failed_task = running_task.model_copy(update={"status": TaskStatus.FAILED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(failed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.result == {"created": 3, "failed": 1}


async def test_nonzero_exit_ignores_an_unreadable_result_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A nonzero exit with an unreadable result file fails with no result."""
    _patch_run_task_process(
        monkeypatch, _fake_run_task_process(1, result_bytes=b"not json")
    )
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    failed_task = running_task.model_copy(update={"status": TaskStatus.FAILED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(failed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.result is None


async def test_killed_with_cancel_event_set_cancels_the_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A kill with the cancel event set reports the task as canceled."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(None))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    canceled_task = running_task.model_copy(update={"status": TaskStatus.CANCELED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(canceled_task)

    canceled = asyncio.Event()
    canceled.set()
    await TaskRunner(_ctx(tmp_path, client)).execute(make_claimed(task, spec), canceled)

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.CANCELED
    assert request.error is None


async def test_killed_without_cancel_event_times_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A kill without the cancel event reports the task as timed out."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(None, tail="slow"))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id, timeout_seconds=45)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    timed_out_task = running_task.model_copy(update={"status": TaskStatus.TIMED_OUT})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(timed_out_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.TIMED_OUT
    assert "Task timed out after 45 seconds." in request.error
    assert "slow" in request.error


async def test_exit_before_kill_wins_over_a_requested_cancel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A recorded exit code is reported even when a cancel was requested."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    completed_task = running_task.model_copy(update={"status": TaskStatus.COMPLETED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(completed_task)

    canceled = asyncio.Event()
    canceled.set()
    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), canceled
    )

    assert result.status == TaskStatus.COMPLETED
    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.COMPLETED


async def test_execute_scopes_client_calls_and_process_env_to_the_claimed_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The runner threads the claimed task's token to the client and the process."""
    captured_envs: list[dict[str, str]] = []

    async def _fake(process: TaskProcess, canceled: asyncio.Event) -> ProcessResult:
        captured_envs.append(process.env)
        return ProcessResult(returncode=0, tail="")

    _patch_run_task_process(monkeypatch, _fake)
    client = FakeKitaruAPIClient()
    seen_tokens: list[str] = []
    real_with_token = client.with_token

    def _spy_with_token(token: str) -> FakeKitaruAPIClient:
        seen_tokens.append(token)
        return real_with_token(token)

    monkeypatch.setattr(client, "with_token", _spy_with_token)
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    completed_task = running_task.model_copy(update={"status": TaskStatus.COMPLETED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(completed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec, token="claim-token-xyz"), asyncio.Event()
    )

    assert seen_tokens == ["claim-token-xyz"]
    assert captured_envs[0]["KITARU_API_TOKEN"] == "claim-token-xyz"


async def test_prepare_failure_fails_the_task_with_the_label_and_exception(
    tmp_path: Path,
) -> None:
    """A handler.prepare failure fails the task with a labeled message."""
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    # A set() is not JSON serializable, which fails inside AgentHandler.prepare.
    spec = make_agent_spec(task.id, inputs={1, 2, 3})
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    failed_task = running_task.model_copy(update={"status": TaskStatus.FAILED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(failed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.FAILED
    assert request.error.startswith("Failed to prepare the Agent process:")


async def test_failing_setup_hook_fails_the_task(tmp_path: Path) -> None:
    """A setup hook failure fails the task with the hook type in the error."""
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    # The run spec has no working directory, so the copy_workdir setup raises.
    spec = make_agent_spec(task.id, hooks=[CopyWorkdirHook()])
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    failed_task = running_task.model_copy(update={"status": TaskStatus.FAILED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(failed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.FAILED
    assert request.error.startswith("Hook copy_workdir failed:")


async def test_failing_teardown_command_fails_a_successful_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A teardown command failure fails a task whose process succeeded."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    working_dir = tmp_path / "work"
    working_dir.mkdir()
    spec = make_agent_spec(
        task.id,
        working_dir=str(working_dir),
        hooks=[CommandHook(command="exit 3", when="teardown")],
    )
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    failed_task = running_task.model_copy(update={"status": TaskStatus.FAILED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(failed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.FAILED
    assert "Hook command failed:" in request.error


async def test_copy_workdir_hook_runs_the_process_in_the_copied_directory(
    tmp_path: Path,
) -> None:
    """A copy_workdir hook runs the task process in the copy, not the original."""
    client = FakeKitaruAPIClient()
    original = tmp_path / "original"
    original.mkdir()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    command = 'touch marker.txt && printf \'"%s"\' "$PWD" > "$KITARU_TASK_RESULT_PATH"'
    spec = make_agent_spec(
        task.id,
        command=command,
        working_dir=str(original),
        hooks=[CopyWorkdirHook()],
    )
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    completed_task = running_task.model_copy(update={"status": TaskStatus.COMPLETED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(completed_task)

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.COMPLETED
    assert request.result.endswith("hook-0-workdir")
    assert not (original / "marker.txt").exists()


async def test_running_transition_conflict_abandons_without_running_the_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 409 on the running transition abandons the attempt immediately."""
    called = False

    async def _unexpected_run(
        process: TaskProcess, canceled: asyncio.Event
    ) -> ProcessResult:
        nonlocal called
        called = True
        return ProcessResult(returncode=0, tail="")

    _patch_run_task_process(monkeypatch, _unexpected_run)
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    client.tasks.update_responses.append(APIError(409, "stale attempt"))

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result == task
    assert called is False
    assert len(client.tasks.update_calls) == 1


async def test_running_transition_hard_failure_abandons_the_attempt(
    tmp_path: Path,
) -> None:
    """A hard transport failure on the running transition abandons the attempt."""
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    client.tasks.update_responses.append(httpx.ConnectError("down"))

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result == task
    assert len(client.tasks.update_calls) == 1


@pytest.mark.parametrize(
    "error",
    [
        AuthenticationError(401, "token rejected"),
        AuthorizationError(403, "token rejected"),
    ],
)
async def test_running_transition_token_rejection_abandons_without_running_the_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: APIError
) -> None:
    """A rejected token on the running transition abandons the attempt immediately."""
    called = False

    async def _unexpected_run(
        process: TaskProcess, canceled: asyncio.Event
    ) -> ProcessResult:
        nonlocal called
        called = True
        return ProcessResult(returncode=0, tail="")

    _patch_run_task_process(monkeypatch, _unexpected_run)
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    client.tasks.update_responses.append(error)

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result == task
    assert called is False
    assert len(client.tasks.update_calls) == 1


async def test_completion_conflict_with_reclaimed_attempt_drops_the_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A completion conflict on a superseded attempt only refetches the task."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(APIError(409, "conflict"))
    reclaimed_task = task.model_copy(
        update={"attempt": 2, "status": TaskStatus.RUNNING}
    )
    client.tasks.get_responses.append(reclaimed_task)

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result == reclaimed_task
    # Only the running PATCH and the rejected completion PATCH were sent, no
    # failed transition follows a superseded attempt.
    assert len(client.tasks.update_calls) == 2


async def test_completion_conflict_agent_without_result_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An agent completion conflict with no linked session names that fact."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(APIError(409, "conflict"))
    client.tasks.get_responses.append(running_task)
    client.sessions.list_responses.append(Page(items=[], next_cursor=None))
    client.tasks.update_responses.append(
        running_task.model_copy(update={"status": TaskStatus.FAILED})
    )

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.status == TaskStatus.FAILED
    assert request.error == (
        "Agent process exited successfully without recording a result session."
    )


async def test_completion_conflict_agent_with_incomplete_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An agent completion conflict with a non-completed session names its status."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(APIError(409, "conflict"))
    client.tasks.get_responses.append(running_task)
    session = make_session_response(status=SessionStatus.FAILED)
    client.sessions.list_responses.append(Page(items=[session], next_cursor=None))
    client.tasks.update_responses.append(
        running_task.model_copy(update={"status": TaskStatus.FAILED})
    )

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.error == f"Result session {session.id} is failed, not completed."


async def test_completion_conflict_non_agent_missing_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An evaluator/importer completion conflict names the missing result."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.EVALUATOR, attempt=1)
    plugin = PackagePluginSpec(entrypoint="pkg.mod:evaluate", requirement="pkg==1.0")
    spec = make_evaluator_spec(task.id, plugin=plugin)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(APIError(409, "conflict"))
    client.tasks.get_responses.append(running_task)
    client.tasks.update_responses.append(
        running_task.model_copy(update={"status": TaskStatus.FAILED})
    )

    await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    _, request = client.tasks.update_calls[-1]
    assert request.error == (
        "Evaluator process exited successfully without writing a result."
    )


async def test_completion_conflict_transport_failure_on_the_refetch_abandons(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hard failure fetching the task after a completion conflict abandons."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(APIError(409, "conflict"))
    client.tasks.get_responses.append(httpx.ConnectError("down"))

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result == running_task
    assert len(client.tasks.update_calls) == 2


@pytest.mark.parametrize(
    "error",
    [
        AuthenticationError(401, "token rejected"),
        AuthorizationError(403, "token rejected"),
    ],
)
async def test_completion_conflict_token_rejection_on_the_refetch_abandons(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: APIError
) -> None:
    """A rejected token fetching the task after a completion conflict abandons."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(APIError(409, "conflict"))
    client.tasks.get_responses.append(error)

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result == running_task
    assert len(client.tasks.update_calls) == 2


async def test_completion_hard_failure_returns_the_running_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-409 completion failure logs and abandons, keeping the running task."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(APIError(500, "boom"))

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result == running_task
    assert len(client.tasks.update_calls) == 2


@pytest.mark.parametrize(
    "error",
    [
        AuthenticationError(401, "token rejected"),
        AuthorizationError(403, "token rejected"),
    ],
)
async def test_completion_token_rejection_returns_the_running_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: APIError
) -> None:
    """A rejected token on completion abandons, keeping the running task."""
    _patch_run_task_process(monkeypatch, _fake_run_task_process(0))
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT, attempt=1)
    spec = make_agent_spec(task.id)
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(error)

    result = await TaskRunner(_ctx(tmp_path, client)).execute(
        make_claimed(task, spec), asyncio.Event()
    )

    assert result == running_task
    assert len(client.tasks.update_calls) == 2
