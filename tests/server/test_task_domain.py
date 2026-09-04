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
"""Tests for the task and job domain entities."""

import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.task import TaskKind, TaskOnFailure, TaskStatus
from kitaru.server.domain.job import IllegalJobStatusTransition, Job
from kitaru.server.domain.task import (
    AgentTask,
    AnalysisTask,
    EvaluationTask,
    IllegalTaskStatusTransition,
    ImportTask,
    InvalidTaskEnv,
    InvalidTaskResult,
    Task,
    TaskAttemptMismatch,
    TaskNotRunning,
)

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
WORKER_ID = uuid.uuid4()

# Every legal cell of the task transition table, keyed by (from, to).
TRANSITIONS: dict[tuple[TaskStatus, TaskStatus], Callable[[Task], None]] = {
    (TaskStatus.PENDING, TaskStatus.CLAIMED): lambda task: task.claim(WORKER_ID, NOW),
    (TaskStatus.PENDING, TaskStatus.CANCELED): lambda task: task.request_cancel(NOW),
    (TaskStatus.CLAIMED, TaskStatus.PENDING): lambda task: task.requeue(),
    (TaskStatus.CLAIMED, TaskStatus.RUNNING): lambda task: task.start(NOW),
    (TaskStatus.CLAIMED, TaskStatus.FAILED): lambda task: task.fail("boom", None, NOW),
    (TaskStatus.CLAIMED, TaskStatus.CANCELED): lambda task: task.cancel(NOW),
    (TaskStatus.CLAIMED, TaskStatus.ABANDONED): lambda task: task.abandon("gone", NOW),
    (TaskStatus.RUNNING, TaskStatus.PENDING): lambda task: task.requeue(),
    (TaskStatus.RUNNING, TaskStatus.COMPLETED): lambda task: task.complete(
        {"created": 0}, NOW
    ),
    (TaskStatus.RUNNING, TaskStatus.FAILED): lambda task: task.fail("boom", None, NOW),
    (TaskStatus.RUNNING, TaskStatus.TIMED_OUT): lambda task: task.time_out("slow", NOW),
    (TaskStatus.RUNNING, TaskStatus.CANCELED): lambda task: task.cancel(NOW),
    (TaskStatus.RUNNING, TaskStatus.ABANDONED): lambda task: task.abandon("gone", NOW),
}

TERMINAL_STATUSES = [
    TaskStatus.COMPLETED,
    TaskStatus.FAILED,
    TaskStatus.TIMED_OUT,
    TaskStatus.CANCELED,
    TaskStatus.ABANDONED,
]

# Every status reached by some legal transition, used to probe that a
# terminal task rejects every one of them.
TRANSITION_TARGETS = {target for (_, target) in TRANSITIONS}

ILLEGAL_TRANSITIONS = [
    (TaskStatus.PENDING, TaskStatus.RUNNING),
    (TaskStatus.PENDING, TaskStatus.COMPLETED),
    (TaskStatus.PENDING, TaskStatus.FAILED),
    (TaskStatus.PENDING, TaskStatus.TIMED_OUT),
    (TaskStatus.PENDING, TaskStatus.ABANDONED),
    (TaskStatus.CLAIMED, TaskStatus.CLAIMED),
    (TaskStatus.CLAIMED, TaskStatus.COMPLETED),
    (TaskStatus.CLAIMED, TaskStatus.TIMED_OUT),
    (TaskStatus.RUNNING, TaskStatus.CLAIMED),
    (TaskStatus.RUNNING, TaskStatus.RUNNING),
    *[
        (terminal, target)
        for terminal in TERMINAL_STATUSES
        for target in TRANSITION_TARGETS
    ],
]


def _task(status: TaskStatus = TaskStatus.PENDING, **overrides: Any) -> ImportTask:
    """Build an import task in a given status.

    Args:
        status: Status the task starts in.
        overrides: Extra field values.

    Returns:
        Import task.
    """
    return ImportTask(
        job_id=uuid.uuid4(),
        import_id=uuid.uuid4(),
        status=status,
        **overrides,
    )


def test_kinds() -> None:
    """Each subclass reports its wire kind."""
    agent = AgentTask(job_id=uuid.uuid4(), agent_version_id=uuid.uuid4())
    evaluator = EvaluationTask(
        job_id=uuid.uuid4(),
        plugin_version_id=uuid.uuid4(),
        input_session_id=uuid.uuid4(),
    )
    analyzer = AnalysisTask(
        job_id=uuid.uuid4(),
        plugin_version_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        input_session_ids=[uuid.uuid4()],
    )
    assert agent.kind is TaskKind.AGENT
    assert evaluator.kind is TaskKind.EVALUATOR
    assert analyzer.kind is TaskKind.ANALYZER
    assert _task().kind is TaskKind.IMPORTER


@pytest.mark.parametrize(("origin", "target"), list(TRANSITIONS))
def test_legal_transitions(origin: TaskStatus, target: TaskStatus) -> None:
    """Every legal cell of the transition table applies."""
    task = _task(origin)
    TRANSITIONS[(origin, target)](task)
    assert task.status is target


@pytest.mark.parametrize(("origin", "target"), ILLEGAL_TRANSITIONS)
def test_illegal_transitions(origin: TaskStatus, target: TaskStatus) -> None:
    """Every illegal cell of the transition table conflicts."""
    transition = TRANSITIONS.get((origin, target))
    if transition is None:
        transition = next(
            apply for (_, cell), apply in TRANSITIONS.items() if cell is target
        )
    task = _task(origin)
    with pytest.raises(IllegalTaskStatusTransition):
        transition(task)
    assert task.status is origin


def test_claim_increments_the_attempt() -> None:
    """A claim takes the next attempt and stamps the worker."""
    task = _task()
    task.claim(WORKER_ID, NOW)
    assert task.attempt == 1
    assert task.worker_id == WORKER_ID
    assert task.claimed_at == NOW


def test_requeue_drops_the_attempt_state() -> None:
    """A requeue clears everything the stale attempt wrote."""
    task = _task(TaskStatus.RUNNING, attempt=1)
    task.worker_id = WORKER_ID
    task.claimed_at = NOW
    task.heartbeat_at = NOW
    task.started_at = NOW
    task.requeue()
    assert task.attempt == 1
    assert task.worker_id is None
    assert task.claimed_at is None
    assert task.heartbeat_at is None
    assert task.started_at is None


def test_request_cancel_leaves_an_in_flight_status_alone() -> None:
    """A cancel request stamps a running task without moving it."""
    task = _task(TaskStatus.RUNNING)
    task.request_cancel(NOW)
    assert task.status is TaskStatus.RUNNING
    assert task.cancel_requested_at == NOW


def test_request_cancel_is_idempotent() -> None:
    """A second cancel request keeps the first stamp."""
    later = NOW + timedelta(minutes=1)
    task = _task(TaskStatus.RUNNING)
    task.request_cancel(NOW)
    task.request_cancel(later)
    assert task.cancel_requested_at == NOW


def test_check_attempt_rejects_a_stale_fence() -> None:
    """A fencing attempt the task moved past conflicts."""
    task = _task(TaskStatus.RUNNING, attempt=2)
    task.check_attempt(2)
    with pytest.raises(TaskAttemptMismatch):
        task.check_attempt(1)
    with pytest.raises(TaskAttemptMismatch):
        task.check_attempt(None)


def test_contract_env_names_are_rejected() -> None:
    """Env extras naming a contract variable fail validation."""
    for name in (
        "KITARU_API_URL",
        "KITARU_API_KEY",
        "KITARU_REPLAY_ID",
        "KITARU_TASK_RESULT_PATH",
    ):
        with pytest.raises(InvalidTaskEnv):
            _task(env={name: "x"})


def test_creator_env_extras_pass() -> None:
    """Env extras outside the contract namespace are kept."""
    task = _task(env={"KITARU_SESSION_NAME": "run-1"})
    assert task.env == {"KITARU_SESSION_NAME": "run-1"}


@pytest.mark.parametrize(
    "result",
    [
        None,
        [],
        "not-a-list",
        [{"score": 1.0}],
        [{"name": "accuracy"}],
        [{"name": "accuracy", "score": 1.0}, {"name": "accuracy", "score": 0.5}],
    ],
)
def test_evaluator_result_validation(result: object) -> None:
    """An evaluator result must be a non-empty list of uniquely named results."""
    task = EvaluationTask(
        job_id=uuid.uuid4(),
        plugin_version_id=uuid.uuid4(),
        input_session_id=uuid.uuid4(),
        status=TaskStatus.RUNNING,
    )
    with pytest.raises(InvalidTaskResult):
        task.complete(result, NOW)
    assert task.status is TaskStatus.RUNNING


def test_evaluator_result_accepted() -> None:
    """A valid evaluator result completes the task and is stored."""
    result = [{"name": "accuracy", "score": 0.9}, {"name": "tone", "value": "warm"}]
    task = EvaluationTask(
        job_id=uuid.uuid4(),
        plugin_version_id=uuid.uuid4(),
        input_session_id=uuid.uuid4(),
        status=TaskStatus.RUNNING,
    )
    task.complete(result, NOW)
    assert task.status is TaskStatus.COMPLETED
    assert task.result == result


_INSIGHT = {"name": "trend", "title": "Trend", "data": {"type": "text", "content": "x"}}


def _analysis_task(**overrides: Any) -> AnalysisTask:
    """Build an analysis task in the running status.

    Args:
        overrides: Extra field values.

    Returns:
        Analysis task.
    """
    return AnalysisTask(
        job_id=uuid.uuid4(),
        plugin_version_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        input_session_ids=[uuid.uuid4()],
        status=TaskStatus.RUNNING,
        **overrides,
    )


@pytest.mark.parametrize(
    "result",
    [
        None,
        [],
        "not-a-list",
        [{"title": "Trend", "data": {"type": "text", "content": "x"}}],
        [{"name": "trend", "data": {"type": "text", "content": "x"}}],
        [_INSIGHT, _INSIGHT],
    ],
)
def test_analyzer_result_validation(result: object) -> None:
    """An analyzer result must be a non-empty list of uniquely named results."""
    task = _analysis_task()
    with pytest.raises(InvalidTaskResult):
        task.complete(result, NOW)
    assert task.status is TaskStatus.RUNNING


def test_analyzer_result_accepted() -> None:
    """A valid analyzer result completes the task and is stored."""
    result = [
        _INSIGHT,
        {
            "name": "outliers",
            "title": "Outliers",
            "data": {"type": "text", "content": "y"},
        },
    ]
    task = _analysis_task()
    task.complete(result, NOW)
    assert task.status is TaskStatus.COMPLETED
    assert task.result == result


def test_importer_result_required() -> None:
    """An importer task cannot complete without a result."""
    task = _task(TaskStatus.RUNNING)
    with pytest.raises(InvalidTaskResult):
        task.complete(None, NOW)
    task.complete({"created": 2}, NOW)
    assert task.result == {"created": 2}


def test_failure_stores_a_diagnostic_result() -> None:
    """A failure keeps whatever partial result the executor reported."""
    task = _task(TaskStatus.RUNNING)
    task.fail("boom", {"created": 1, "failed": 3}, NOW)
    assert task.status is TaskStatus.FAILED
    assert task.result == {"created": 1, "failed": 3}
    assert task.error == "boom"


def test_check_running() -> None:
    """A session create only names a running task."""
    _task(TaskStatus.RUNNING).check_running()
    with pytest.raises(TaskNotRunning):
        _task(TaskStatus.CLAIMED).check_running()


def test_counted_hard_failure_skips_ignore_tasks() -> None:
    """An ignore task's failure does not count toward the job outcome."""
    counted = _task(TaskStatus.FAILED, on_failure=TaskOnFailure.CONTINUE)
    ignored = _task(TaskStatus.FAILED, on_failure=TaskOnFailure.IGNORE)
    assert counted.counted_hard_failure is True
    assert ignored.counted_hard_failure is False
    assert _task(TaskStatus.CANCELED).counted_hard_failure is False


def test_is_stale_reads_the_heartbeat_then_the_claim() -> None:
    """Staleness rides the heartbeat, falling back to the claim time."""
    task = _task(TaskStatus.CLAIMED, claimed_at=NOW)
    assert task.is_stale(NOW + timedelta(seconds=30), 60) is False
    assert task.is_stale(NOW + timedelta(seconds=90), 60) is True
    task.heartbeat_at = NOW + timedelta(seconds=80)
    assert task.is_stale(NOW + timedelta(seconds=90), 60) is False
    assert _task(TaskStatus.PENDING).is_stale(NOW, 60) is False


def test_with_staleness_reports_the_effective_status() -> None:
    """A stale task reads as whatever the next sweep will write."""
    late = NOW + timedelta(seconds=120)
    requeued = _task(TaskStatus.CLAIMED, claimed_at=NOW, attempt=1)
    abandoned = _task(TaskStatus.CLAIMED, claimed_at=NOW, attempt=3)
    canceled = _task(
        TaskStatus.RUNNING, claimed_at=NOW, attempt=1, cancel_requested_at=NOW
    )
    fresh = _task(TaskStatus.CLAIMED, claimed_at=late, attempt=1)
    assert requeued.with_staleness(late, 60, 3).status is TaskStatus.PENDING
    assert abandoned.with_staleness(late, 60, 3).status is TaskStatus.ABANDONED
    assert canceled.with_staleness(late, 60, 3).status is TaskStatus.CANCELED
    assert fresh.with_staleness(late, 60, 3).status is TaskStatus.CLAIMED


def test_job_lifecycle() -> None:
    """A job starts once, settles once, and keeps its first cancel request."""
    job = Job(owner_id=uuid.uuid4(), kind=JobKind.SESSION_RUN)
    job.start(NOW)
    assert job.status is JobStatus.RUNNING
    assert job.started_at == NOW
    with pytest.raises(IllegalJobStatusTransition):
        job.start(NOW)
    job.settle(JobStatus.FAILED, "boom", NOW)
    assert job.settled is True
    assert job.error == "boom"
    with pytest.raises(IllegalJobStatusTransition):
        job.settle(JobStatus.COMPLETED, None, NOW)


def test_job_settle_rejects_a_non_terminal_status() -> None:
    """Settlement only writes terminal job statuses."""
    job = Job(owner_id=uuid.uuid4(), kind=JobKind.SESSION_RUN)
    with pytest.raises(IllegalJobStatusTransition):
        job.settle(JobStatus.RUNNING, None, NOW)
