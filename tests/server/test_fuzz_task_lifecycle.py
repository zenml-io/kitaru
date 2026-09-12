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
"""Isolated task-attempt lifecycle properties against PostgreSQL.

Every example starts a real API lifespan on a fresh database. The model below
tracks only documented task states and attempts, independently of the domain
objects that implement them. Receipts retain symbolic ids and credential roles
so a shrunk failure can be replayed without keeping live database values.
"""

import asyncio
import os
import re
import uuid
from contextlib import aclosing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fuzz_sequences import (
    CredentialRole,
    SequenceAction,
    SequenceReceipt,
    SequenceRuntime,
    annotate_sequence_failure,
    isolate_sequence,
)
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.rest.dependencies import (
    get_server_analytics,
    get_task_service,
)
from kitaru.server.database.service import DatabaseService

_SEQUENCES_ENABLED = os.environ.get("KITARU_FUZZ_TASK_LIFECYCLE") == "1"
if not _SEQUENCES_ENABLED:
    pytest.skip(
        "Task lifecycle fuzzing is opt-in; set KITARU_FUZZ_TASK_LIFECYCLE=1 "
        "(needs docker compose up -d db)",
        allow_module_level=True,
    )

_UUID_PATTERN = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_SCOPE = {"claims": [{"kind": "agent"}]}
_RUNTIME = {"platform": "bare"}
_REPLAY_ACTION = "_replay_action"
_SEQUENCE_TIMEOUT_SECONDS = 30


def _get_positive_int(name: str, default: int, *, maximum: int | None = None) -> int:
    """Read a positive bounded integer from the environment."""
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise pytest.UsageError(f"{name} must be a positive integer") from exc
    if value < 1 or (maximum is not None and value > maximum):
        bound = f" no greater than {maximum}" if maximum is not None else ""
        raise pytest.UsageError(f"{name} must be a positive integer{bound}")
    return value


MAX_EXAMPLES = _get_positive_int("KITARU_FUZZ_TASK_LIFECYCLE_MAX_EXAMPLES", 25)
MAX_ACTIONS = _get_positive_int(
    "KITARU_FUZZ_TASK_LIFECYCLE_MAX_ACTIONS", 10, maximum=10
)
if MAX_ACTIONS < 2:
    raise pytest.UsageError("KITARU_FUZZ_TASK_LIFECYCLE_MAX_ACTIONS must be at least 2")

SEQUENCE_SETTINGS = settings(
    max_examples=MAX_EXAMPLES,
    deadline=None,
    derandomize=os.environ.get("KITARU_FUZZ_RANDOM") is None,
    suppress_health_check=[HealthCheck.too_slow],
)


@dataclass
class TaskModel:
    """Independent expected state for the target task and its job."""

    status: str = "claimed"
    attempt: int = 1
    job_status: str = "running"
    cancel_requested: bool = False
    has_result_session: bool = False


def _build_action(name: str, **arguments: Any) -> SequenceAction:
    """Build one replayable lifecycle action."""
    return SequenceAction(
        name=name,
        target="task",
        arguments={_REPLAY_ACTION: True, **arguments},
    )


@st.composite
def _task_attempt_sequences(draw: st.DrawFn) -> list[SequenceAction]:
    """Generate bounded action lists with valid task-state prerequisites."""
    count = draw(st.integers(min_value=2, max_value=MAX_ACTIONS))
    model = TaskModel()
    actions: list[SequenceAction] = []

    while len(actions) < count:
        if model.status not in {"claimed", "running"}:
            choices = ["repeat_terminal", "foreign_start"]
        elif model.cancel_requested and model.status == "claimed":
            choices = ["acknowledge_cancel", "foreign_start"]
        elif model.cancel_requested:
            choices = ["acknowledge_cancel", "complete", "foreign_start"]
        elif model.status == "claimed":
            choices = ["start", "fail", "cancel_job", "foreign_start"]
            if model.attempt < 3:
                choices.append("stale_reclaim")
            else:
                choices.append("stale_exhaust")
        elif model.status == "running":
            choices = ["complete", "fail", "cancel_job", "foreign_start"]
            if not model.has_result_session:
                choices.append("link_result_session")
            if model.attempt < 3:
                choices.append("stale_reclaim")
            else:
                choices.append("stale_exhaust")
        name = draw(st.sampled_from(choices))
        actions.append(_build_action(name))
        if name == "start":
            model.status = "running"
        elif name == "complete":
            model.status = "completed"
            model.job_status = "completed"
            model.has_result_session = True
        elif name == "link_result_session":
            model.has_result_session = True
        elif name == "fail":
            model.status = "failed"
            model.job_status = "failed"
        elif name == "cancel_job":
            model.cancel_requested = True
        elif name == "acknowledge_cancel":
            model.status = "canceled"
            model.job_status = "canceled"
        elif name == "stale_reclaim":
            model.attempt += 1
            model.status = "running"
            model.has_result_session = True
        elif name == "stale_exhaust":
            model.status = "abandoned"
            model.job_status = "failed"

    return actions


def _get_role_for_attempt(attempt: int) -> CredentialRole:
    """Return the symbolic credential role for one bounded retry attempt."""
    return {
        1: CredentialRole.TASK_ATTEMPT_1,
        2: CredentialRole.TASK_ATTEMPT_2,
        3: CredentialRole.TASK_ATTEMPT_3,
    }[attempt]


def _record(
    runtime: SequenceRuntime,
    action: SequenceAction,
    role: CredentialRole,
    response: httpx.Response,
    *invariants: str,
) -> None:
    """Record a response before checking its expected contract."""
    runtime.record_response(
        action=action,
        credential_role=role,
        response=response,
        invariants=invariants,
    )


async def _bootstrap(runtime: SequenceRuntime) -> TaskModel:
    """Create one target task, one foreign task, and bind their credentials."""
    account = CredentialRole.ACCOUNT
    account_headers = runtime.get_headers(account)

    agent_action = SequenceAction(name="create_agent", target="agent")
    response = await runtime.client.post(
        "/api/v1/agents", json={"name": "task-lifecycle"}, headers=account_headers
    )
    assert response.status_code == 201, response.text
    runtime.bind_id("agent", response.json()["id"])
    _record(runtime, agent_action, account, response, "agent_created")

    version_action = SequenceAction(name="create_version", target="version")
    response = await runtime.client.post(
        f"/api/v1/agents/{runtime.resolve_id('agent')}/versions",
        json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        headers=account_headers,
    )
    assert response.status_code == 201, response.text
    runtime.bind_id("version", response.json()["id"])
    _record(runtime, version_action, account, response, "version_created")

    for target in ("job", "foreign_job"):
        action = SequenceAction(name="create_session_run", target=target)
        response = await runtime.client.post(
            "/api/v1/session-runs",
            json={
                "agent_version_id": runtime.resolve_id("version"),
                "inputs": {"target": target},
            },
            headers=account_headers,
        )
        assert response.status_code == 201, response.text
        runtime.bind_id(target, response.json()["id"])
        _record(runtime, action, account, response, "job_created")

    worker_action = SequenceAction(name="create_worker", target="worker")
    response = await runtime.client.post(
        "/api/v1/workers",
        json={
            "name": "task-lifecycle-worker",
            "scope": _SCOPE,
            "runtime": _RUNTIME,
            "metadata": {},
        },
        headers=account_headers,
    )
    assert response.status_code == 200, response.text
    runtime.bind_id("worker", response.json()["worker"]["id"])
    runtime.set_credential(CredentialRole.WORKER, response.json()["token"])
    _record(runtime, worker_action, account, response, "worker_created")

    claim_action = SequenceAction(name="claim_tasks", target="task")
    response = await runtime.client.post(
        "/api/v1/tasks/claim",
        json={"max_tasks": 2},
        headers=runtime.get_headers(CredentialRole.WORKER),
    )
    assert response.status_code == 200, response.text
    entries = response.json()["tasks"]
    assert len(entries) == 2, entries
    by_job = {entry["task"]["job_id"]: entry for entry in entries}
    target_entry = by_job[runtime.resolve_id("job")]
    foreign_entry = by_job[runtime.resolve_id("foreign_job")]
    runtime.bind_id("task", target_entry["task"]["id"])
    runtime.bind_id("foreign_task", foreign_entry["task"]["id"])
    runtime.set_credential(CredentialRole.TASK_ATTEMPT_1, target_entry["token"])
    runtime.set_credential(CredentialRole.FOREIGN_TASK, foreign_entry["token"])
    _record(
        runtime,
        claim_action,
        CredentialRole.WORKER,
        response,
        "two_tasks_claimed",
        "attempt_one",
    )
    return TaskModel()


async def _sweep_at_synthetic_time(runtime: SequenceRuntime) -> None:
    """Sweep the target through the production service at a future timestamp."""
    assert runtime.settings is not None
    database = DatabaseService(runtime.settings)
    now = datetime.now(UTC) + timedelta(
        seconds=runtime.settings.TASK_HEARTBEAT_TIMEOUT_SECONDS + 1
    )
    try:
        async with aclosing(database.get_async_session()) as sessions:
            session = await anext(sessions)
            try:
                analytics = get_server_analytics(
                    session, AnalyticsClient(enabled=False)
                )
                service = get_task_service(
                    session, database.engine, runtime.settings, analytics
                )
                await service.sweep_stale_task(
                    uuid.UUID(runtime.resolve_id("task")), now
                )
                await session.commit()
            except BaseException:
                await session.rollback()
                raise
    finally:
        await database.cleanup()


async def _get_task_and_job(
    runtime: SequenceRuntime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the target task and job through separate committed requests."""
    headers = runtime.get_headers(CredentialRole.ACCOUNT)
    task_response = await runtime.client.get(
        f"/api/v1/tasks/{runtime.resolve_id('task')}", headers=headers
    )
    job_response = await runtime.client.get(
        f"/api/v1/jobs/{runtime.resolve_id('job')}", headers=headers
    )
    assert task_response.status_code == 200, task_response.text
    assert job_response.status_code == 200, job_response.text
    return task_response.json(), job_response.json()


async def _create_completed_result_session(
    runtime: SequenceRuntime,
    model: TaskModel,
    role: CredentialRole,
    receipt_action: SequenceAction | None = None,
) -> None:
    """Create and complete the target attempt's linked result session."""
    session_symbol = f"result_session_{model.attempt}"
    response = await runtime.client.post(
        "/api/v1/sessions",
        json={"origin": "recorded", "inputs": None, "outputs": None},
        headers=runtime.get_headers(role),
    )
    assert response.status_code == 201, response.text
    runtime.bind_id(session_symbol, response.json()["id"])
    _record(
        runtime,
        receipt_action
        or SequenceAction(name="link_result_session", target=session_symbol),
        role,
        response,
        "task_session_linked",
    )
    response = await runtime.client.patch(
        f"/api/v1/sessions/{runtime.resolve_id(session_symbol)}",
        json={"status": "completed", "outputs": {"ok": True}},
        headers=runtime.get_headers(CredentialRole.ACCOUNT),
    )
    _record(
        runtime,
        SequenceAction(name="complete_result_session", target=session_symbol),
        CredentialRole.ACCOUNT,
        response,
        "result_session_completed",
    )
    assert response.status_code == 200, response.text
    model.has_result_session = True


async def _assert_model(runtime: SequenceRuntime, model: TaskModel) -> None:
    """Compare independently modeled state with persisted API state."""
    task, job = await _get_task_and_job(runtime)
    assert task["status"] == model.status
    assert task["attempt"] == model.attempt
    assert (task["cancel_requested_at"] is not None) is model.cancel_requested
    assert job["status"] == model.job_status
    if model.job_status in {"completed", "failed", "canceled"}:
        assert job["ended_at"] is not None
    if model.has_result_session:
        response = await runtime.client.get(
            f"/api/v1/sessions/{runtime.resolve_id(f'result_session_{model.attempt}')}",
            headers=runtime.get_headers(CredentialRole.ACCOUNT),
        )
        assert response.status_code == 200, response.text
        session = response.json()
        assert session["task_id"] == runtime.resolve_id("task")
        assert session["status"] == "completed"
        assert session["outputs"] == {"ok": True}


async def _execute_action(
    runtime: SequenceRuntime, action: SequenceAction, model: TaskModel
) -> None:
    """Execute one generated action and compare it with the model."""
    task_path = f"/api/v1/tasks/{runtime.resolve_id('task')}"
    role = _get_role_for_attempt(model.attempt)

    if action.name == "start":
        response = await runtime.client.patch(
            task_path,
            json={"status": "running"},
            headers=runtime.get_headers(role),
        )
        _record(runtime, action, role, response, "claimed_to_running")
        assert response.status_code == 200, response.text
        model.status = "running"
    elif action.name == "link_result_session":
        await _create_completed_result_session(runtime, model, role, action)
    elif action.name == "complete":
        if not model.has_result_session:
            await _create_completed_result_session(runtime, model, role)
        response = await runtime.client.patch(
            task_path,
            json={"status": "completed"},
            headers=runtime.get_headers(role),
        )
        _record(runtime, action, role, response, "task_and_job_completed")
        assert response.status_code == 200, response.text
        model.status = "completed"
        model.job_status = "completed"
        model.has_result_session = True
    elif action.name == "fail":
        response = await runtime.client.patch(
            task_path,
            json={"status": "failed", "error": "generated failure"},
            headers=runtime.get_headers(role),
        )
        _record(runtime, action, role, response, "task_and_job_failed")
        assert response.status_code == 200, response.text
        model.status = "failed"
        model.job_status = "failed"
    elif action.name == "cancel_job":
        response = await runtime.client.post(
            f"/api/v1/jobs/{runtime.resolve_id('job')}/cancel",
            headers=runtime.get_headers(CredentialRole.ACCOUNT),
        )
        _record(
            runtime,
            action,
            CredentialRole.ACCOUNT,
            response,
            "in_flight_cancel_requested",
        )
        assert response.status_code == 200, response.text
        model.cancel_requested = True
    elif action.name == "acknowledge_cancel":
        response = await runtime.client.patch(
            task_path,
            json={"status": "canceled"},
            headers=runtime.get_headers(role),
        )
        _record(runtime, action, role, response, "cancel_acknowledged")
        assert response.status_code == 200, response.text
        model.status = "canceled"
        model.job_status = "canceled"
    elif action.name in {"stale_reclaim", "stale_exhaust"}:
        stale_role = role
        linked_session_symbol = (
            f"result_session_{model.attempt}" if model.has_result_session else None
        )
        await _sweep_at_synthetic_time(runtime)
        if action.arguments.get("force_failure_after_sweep") is True:
            raise AssertionError(
                f"forced lifecycle invariant failure after {action.name} sweep"
            )
        task, job = await _get_task_and_job(runtime)
        if action.name == "stale_exhaust":
            assert task["status"] == "abandoned"
            expected_error = (
                f"Task stopped reporting after {model.attempt} attempts "
                "and was abandoned"
            )
            assert task["error"] == expected_error
            assert job["status"] == "failed"
            assert job["error"] == expected_error
            response = await runtime.client.patch(
                task_path,
                json={"status": "running"},
                headers=runtime.get_headers(stale_role),
            )
            _record(runtime, action, stale_role, response, "retry_limit_exhausted")
            assert response.status_code == 409, response.text
            model.status = "abandoned"
            model.job_status = "failed"
        else:
            assert task["status"] == "pending"
            assert task["worker_id"] is None
            assert task["claimed_at"] is None
            assert task["heartbeat_at"] is None
            assert task["started_at"] is None
            if linked_session_symbol is not None:
                session_response = await runtime.client.get(
                    f"/api/v1/sessions/{runtime.resolve_id(linked_session_symbol)}",
                    headers=runtime.get_headers(CredentialRole.ACCOUNT),
                )
                assert session_response.status_code == 200, session_response.text
                assert session_response.json()["task_id"] is None
            response = await runtime.client.post(
                "/api/v1/tasks/claim",
                json={"max_tasks": 1},
                headers=runtime.get_headers(CredentialRole.WORKER),
            )
            assert response.status_code == 200, response.text
            entries = response.json()["tasks"]
            assert len(entries) == 1, entries
            assert entries[0]["task"]["id"] == runtime.resolve_id("task")
            model.attempt += 1
            next_role = _get_role_for_attempt(model.attempt)
            runtime.set_credential(next_role, entries[0]["token"])
            _record(runtime, action, CredentialRole.WORKER, response, "task_reclaimed")
            response = await runtime.client.patch(
                task_path,
                json={"status": "running"},
                headers=runtime.get_headers(next_role),
            )
            _record(
                runtime,
                SequenceAction(name="start_reclaimed_attempt", target="task"),
                next_role,
                response,
                "fresh_attempt_started",
            )
            assert response.status_code == 200, response.text
            model.status = "running"
            model.has_result_session = False
            await _create_completed_result_session(runtime, model, next_role)
            response = await runtime.client.patch(
                task_path,
                json={"status": "completed"},
                headers=runtime.get_headers(stale_role),
            )
            _record(
                runtime,
                SequenceAction(name="reject_stale_attempt", target="task"),
                stale_role,
                response,
                "attempt_fence",
            )
            assert response.status_code == 409, response.text
            task, _job = await _get_task_and_job(runtime)
            assert task["status"] == "running"
            assert task["attempt"] == model.attempt
            assert task["result"] is None
            session_response = await runtime.client.get(
                f"/api/v1/sessions/{runtime.resolve_id(f'result_session_{model.attempt}')}",
                headers=runtime.get_headers(CredentialRole.ACCOUNT),
            )
            assert session_response.status_code == 200, session_response.text
            assert session_response.json()["task_id"] == runtime.resolve_id("task")
    elif action.name == "foreign_start":
        response = await runtime.client.patch(
            task_path,
            json={"status": "running"},
            headers=runtime.get_headers(CredentialRole.FOREIGN_TASK),
        )
        _record(
            runtime,
            action,
            CredentialRole.FOREIGN_TASK,
            response,
            "foreign_task_forbidden",
        )
        assert response.status_code == 403, response.text
    elif action.name == "repeat_terminal":
        response = await runtime.client.patch(
            task_path,
            json={"status": model.status},
            headers=runtime.get_headers(role),
        )
        _record(runtime, action, role, response, "terminal_transition_rejected")
        assert response.status_code == 409, response.text
    elif action.name == "completion_rejected":
        response = await runtime.client.patch(
            task_path,
            json={"status": "completed"},
            headers=runtime.get_headers(role),
        )
        _record(runtime, action, role, response, "completion_rejected")
        assert response.status_code == 409, response.text
    else:
        raise AssertionError(f"Unknown action: {action.name}")

    await _assert_model(runtime, model)


async def _run(
    actions: list[SequenceAction],
) -> SequenceReceipt:
    """Run one action list in isolation and return its clean receipt."""
    receipt = SequenceReceipt()
    with annotate_sequence_failure(receipt):
        async with asyncio.timeout(_SEQUENCE_TIMEOUT_SECONDS):
            async with isolate_sequence(
                receipt,
                TASK_HEARTBEAT_TIMEOUT_SECONDS=30,
                TASK_RETRY_LIMIT=3,
            ) as runtime:
                model = await _bootstrap(runtime)
                for action in actions:
                    runtime.record_requested_action(action)
                    await _execute_action(runtime, action, model)
                receipt.assert_accepted()
                serialized = receipt.serialize()
                assert not _UUID_PATTERN.search(serialized)
                assert "Bearer " not in serialized
                assert "sequence-secret" not in serialized
    return receipt


@pytest.mark.parametrize(
    "actions",
    [
        ["start", "complete"],
        ["stale_reclaim", "complete"],
        ["start", "link_result_session", "stale_reclaim", "complete"],
        ["cancel_job", "acknowledge_cancel", "completion_rejected"],
        ["start", "cancel_job", "acknowledge_cancel", "completion_rejected"],
        ["start", "cancel_job", "complete"],
        ["start", "complete", "repeat_terminal"],
        ["foreign_start", "start"],
        ["stale_reclaim", "stale_reclaim", "stale_exhaust"],
    ],
)
def test_fixed_task_lifecycle_receipts(actions: list[str]) -> None:
    """Pin the principal success, fencing, cancellation, and retry contracts."""
    receipt = asyncio.run(_run([_build_action(name) for name in actions]))
    assert receipt.successful_operations > 0


@SEQUENCE_SETTINGS
@given(actions=_task_attempt_sequences())
def test_generated_task_attempt_sequences(actions: list[SequenceAction]) -> None:
    """Generated prerequisite-valid task attempts agree with the model."""
    receipt = asyncio.run(_run(actions))
    assert receipt.successful_operations > 0


def _get_replay_actions(receipt: SequenceReceipt) -> list[SequenceAction]:
    """Recover only caller-requested actions from a sanitized receipt."""
    return receipt.requested_actions


def _get_replay_contract(receipt: SequenceReceipt) -> list[tuple[object, ...]]:
    """Select stable evidence for caller-requested sequence actions."""
    return [
        (
            step.action,
            step.credential_role,
            step.status,
            step.invariants,
        )
        for step in receipt.steps
        if step.action.arguments.get(_REPLAY_ACTION) is True
    ]


def test_task_lifecycle_receipt_replays_cleanly() -> None:
    """A serialized symbolic receipt maps back to the same replay actions."""
    actions = [
        _build_action("stale_reclaim"),
        _build_action("complete"),
        _build_action("repeat_terminal"),
    ]
    first = asyncio.run(_run(actions))
    parsed = SequenceReceipt.model_validate_json(first.serialize())
    replay_actions = _get_replay_actions(parsed)
    assert replay_actions == actions
    replayed = asyncio.run(_run(replay_actions))
    assert _get_replay_contract(replayed) == _get_replay_contract(first)


def test_failing_receipt_replays_the_same_invariant() -> None:
    """Reproduce one semantic failure from its receipt in a fresh database."""
    expected = "forced lifecycle invariant failure after stale_reclaim sweep"
    actions = [
        _build_action("start"),
        _build_action("stale_reclaim", force_failure_after_sweep=True),
    ]
    observed_messages: list[str] = []

    for _ in range(2):
        with pytest.raises(AssertionError, match=expected) as raised:
            asyncio.run(_run(actions))
        observed_messages.append(str(raised.value))
        note = next(
            item
            for item in raised.value.__notes__
            if item.startswith("Sequence receipt:\n")
        )
        receipt = SequenceReceipt.model_validate_json(
            note.removeprefix("Sequence receipt:\n")
        )
        receipt.assert_accepted()
        actions = _get_replay_actions(receipt)

    assert observed_messages == [expected, expected]
