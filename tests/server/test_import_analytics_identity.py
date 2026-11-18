"""Regular import telemetry exposes its owner's email."""

import uuid
from datetime import UTC, datetime
from functools import partial
from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.analytics.client import AnalyticsClient
from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.adapters.db.analytics import register_analytics_listeners
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.services.server_analytics import (
    ServerAnalytics,
    current_actor,
)
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.task import Task
from kitaru_test_support import (
    FakeAccountRepository,
    FakeImportRepository,
    FakeJobRepository,
    FakeTaskRepository,
    create_import,
    create_import_task,
    create_job,
)


@pytest.mark.parametrize("hosted", [False, True])
@pytest.mark.parametrize("email", ["owner@example.com", None])
@pytest.mark.parametrize("count", [0, 3])
@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_import_events_use_owner_email_on_every_import(
    hosted: bool, email: str | None, count: int, failed: bool, commit: bool
) -> None:
    register_analytics_listeners()
    accounts = FakeAccountRepository()
    owner = await accounts.create(
        Account(name="owner", external_id=uuid.uuid4() if hosted else None, email=email)
    )
    worker = Account(
        name="worker",
        external_id=uuid.uuid4(),
        is_service_account=True,
        email="worker@example.com",
    )
    tasks = FakeTaskRepository()
    jobs = FakeJobRepository(tasks=tasks)
    imports = FakeImportRepository()
    client = MagicMock(spec=AnalyticsClient)
    client.enabled = True
    token = current_actor.set(worker)
    try:
        async with AsyncSession() as session:
            await session.begin()
            analytics = ServerAnalytics(client=client, session=session)
            transitions = TaskTransitions(
                tasks,
                jobs,
                EventDispatcher(),
                analytics,
                import_repository=imports,
                account_repository=accounts,
            )
            for _ in range(2):
                job = await create_job(jobs, owner.id)
                imported = await create_import(
                    imports, owner.id, uuid.uuid4(), job_id=job.id
                )
                task = await create_import_task(tasks, job.id, import_id=imported.id)
                now = datetime.now(UTC)
                finish = (
                    partial(
                        Task.fail,
                        error="import failed",
                        result={"created": count},
                        now=now,
                    )
                    if failed
                    else partial(Task.complete, result={"created": count}, now=now)
                )
                for transition in (
                    partial(Task.claim, worker_id=uuid.uuid4(), now=now),
                    partial(Task.start, now=now),
                    finish,
                ):
                    task = await transitions.apply_status(task, transition)
            client.track.assert_not_called()
            if commit:
                await session.commit()
            else:
                await session.rollback()
    finally:
        current_actor.reset(token)
    if not commit:
        client.track.assert_not_called()
        return
    events = [
        call.args
        for call in client.track.call_args_list
        if call.args[1] == AnalyticsEvent.IMPORT_COMPLETED
    ]
    assert len(events) == 2
    for user_id, event, properties in events:
        assert user_id == owner.id
        assert event == "Import Completed"
        assert properties["email"] == owner.email
        assert properties["control_plane_user_id"] == owner.external_id
        assert properties["service_account"] is False
        assert properties["status"] == ("failed" if failed else "completed")
        assert properties["session_count"] == count
        assert properties["email"] != worker.email
