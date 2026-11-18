"""First insight milestones use the owning person's cloud identity."""

import uuid
from typing import Any

import pytest

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.insight import TextInsightData
from kitaru.api_models.v1.job import JobKind
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.insight import InsightCreate, InsightInput
from kitaru.server.application.services.insight_recording import record_task_insights
from kitaru.server.application.services.insight_service import InsightService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.task import AnalysisTask
from kitaru_test_support import (
    FakeAccountRepository,
    FakeAgentRepository,
    FakeInsightRepository,
    FakeJobRepository,
    create_agent,
    create_job,
)


class RecordingAnalytics(ServerAnalytics):
    def __init__(self) -> None:
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        self.tracked.append((user_id, event, properties or {}))


@pytest.mark.parametrize("hosted", [False, True])
async def test_manual_batch_emits_one_milestone_then_stops(hosted: bool) -> None:
    owner = Account(
        name="person",
        external_id=uuid.uuid4() if hosted else None,
        email="person@example.com",
    )
    agents = FakeAgentRepository()
    insights = FakeInsightRepository()
    agent = await create_agent(agents, owner.id)
    tracker = RecordingAnalytics()
    service = InsightService(insights, agents, tracker)
    command = InsightCreate(
        agent_id=agent.id,
        insights=[
            InsightInput(name="one", title="One", data=TextInsightData(content="one")),
            InsightInput(name="two", title="Two", data=TextInsightData(content="two")),
        ],
    )
    # Another user's history does not suppress this user's milestone.
    await insights.create_many(
        [
            Insight(
                owner_id=uuid.uuid4(),
                agent_id=agent.id,
                name="other",
                title="Other",
                data=TextInsightData(content="other"),
            )
        ]
    )
    await service.create_insights(command, actor=AuthContext(account=owner))
    await service.create_insights(command, actor=AuthContext(account=owner))
    milestones = [
        item
        for item in tracker.tracked
        if item[1] == AnalyticsEvent.FIRST_INSIGHT_GENERATED
    ]
    assert len(milestones) == 1
    assert milestones[0][0] == (owner.external_id or owner.id)
    assert milestones[0][2]["email"] == owner.email
    # Normal per-insight analytics retain the local account identity.
    normal = [
        item for item in tracker.tracked if item[1] == AnalyticsEvent.INSIGHT_CREATED
    ]
    assert len(normal) == 4
    assert all(item[0] == owner.id for item in normal)


@pytest.mark.parametrize("service_account", [False, True])
async def test_empty_batch_and_service_account_have_no_milestone(
    service_account: bool,
) -> None:
    owner = Account(name="person", is_service_account=service_account)
    agents = FakeAgentRepository()
    insights = FakeInsightRepository()
    agent = await create_agent(agents, owner.id)
    tracker = RecordingAnalytics()
    service = InsightService(insights, agents, tracker)
    await service.create_insights(
        InsightCreate(agent_id=agent.id, insights=[]), actor=AuthContext(account=owner)
    )
    if service_account:
        await service.create_insights(
            InsightCreate(
                agent_id=agent.id,
                insights=[
                    InsightInput(
                        name="one", title="One", data=TextInsightData(content="a")
                    )
                ],
            ),
            actor=AuthContext(account=owner),
        )
    assert not any(
        event == AnalyticsEvent.FIRST_INSIGHT_GENERATED
        for _, event, _ in tracker.tracked
    )


@pytest.mark.parametrize("hosted", [False, True])
async def test_analysis_results_are_attributed_to_job_owner(hosted: bool) -> None:
    owner = Account(
        name="person",
        external_id=uuid.uuid4() if hosted else None,
        email="person@example.com",
    )
    accounts = FakeAccountRepository()
    owner = await accounts.create(owner)
    jobs = FakeJobRepository()
    job = await create_job(jobs, owner.id, kind=JobKind.SESSION_RUN)
    insights = FakeInsightRepository()
    tracker = RecordingAnalytics()
    task = AnalysisTask(
        job_id=job.id,
        agent_id=uuid.uuid4(),
        import_id=uuid.uuid4(),
        plugin_version_id=uuid.uuid4(),
        status=TaskStatus.COMPLETED,
        result=[
            {
                "name": "result",
                "title": "Result",
                "data": {"type": "text", "content": "ok"},
            }
        ],
    )
    await record_task_insights(
        TaskTerminal(task=task, previous_status=TaskStatus.RUNNING),
        insights,
        jobs,
        accounts,
        tracker,
    )
    assert len(tracker.tracked) == 1
    assert tracker.tracked[0][0] == (owner.external_id or owner.id)
    assert tracker.tracked[0][1] == AnalyticsEvent.FIRST_INSIGHT_GENERATED
    assert tracker.tracked[0][2]["email"] == owner.email
    # Manual creation after a generated insight must not emit another milestone.
    agents = FakeAgentRepository()
    agent = await create_agent(agents, owner.id)
    service = InsightService(insights, agents, tracker)
    await service.create_insights(
        InsightCreate(
            agent_id=agent.id,
            insights=[
                InsightInput(
                    name="manual", title="Manual", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=AuthContext(account=owner),
    )
    assert (
        sum(
            event == AnalyticsEvent.FIRST_INSIGHT_GENERATED
            for _, event, _ in tracker.tracked
        )
        == 1
    )


@pytest.mark.parametrize("commit", [False, True])
async def test_delivery_waits_for_commit_and_preserves_owner(commit: bool) -> None:
    from unittest.mock import MagicMock

    from sqlalchemy.ext.asyncio import AsyncSession

    from kitaru.analytics.client import AnalyticsClient
    from kitaru.server.adapters.db.analytics import register_analytics_listeners
    from kitaru.server.application.services.insight_analytics import (
        track_first_insight_generated,
    )
    from kitaru.server.application.services.server_analytics import current_actor

    register_analytics_listeners()
    owner = Account(name="owner", external_id=uuid.uuid4(), email="owner@example.com")
    worker = Account(name="worker", external_id=uuid.uuid4(), is_service_account=True)
    repository = FakeInsightRepository()
    insights = await repository.create_many(
        [
            Insight(
                owner_id=owner.id,
                agent_id=uuid.uuid4(),
                name="first",
                title="First",
                data=TextInsightData(content="a"),
            )
        ]
    )
    client = MagicMock(spec=AnalyticsClient)
    client.enabled = True
    token = current_actor.set(worker)
    try:
        # A logical session transaction tests the commit listeners without a DB.
        async with AsyncSession() as session:
            await session.begin()
            analytics = ServerAnalytics(client=client, session=session)
            await track_first_insight_generated(repository, insights, owner, analytics)
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
    client.track.assert_called_once()
    user_id, event, properties = client.track.call_args.args
    assert user_id == owner.external_id
    assert event == AnalyticsEvent.FIRST_INSIGHT_GENERATED
    assert properties["control_plane_user_id"] == owner.external_id
    assert properties["service_account"] is False
