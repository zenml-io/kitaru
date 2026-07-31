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
"""Tests for experiment run finalization against fake repositories."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from conftest import FakeExperimentRunRepository, FakeReplayRepository, create_replay
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.server.application.events import ReplaySettled
from kitaru.server.application.services.run_finalization import (
    finalize_run_if_drained,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.experiment_run import ExperimentRun

OWNER_ID = uuid.uuid4()


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


async def _drained_run(
    run_repository: FakeExperimentRunRepository,
    replay_repository: FakeReplayRepository,
    status: ReplayStatus,
) -> tuple[ReplaySettled, uuid.UUID]:
    """Store a run with its single, now-settled replay and return the settled event."""
    started_at = datetime.now(UTC) - timedelta(seconds=5)
    run = await run_repository.create(
        ExperimentRun(
            owner_id=OWNER_ID,
            experiment_id=uuid.uuid4(),
            number=1,
            cohort_version_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            status=ExperimentRunStatus.RUNNING,
            started_at=started_at,
        )
    )
    replay = await create_replay(
        replay_repository,
        OWNER_ID,
        job_id=uuid.uuid4(),
        replay_config_id=uuid.uuid4(),
        baseline_session_id=uuid.uuid4(),
        experiment_run_id=run.id,
        status=status,
    )
    return ReplaySettled(replay=replay), run.id


async def test_finalize_run_if_drained_tracks_experiment_run_completed() -> None:
    """Fire EXPERIMENT_RUN_COMPLETED with the outcome, replay count, and duration."""
    run_repository = FakeExperimentRunRepository()
    replay_repository = FakeReplayRepository()
    event, _ = await _drained_run(
        run_repository, replay_repository, ReplayStatus.COMPLETED
    )
    analytics = _RecordingAnalytics()

    await finalize_run_if_drained(
        event,
        replay_repository=replay_repository,
        experiment_run_repository=run_repository,
        analytics=analytics,
    )

    assert len(analytics.tracked) == 1
    user_id, tracked_event, properties = analytics.tracked[0]
    assert user_id == OWNER_ID
    assert tracked_event == AnalyticsEvent.EXPERIMENT_RUN_COMPLETED
    assert properties["status"] == ExperimentRunStatus.COMPLETED.value
    assert properties["replay_count"] == 1
    assert properties["duration_seconds"] >= 0


async def test_finalize_run_if_drained_without_analytics_tracker() -> None:
    """Finalize a run normally when no analytics tracker is configured."""
    run_repository = FakeExperimentRunRepository()
    replay_repository = FakeReplayRepository()
    event, run_id = await _drained_run(
        run_repository, replay_repository, ReplayStatus.COMPLETED
    )

    await finalize_run_if_drained(
        event,
        replay_repository=replay_repository,
        experiment_run_repository=run_repository,
    )

    run = await run_repository.get(run_id)
    assert run.status is ExperimentRunStatus.COMPLETED
