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
"""Experiment run finalization, driven off the run's replay rows."""

import uuid
from datetime import UTC, datetime

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.server.application.events import ReplaysSettled
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.services import analytics_events
from kitaru.server.application.services.server_analytics import ServerAnalytics


async def finalize_runs_if_drained(
    event: ReplaysSettled,
    replay_repository: ReplayRepository,
    experiment_run_repository: ExperimentRunRepository,
    analytics: ServerAnalytics | None = None,
) -> None:
    """Finalize the settled replays' runs once all of their replays settled.

    Standalone replays are skipped and each run is finalized once however
    many of its replays the event carries.

    Args:
        event: ReplaysSettled event.
        replay_repository: Replay repository, for the drained counts.
        experiment_run_repository: Experiment run repository.
        analytics: Analytics tracker, None skips tracking.
    """
    run_ids = {
        replay.experiment_run_id
        for replay in event.replays
        if replay.experiment_run_id is not None
    }
    for run_id in sorted(run_ids):
        await _finalize_run(
            run_id,
            replay_repository,
            experiment_run_repository,
            analytics,
        )


async def _finalize_run(
    experiment_run_id: uuid.UUID,
    replay_repository: ReplayRepository,
    experiment_run_repository: ExperimentRunRepository,
    analytics: ServerAnalytics | None = None,
) -> None:
    """Finalize a run once every one of its replays has settled.

    Cancellation wins over everything, then any failed or canceled replay
    fails the run, otherwise the run completes. A no-op when the run still
    has live replays.

    The run row is locked before the drained count is read, so two callers
    settling replays of the same run concurrently serialize on the lock
    instead of both reading the other's still-uncommitted settlement and
    skipping finalization, which would stall the run forever.

    Args:
        experiment_run_id: Id of the run.
        replay_repository: Replay repository, for the drained count.
        experiment_run_repository: Experiment run repository.
        analytics: Analytics tracker, None skips tracking.
    """
    run = await experiment_run_repository.get(experiment_run_id, exclusive=True)
    if run.settled:
        return
    counts = await replay_repository.count_by_status(experiment_run_id)
    if counts.non_settled > 0:
        return
    if run.status is ExperimentRunStatus.CANCELING:
        outcome = ExperimentRunStatus.CANCELED
    elif counts.failed > 0 or counts.canceled > 0:
        outcome = ExperimentRunStatus.FAILED
    else:
        outcome = ExperimentRunStatus.COMPLETED
    run.finalize(outcome, None, datetime.now(UTC))
    run = await experiment_run_repository.update(run)
    if analytics is not None:
        analytics.track(
            run.owner_id,
            AnalyticsEvent.EXPERIMENT_RUN_COMPLETED,
            analytics_events.build_experiment_run_completed_properties(
                run, counts.total
            ),
        )
