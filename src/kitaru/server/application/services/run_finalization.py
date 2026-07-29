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

from datetime import UTC, datetime

from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.server.application.events import ReplaySettled
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.replay_repository import ReplayRepository


async def finalize_run_if_drained(
    event: ReplaySettled,
    replay_repository: ReplayRepository,
    experiment_run_repository: ExperimentRunRepository,
) -> None:
    """Finalize a run once every one of its replays has settled.

    Cancellation wins over everything, then any failed or canceled replay
    fails the run, otherwise the run completes. A no-op when the settled
    replay is standalone or the run still has live replays.

    Args:
        event: ReplaySettled event.
        replay_repository: Replay repository, for the drained count.
        experiment_run_repository: Experiment run repository.
    """
    replay = event.replay
    if replay.experiment_run_id is None:
        return
    counts = await replay_repository.count_by_status(replay.experiment_run_id)
    if counts.non_settled > 0:
        return
    run = await experiment_run_repository.get(replay.experiment_run_id, exclusive=True)
    if run.settled:
        return
    if run.status is ExperimentRunStatus.CANCELING:
        outcome = ExperimentRunStatus.CANCELED
    elif counts.failed > 0 or counts.canceled > 0:
        outcome = ExperimentRunStatus.FAILED
    else:
        outcome = ExperimentRunStatus.COMPLETED
    run.finalize(outcome, None, datetime.now(UTC))
    await experiment_run_repository.update(run)
