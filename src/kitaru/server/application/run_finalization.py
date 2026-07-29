"""Finalize experiment runs after their replay set drains."""

from kitaru.server.application.events import ReplaySettled
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.domain.experiment_run import ExperimentRunStatus
from kitaru.server.domain.replay import ReplayStatus


async def finalize_run_if_drained(
    event: ReplaySettled,
    replay_repository: ReplayRepository,
    run_repository: ExperimentRunRepository,
) -> None:
    """Finalize the parent run when it has no unsettled replay rows."""
    run_id = event.replay.experiment_run_id
    if run_id is None or await replay_repository.count_unsettled(run_id):
        return
    run = await run_repository.get(run_id, exclusive=True)
    if run.status.terminal:
        return
    statuses = await replay_repository.count_statuses(run_id)
    if run.status is ExperimentRunStatus.CANCELING:
        run.finalize(ExperimentRunStatus.CANCELED)
    elif statuses.get(ReplayStatus.FAILED, 0) or statuses.get(ReplayStatus.CANCELED, 0):
        run.finalize(
            ExperimentRunStatus.FAILED,
            error="One or more replays failed or were canceled",
        )
    else:
        run.finalize(ExperimentRunStatus.COMPLETED)
    await run_repository.update(run)
