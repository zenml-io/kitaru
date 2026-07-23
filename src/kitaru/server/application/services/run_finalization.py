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
"""Experiment run finalization helpers."""

import uuid

from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.replay_repository import (
    ReplayRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.domain.experiment_run import TERMINAL_RUN_STATUSES
from kitaru.server.domain.replay import TERMINAL_REPLAY_STATUSES, Replay
from kitaru.server.domain.replay_diff import compute_run_summary
from kitaru.server.domain.session import Session

# Page size for resolving every replay of an experiment run.
_REPLAY_RESOLUTION_PAGE_SIZE = 1000


async def load_run_replays(
    replay_repository: ReplayRepository, run_id: uuid.UUID
) -> list[Replay]:
    """Load every replay of an experiment run across all pages.

    Args:
        replay_repository: Replay repository.
        run_id: Id of the experiment run.

    Returns:
        Replays of the run.
    """
    replays: list[Replay] = []
    page = 1
    while True:
        batch, total = await replay_repository.query(
            ReplayFilter(
                experiment_run_id=run_id,
                page=page,
                page_size=_REPLAY_RESOLUTION_PAGE_SIZE,
            )
        )
        replays.extend(batch)
        if len(replays) >= total or not batch:
            return replays
        page += 1


async def finalize_run_if_drained(
    run_repository: ExperimentRunRepository,
    replay_repository: ReplayRepository,
    session_repository: SessionRepository,
    run_id: uuid.UUID,
) -> None:
    """Finalize an experiment run once its last replay went terminal.

    A canceling run lands on canceled, any other run on completed, with the
    aggregate summary computed from the replays and their sessions. Runs
    with non-terminal replays stay untouched.

    Args:
        run_repository: Experiment run repository.
        replay_repository: Replay repository.
        session_repository: Session repository.
        run_id: Id of the experiment run.
    """
    run = await run_repository.get(run_id)
    if run.status in TERMINAL_RUN_STATUSES:
        return
    replays = await load_run_replays(replay_repository, run_id)
    if any(replay.status not in TERMINAL_REPLAY_STATUSES for replay in replays):
        return
    sessions: dict[uuid.UUID, Session] = {}
    for replay in replays:
        for session_id in (replay.original_session_id, replay.result_session_id):
            if session_id is not None and session_id not in sessions:
                sessions[session_id] = await session_repository.get(session_id)
    run.finalize(compute_run_summary(replays, sessions))
    await run_repository.update(run)
