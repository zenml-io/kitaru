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
"""Event subscriber composition."""

from functools import partial

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.server.adapters.db.repositories.evaluation_repository import (
    SQLEvaluationRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.replay_repository import (
    SQLReplayRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.application.events import (
    EventDispatcher,
    JobsSettled,
    ReplaysSettled,
    SessionImportFinalized,
    TaskTerminal,
)
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.services import (
    evaluation_recording,
    replay_pipeline,
    run_finalization,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.application.services.task_transitions import TaskTransitions


def register_subscribers(
    dispatcher: EventDispatcher,
    job_repository: JobRepository,
    task_repository: TaskRepository,
    replay_repository: ReplayRepository,
    session_repository: SessionRepository,
    experiment_repository: ExperimentRepository,
    experiment_run_repository: ExperimentRunRepository,
    evaluation_repository: EvaluationRepository,
    analytics: ServerAnalytics | None = None,
) -> None:
    """Register every task-transition subscriber on a dispatcher.

    Each subscriber owns one aggregate and is bound to the repositories it
    needs, so a handler commits or rolls back with the transition that
    emitted its event.

    Args:
        dispatcher: Event dispatcher to register on.
        job_repository: Job repository.
        task_repository: Task repository.
        replay_repository: Replay repository.
        session_repository: Session repository.
        experiment_repository: Experiment repository, for replay configs.
        experiment_run_repository: Experiment run repository.
        evaluation_repository: Evaluation repository.
        analytics: Analytics tracker, None skips tracking.
    """
    dispatcher.register(
        TaskTerminal,
        partial(
            evaluation_recording.record_task_evaluations,
            evaluation_repository=evaluation_repository,
            job_repository=job_repository,
        ),
    )
    dispatcher.register(
        TaskTerminal,
        partial(
            replay_pipeline.append_result_evaluations,
            replay_repository=replay_repository,
            experiment_repository=experiment_repository,
            task_repository=task_repository,
            session_repository=session_repository,
        ),
    )
    dispatcher.register(
        SessionImportFinalized,
        partial(
            replay_pipeline.complete_import_wait,
            task_repository=task_repository,
            transitions=TaskTransitions(
                task_repository=task_repository,
                job_repository=job_repository,
                dispatcher=dispatcher,
                analytics=analytics,
            ),
        ),
    )
    dispatcher.register(
        JobsSettled,
        partial(
            replay_pipeline.settle_replays,
            replay_repository=replay_repository,
            dispatcher=dispatcher,
        ),
    )
    dispatcher.register(
        ReplaysSettled,
        partial(
            run_finalization.finalize_runs_if_drained,
            replay_repository=replay_repository,
            experiment_run_repository=experiment_run_repository,
            analytics=analytics,
        ),
    )


def build_event_dispatcher(
    session: AsyncSession,
    engine: AsyncEngine,
    analytics: ServerAnalytics | None = None,
) -> EventDispatcher:
    """Build the event dispatcher every subscriber of one request shares.

    Subscribers are constructed here with repositories bound to the request's
    database session, so a handler commits or rolls back with the transition
    that emitted its event.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        analytics: Analytics tracker, None skips tracking.

    Returns:
        Dispatcher carrying the registered subscribers.
    """
    dispatcher = EventDispatcher()
    register_subscribers(
        dispatcher,
        job_repository=SQLJobRepository(session),
        task_repository=SQLTaskRepository(session),
        replay_repository=SQLReplayRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        experiment_repository=SQLExperimentRepository(session),
        experiment_run_repository=SQLExperimentRunRepository(session),
        evaluation_repository=SQLEvaluationRepository(session),
        analytics=analytics,
    )
    return dispatcher
