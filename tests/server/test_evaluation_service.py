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
"""Tests for evaluation use cases."""

import uuid

import pytest

from conftest import FakeEvaluationRepository, FakeSessionRepository, create_session
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionStatus
from kitaru.server.application.models.auth import (
    AuthContext,
    GrantKind,
    TaskPrincipal,
)
from kitaru.server.application.models.evaluation import (
    EvaluationCreate,
    EvaluationFilter,
)
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.domain.account import Account
from kitaru.server.domain.evaluation import (
    DuplicateEvaluationNameInBatch,
    EvaluationNameConflict,
)
from kitaru.server.domain.session import (
    SessionAccessDenied,
    SessionNotEvaluatable,
    SessionNotFound,
)
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def evaluation_repository() -> FakeEvaluationRepository:
    """Provide a fake evaluation repository."""
    return FakeEvaluationRepository()


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository()


@pytest.fixture
def service(
    evaluation_repository: FakeEvaluationRepository,
    session_repository: FakeSessionRepository,
) -> EvaluationService:
    """Provide an evaluation service backed by the fake repositories."""
    return EvaluationService(
        repository=evaluation_repository, session_repository=session_repository
    )


async def test_get_evaluation(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Get a stored evaluation by id."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    stored = await service.create_evaluations(
        session.id,
        [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
        actor=ACTOR,
    )
    item = await service.get_evaluation(stored[0].id, actor=ACTOR)
    assert item.evaluation == stored[0]


async def test_list_evaluations(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """List evaluations matching a filter."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    await service.create_evaluations(
        session.id,
        [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
        actor=ACTOR,
    )
    items, next_cursor = await service.list_evaluations(
        EvaluationFilter(
            expression=FilterCondition(
                field="session_id", op=FilterOp.EQ, value=session.id
            )
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert len(items) == 1


async def test_create_evaluations_requires_existing_session(
    service: EvaluationService,
) -> None:
    """Reject a create into an unknown session."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await service.create_evaluations(
            missing_id,
            [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
            actor=ACTOR,
        )


async def test_create_evaluations_rejects_a_repeated_name(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Resending a name already stored for the session fails."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    await service.create_evaluations(
        session.id,
        [
            EvaluationCreate(
                name="accuracy", data_type=EvaluationDataType.FLOAT, score=0.5
            )
        ],
        actor=ACTOR,
    )
    with pytest.raises(EvaluationNameConflict):
        await service.create_evaluations(
            session.id,
            [
                EvaluationCreate(
                    name="accuracy", data_type=EvaluationDataType.STR, value="high"
                )
            ],
            actor=ACTOR,
        )


async def test_create_evaluations_carries_passed(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Carry the pass flag from the create command onto the stored evaluation."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    stored = await service.create_evaluations(
        session.id,
        [
            EvaluationCreate(
                name="a", data_type=EvaluationDataType.FLOAT, score=1.0, passed=False
            ),
            EvaluationCreate(name="b", data_type=EvaluationDataType.FLOAT, score=1.0),
        ],
        actor=ACTOR,
    )
    assert [evaluation.passed for evaluation in stored] == [False, None]


async def test_create_evaluations_owner_is_actor(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Stamp the created evaluations with the caller's account id."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    stored = await service.create_evaluations(
        session.id,
        [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
        actor=ACTOR,
    )
    assert stored[0].owner_id == ACTOR.account.id


async def test_create_evaluations_rejects_duplicate_name_in_batch(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Reject a request naming the same evaluation twice."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    with pytest.raises(
        DuplicateEvaluationNameInBatch,
        match="Evaluation name 'accuracy' appears more than once in the request",
    ):
        await service.create_evaluations(
            session.id,
            [
                EvaluationCreate(
                    name="accuracy", data_type=EvaluationDataType.FLOAT, score=1.0
                ),
                EvaluationCreate(
                    name="accuracy", data_type=EvaluationDataType.FLOAT, score=2.0
                ),
            ],
            actor=ACTOR,
        )


async def test_create_evaluations_manual_rows_carry_no_evaluator_or_task(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Leave evaluator_version_id and task_id null on manually created rows."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    stored = await service.create_evaluations(
        session.id,
        [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
        actor=ACTOR,
    )
    assert stored[0].evaluator_version_id is None
    assert stored[0].task_id is None


async def test_create_evaluations_accepts_any_session_status(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Allow a create regardless of the session's status."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status="completed",
    )
    stored = await service.create_evaluations(
        session.id,
        [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
        actor=ACTOR,
    )
    assert stored[0].name == "a"


async def test_create_evaluations_rejects_an_in_progress_session(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Reject merging evaluations into an in-progress session."""
    session = await create_session(
        session_repository, ACTOR.account.id, agent_id=uuid.uuid4()
    )
    with pytest.raises(SessionNotEvaluatable):
        await service.create_evaluations(
            session.id,
            [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
            actor=ACTOR,
        )


def _task_principal(
    task_id: uuid.UUID, granted_session_id: uuid.UUID | None = None
) -> AuthContext:
    """Build an auth context for a task principal owning the given task."""
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if granted_session_id is not None:
        grants[GrantKind.SESSION] = frozenset({granted_session_id})
    return AuthContext(
        account=Account(id=uuid.uuid4(), name="job-owner"),
        principal=TaskPrincipal(
            task_id=task_id,
            attempt=1,
            worker_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            grants=grants,
        ),
    )


async def test_create_evaluations_allows_a_task_principal_for_its_own_session(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Allow a task principal to create evaluations on the session it owns."""
    task_id = uuid.uuid4()
    session = await create_session(
        session_repository,
        uuid.uuid4(),
        agent_id=uuid.uuid4(),
        task_id=task_id,
        status=SessionStatus.COMPLETED,
    )
    actor = _task_principal(task_id)
    stored = await service.create_evaluations(
        session.id,
        [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
        actor=actor,
    )
    assert stored[0].name == "a"


async def test_create_evaluations_allows_a_task_principal_for_its_input_session(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Allow an evaluator task to create results on the session it scored."""
    session = await create_session(
        session_repository,
        uuid.uuid4(),
        agent_id=uuid.uuid4(),
        task_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    actor = _task_principal(uuid.uuid4(), granted_session_id=session.id)
    stored = await service.create_evaluations(
        session.id,
        [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
        actor=actor,
    )
    assert stored[0].name == "a"


async def test_create_evaluations_denies_a_task_principal_for_an_unrelated_session(
    service: EvaluationService, session_repository: FakeSessionRepository
) -> None:
    """Reject a task principal merging evaluations into an unrelated session."""
    session = await create_session(
        session_repository, uuid.uuid4(), agent_id=uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4())
    with pytest.raises(SessionAccessDenied):
        await service.create_evaluations(
            session.id,
            [EvaluationCreate(name="a", data_type=EvaluationDataType.FLOAT, score=1.0)],
            actor=actor,
        )
