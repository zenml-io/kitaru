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
"""Tests for task-principal resource access checks and grants."""

import uuid

import pytest

from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import TaskKind
from kitaru.server.application.models.auth import (
    AuthContext,
    GrantKind,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.services.resource_access import (
    build_task_grants,
    check_task_session_read,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.session import Session, SessionAccessDenied
from kitaru.server.domain.task import (
    CommandAgentTaskDetails,
    EvaluationTaskDetails,
    ImportTaskDetails,
    PackagePluginSpec,
    PayloadSpec,
    ScriptPluginSpec,
    TaskRunSpec,
    TaskSpec,
)


def _script_plugin(blob_id: uuid.UUID) -> ScriptPluginSpec:
    return ScriptPluginSpec(entrypoint="score", blob_id=blob_id, sha256="abc")


def test_agent_spec_grants_nothing() -> None:
    """Grant an agent task nothing, it owns the session it creates."""
    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.AGENT,
        timeout_seconds=60,
        run_spec=TaskRunSpec(command="run.sh"),
        details=CommandAgentTaskDetails(),
    )
    assert build_task_grants(spec) == {}


def test_evaluation_spec_grants_its_input_session_and_script_blob() -> None:
    """Grant an evaluator task its input session and the blob holding its script."""
    input_session_id = uuid.uuid4()
    blob_id = uuid.uuid4()
    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.EVALUATOR,
        timeout_seconds=60,
        details=EvaluationTaskDetails(
            evaluator_name="accuracy",
            plugin=_script_plugin(blob_id),
            input_session_id=input_session_id,
        ),
    )
    assert build_task_grants(spec) == {
        GrantKind.SESSION: frozenset({input_session_id}),
        GrantKind.BLOB: frozenset({blob_id}),
    }


def test_package_plugin_spec_grants_no_blob() -> None:
    """Grant no blob for a plugin installed from a package requirement."""
    input_session_id = uuid.uuid4()
    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.EVALUATOR,
        timeout_seconds=60,
        details=EvaluationTaskDetails(
            evaluator_name="accuracy",
            plugin=PackagePluginSpec(entrypoint="mod:score", requirement="scorer==1.0"),
            input_session_id=input_session_id,
        ),
    )
    assert build_task_grants(spec) == {GrantKind.SESSION: frozenset({input_session_id})}


def test_import_spec_grants_its_payload_and_script_blob() -> None:
    """Grant an importer task the blobs holding its payload and its script."""
    payload_blob_id = uuid.uuid4()
    plugin_blob_id = uuid.uuid4()
    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.IMPORTER,
        timeout_seconds=60,
        details=ImportTaskDetails(
            plugin=_script_plugin(plugin_blob_id),
            payload=PayloadSpec(blob_id=payload_blob_id, sha256="abc"),
            agent_id=uuid.uuid4(),
        ),
    )
    assert build_task_grants(spec) == {
        GrantKind.BLOB: frozenset({payload_blob_id, plugin_blob_id})
    }


def _pending_import_session(task_id: uuid.UUID | None = None) -> Session:
    """Build a pending-import placeholder session."""
    return Session(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        number=1,
        origin=SessionOrigin.REPLAY,
        status=SessionStatus.PENDING_IMPORT,
        task_id=task_id if task_id is not None else uuid.uuid4(),
    )


def _task_actor(task_id: uuid.UUID) -> AuthContext:
    """Build an auth context for a task principal owning the given task."""
    return AuthContext(
        account=Account(id=uuid.uuid4(), name="job-owner"),
        principal=TaskPrincipal(
            task_id=task_id,
            attempt=1,
            worker_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
        ),
    )


def test_session_read_denies_a_task_for_a_foreign_pending_import_session() -> None:
    """Reject a task principal reading a pending-import session it does not own."""
    session = _pending_import_session()
    with pytest.raises(SessionAccessDenied):
        check_task_session_read(session.id, session.task_id, _task_actor(uuid.uuid4()))


def test_session_read_allows_the_producing_task_for_its_pending_import_session() -> (
    None
):
    """Allow the producing task principal to read its own pending-import session."""
    task_id = uuid.uuid4()
    session = _pending_import_session(task_id=task_id)
    check_task_session_read(session.id, session.task_id, _task_actor(task_id))


def test_session_read_skips_the_check_for_an_account_principal() -> None:
    """Allow an account principal to read any pending-import session."""
    session = _pending_import_session()
    actor = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
    check_task_session_read(session.id, session.task_id, actor)


def test_session_read_skips_the_check_for_a_worker_principal() -> None:
    """Allow a worker principal to read any pending-import session."""
    session = _pending_import_session()
    actor = WorkerAuthContext(
        account=Account(id=uuid.uuid4(), name="ann"),
        principal=WorkerPrincipal(worker_id=uuid.uuid4()),
    )
    check_task_session_read(session.id, session.task_id, actor)
