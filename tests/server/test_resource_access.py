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
"""Tests for the grants a task spec derives and the access they scope."""

import uuid

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.imports import ImportQuery
from kitaru.api_models.v1.task import TaskKind
from kitaru.server.application.models.auth import (
    AuthContext,
    GrantKind,
    TaskPrincipal,
)
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.services.resource_access import (
    build_task_grants,
    scope_task_session_filter,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.task import (
    AgentTaskDetails,
    AnalysisTaskDetails,
    ApiImportSourceSpec,
    BlobImportSourceSpec,
    EvaluationTaskDetails,
    ImportTaskDetails,
    PackagePluginSpec,
    ScriptPluginSpec,
    TaskRunSpec,
    TaskSpec,
)
from kitaru.server.filtering import AndExpression, FilterCondition, OrExpression


def _script_plugin(blob_id: uuid.UUID) -> ScriptPluginSpec:
    return ScriptPluginSpec(entrypoint="score", blob_id=blob_id, sha256="abc")


def test_agent_spec_grants_nothing() -> None:
    """Grant an agent task nothing, it owns the session it creates."""
    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.AGENT,
        timeout_seconds=60,
        run_spec=TaskRunSpec(command="run.sh"),
        details=AgentTaskDetails(),
    )
    assert build_task_grants(spec) == {}


def test_mastra_replay_agent_spec_grants_its_recorded_file_blobs() -> None:
    """Grant a replaying agent task the blobs holding its recorded Mastra files."""
    blob_id = uuid.uuid4()

    def file(**fields: object) -> dict[str, object]:
        return {
            "url": "kitaru-file://sha256/" + "a" * 64,
            "mediaType": "image/png",
            "length": 4,
            "sha256": "b" * 64,
            **fields,
        }

    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.AGENT,
        timeout_seconds=60,
        run_spec=TaskRunSpec(command="run.sh"),
        details=AgentTaskDetails(
            inputs={
                "mastra_memory_replay": {
                    "files": [
                        file(blobId=str(blob_id)),
                        file(base64="AAAA"),
                        file(blobId="not-a-uuid"),
                        "malformed",
                    ]
                }
            },
            replay_id=uuid.uuid4(),
        ),
    )
    assert build_task_grants(spec) == {GrantKind.BLOB: frozenset({blob_id})}


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
            source=BlobImportSourceSpec(blob_id=payload_blob_id, sha256="abc"),
            agent_id=uuid.uuid4(),
        ),
    )
    assert build_task_grants(spec) == {
        GrantKind.BLOB: frozenset({payload_blob_id, plugin_blob_id})
    }


def test_import_spec_with_api_source_grants_only_the_script_blob() -> None:
    """Grant an API-sourced importer task only the blob holding its script."""
    plugin_blob_id = uuid.uuid4()
    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.IMPORTER,
        timeout_seconds=60,
        details=ImportTaskDetails(
            plugin=_script_plugin(plugin_blob_id),
            source=ApiImportSourceSpec(query=ImportQuery(trace_ids=[])),
            agent_id=uuid.uuid4(),
        ),
    )
    assert build_task_grants(spec) == {GrantKind.BLOB: frozenset({plugin_blob_id})}


def test_analysis_spec_grants_its_import_and_script_blob() -> None:
    """Grant an analyzer task its import and the blob holding its script."""
    import_id = uuid.uuid4()
    blob_id = uuid.uuid4()
    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.ANALYZER,
        timeout_seconds=60,
        details=AnalysisTaskDetails(
            analyzer_name="trends",
            plugin=_script_plugin(blob_id),
            agent_id=uuid.uuid4(),
            import_id=import_id,
        ),
    )
    assert build_task_grants(spec) == {
        GrantKind.BLOB: frozenset({blob_id}),
        GrantKind.IMPORT: frozenset({import_id}),
    }


ACCOUNT = Account(id=uuid.uuid4(), name="ann")


def _task_actor(grants: dict[GrantKind, frozenset[uuid.UUID]]) -> AuthContext:
    return AuthContext(
        account=ACCOUNT,
        principal=TaskPrincipal(
            task_id=uuid.uuid4(),
            attempt=1,
            worker_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            grants=grants,
        ),
    )


def test_scope_task_session_filter_passes_an_account_filter_through() -> None:
    """Leave an account principal's session filter untouched."""
    session_filter = SessionFilter(
        expression=FilterCondition(field="name", op=FilterOp.EQ, value="a")
    )
    scoped = scope_task_session_filter(session_filter, AuthContext(account=ACCOUNT))
    assert scoped == session_filter


def test_scope_task_session_filter_restricts_a_task_to_its_own_and_imports() -> None:
    """AND the task's own and granted-import sessions onto a task's filter."""
    import_id = uuid.uuid4()
    actor = _task_actor({GrantKind.IMPORT: frozenset({import_id})})
    assert isinstance(actor.principal, TaskPrincipal)
    scope = OrExpression(
        operands=(
            FilterCondition(
                field="task_id", op=FilterOp.EQ, value=actor.principal.task_id
            ),
            FilterCondition(field="import_id", op=FilterOp.IN, value=[import_id]),
        )
    )
    status = FilterCondition(field="status", op=FilterOp.NE, value="in_progress")

    scoped = scope_task_session_filter(SessionFilter(expression=status), actor)
    assert scoped.expression == AndExpression(operands=(scope, status))

    scoped = scope_task_session_filter(SessionFilter(), actor)
    assert scoped.expression == scope


def test_scope_task_session_filter_limits_a_task_without_imports_to_its_own() -> None:
    """Restrict a task principal holding no import grant to its own sessions."""
    actor = _task_actor({GrantKind.SESSION: frozenset({uuid.uuid4()})})
    assert isinstance(actor.principal, TaskPrincipal)
    scoped = scope_task_session_filter(SessionFilter(), actor)
    assert scoped.expression == FilterCondition(
        field="task_id", op=FilterOp.EQ, value=actor.principal.task_id
    )
