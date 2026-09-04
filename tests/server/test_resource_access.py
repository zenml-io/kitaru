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
"""Tests for the grants a task spec derives."""

import uuid

from kitaru.api_models.v1.task import TaskKind
from kitaru.server.application.models.auth import GrantKind
from kitaru.server.application.services.resource_access import build_task_grants
from kitaru.server.domain.task import (
    AgentTaskDetails,
    AnalysisTaskDetails,
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
        details=AgentTaskDetails(),
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


def test_analysis_spec_grants_every_input_session_and_script_blob() -> None:
    """Grant an analyzer task every listed session and the blob holding its script."""
    input_session_ids = [uuid.uuid4(), uuid.uuid4()]
    blob_id = uuid.uuid4()
    spec = TaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.ANALYZER,
        timeout_seconds=60,
        details=AnalysisTaskDetails(
            analyzer_name="trends",
            plugin=_script_plugin(blob_id),
            agent_id=uuid.uuid4(),
            input_session_ids=input_session_ids,
        ),
    )
    assert build_task_grants(spec) == {
        GrantKind.SESSION: frozenset(input_session_ids),
        GrantKind.BLOB: frozenset({blob_id}),
    }
