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
"""Tests for task hook API models."""

import uuid

from kitaru.api_models.v1.agent_version import RunSpec
from kitaru.api_models.v1.hook import CommandHook, CopyWorkdirHook
from kitaru.api_models.v1.task import (
    AgentTaskDetails,
    TaskKind,
    TaskRunSpec,
    TaskSpecResponse,
)


def test_run_spec_hooks_round_trip_json() -> None:
    """Round-trip a run spec's hooks through JSON, preserving each variant."""
    spec = RunSpec(
        command="run.sh",
        hooks=[
            CopyWorkdirHook(),
            CommandHook(command="setup.sh", when="setup"),
            CommandHook(command="teardown.sh", when="teardown", run_on_failure=True),
        ],
    )
    restored = RunSpec.model_validate_json(spec.model_dump_json())
    assert restored == spec
    assert isinstance(restored.hooks[0], CopyWorkdirHook)
    assert isinstance(restored.hooks[1], CommandHook)
    assert restored.hooks[1].command == "setup.sh"
    assert restored.hooks[1].when == "setup"
    assert restored.hooks[1].run_on_failure is False
    assert isinstance(restored.hooks[2], CommandHook)
    assert restored.hooks[2].command == "teardown.sh"
    assert restored.hooks[2].when == "teardown"
    assert restored.hooks[2].run_on_failure is True


def test_task_spec_response_hooks_round_trip_json() -> None:
    """Round-trip a task spec response's hooks through JSON."""
    spec = TaskSpecResponse(
        task_id=uuid.uuid4(),
        kind=TaskKind.AGENT,
        timeout_seconds=60,
        run=TaskRunSpec(command="run.sh", env={}),
        env={},
        secret_env={},
        hooks=[
            CopyWorkdirHook(),
            CommandHook(command="teardown.sh", when="teardown"),
        ],
        details=AgentTaskDetails(inputs=None),
    )
    restored = TaskSpecResponse.model_validate_json(spec.model_dump_json())
    assert restored == spec
    assert isinstance(restored.hooks[0], CopyWorkdirHook)
    assert isinstance(restored.hooks[1], CommandHook)
    assert restored.hooks[1].run_on_failure is False
