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
"""Analysis Completed event properties."""

import uuid

from kitaru.server.application.services.analytics_events import (
    build_analysis_completed_properties,
)
from kitaru.server.domain.task import AnalysisTask


def _task(**scope: object) -> AnalysisTask:
    return AnalysisTask.model_validate(
        {
            "job_id": uuid.uuid4(),
            "plugin_version_id": uuid.uuid4(),
            "agent_id": uuid.uuid4(),
            **scope,
        }
    )


def test_import_analysis_reports_an_import_selection() -> None:
    """Mark a task that reads a whole import as an import selection."""
    properties = build_analysis_completed_properties(
        _task(import_id=uuid.uuid4()), None, None
    )
    assert properties["selection"] == "import"


def test_session_list_analysis_reports_a_sessions_selection() -> None:
    """Mark a task over a fixed session list apart from an import analysis."""
    task = _task(import_id=uuid.uuid4(), session_ids=[uuid.uuid4() for _ in range(3)])
    properties = build_analysis_completed_properties(task, None, None)
    assert properties["selection"] == "sessions"
    assert properties["session_count"] == 3
