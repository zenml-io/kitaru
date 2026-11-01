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
"""Import ingestion failures must not reserve a source identity."""

import pytest
from task_fixtures import build_task_app

from conftest import imported_node, imported_session
from kitaru.api_models.v1.session import SessionListParams
from kitaru.task.importer import SessionImportError, ingest_session


async def test_invalid_node_tree_does_not_store_session() -> None:
    """Reject invalid nodes before persisting a session that would block retry."""
    async for app in build_task_app():
        parsed = imported_session(
            "retryable-session",
            [imported_node("duplicate"), imported_node("duplicate")],
        )
        with pytest.raises(SessionImportError, match="not unique"):
            await ingest_session(app.client, parsed, app.agent.id, "langfuse")
        sessions = await app.client.sessions.list(SessionListParams())
        assert sessions.items == []
