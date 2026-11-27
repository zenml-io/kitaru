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
"""Restart attribution of a server built without its workspace names."""

import uuid

import pytest

from conftest import control_plane_settings, drop_test_database, postgres_available
from kitaru.server.adapters.db.repositories.server_settings_repository import (
    SQLServerSettingsRepository,
)
from kitaru.server.api.app import create_app
from kitaru.server.application.models.workspace_identity import WorkspaceIdentity
from kitaru.server.database.service import DatabaseService


async def test_restarted_server_names_the_stored_workspace() -> None:
    """Fall back to the stored workspace when the settings leave it blank."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = control_plane_settings(use_db=True)
    identity = WorkspaceIdentity(
        organization_id=uuid.uuid4(), organization_name="acme", workspace_name="ml"
    )
    await DatabaseService.create_db(settings)
    try:
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            async for session in app.state.database.get_async_session():
                await SQLServerSettingsRepository(session).save_workspace_identity(
                    identity
                )
                await session.commit()

        restarted = create_app(settings)
        async with restarted.router.lifespan_context(restarted):
            context = restarted.state.analytics.context
    finally:
        await drop_test_database(settings)

    assert context["organization_id"] == identity.organization_id
    assert context["organization_name"] == "acme"
    assert context["workspace_name"] == "ml"
