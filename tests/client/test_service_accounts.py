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
"""Round-trip tests for the service accounts SDK resource."""

import json
import uuid

from fastapi import FastAPI

from conftest import (
    FakeAccountRepository,
    FakePasswordHasher,
    local_settings,
    override_idempotency,
    recording_asgi_api_client,
)
from kitaru.api_models.v1.account import (
    AccountResponse,
    ServiceAccountCreateRequest,
    ServiceAccountUpdateRequest,
)
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.adapters.rest.dependencies import authorize, get_account_service
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.domain.account import Account

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="admin", is_admin=True))


def _build_app() -> FastAPI:
    """Build an app with a fake-backed account service and an admin actor.

    Returns:
        Application with the account service and authorization overridden.
    """
    app = create_app(local_settings())
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )
    app.dependency_overrides[get_account_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: ACTOR
    override_idempotency(app, ACTOR.account)
    return app


async def test_create() -> None:
    """Create a service account through the SDK and check the wire payload."""
    client, recorder = recording_asgi_api_client(_build_app())
    async with client:
        account = await client.service_accounts.create(
            ServiceAccountCreateRequest(name="bot", email="bot@example.com")
        )

    sent_body = json.loads(recorder.requests[0].content)
    assert sent_body["name"] == "bot"
    assert sent_body["email"] == "bot@example.com"
    assert isinstance(account, AccountResponse)
    assert account.name == "bot"
    assert account.email == "bot@example.com"
    assert account.is_service_account is True
    assert account.active is True


async def test_update() -> None:
    """Update a service account through the SDK and check the wire payload."""
    client, recorder = recording_asgi_api_client(_build_app())
    async with client:
        created = await client.service_accounts.create(
            ServiceAccountCreateRequest(name="bot")
        )
        updated = await client.service_accounts.update(
            created.id,
            ServiceAccountUpdateRequest(metadata={"team": "ml"}, active=False),
        )

    sent_body = json.loads(recorder.requests[1].content)
    assert sent_body["metadata"] == {"team": "ml"}
    assert sent_body["active"] is False
    assert isinstance(updated, AccountResponse)
    assert updated.metadata == {"team": "ml"}
    assert updated.active is False
