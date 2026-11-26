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
"""Conflict response contracts for session external IDs."""

import httpx
import pytest
from fastapi import FastAPI

from conftest import local_settings
from kitaru.server.api.app import create_app
from kitaru.server.domain.base import ConflictError
from kitaru.server.domain.session import DuplicateSessionExternalId


@pytest.mark.parametrize("duplicate", [True, False])
async def test_conflict_error_code(duplicate: bool) -> None:
    """Identify duplicate external IDs without changing other conflict bodies."""
    app: FastAPI = create_app(local_settings())
    conflict = (
        DuplicateSessionExternalId("source", "external")
        if duplicate
        else ConflictError("other conflict")
    )

    @app.get("/probe-conflict")
    async def raise_conflict() -> None:
        raise conflict

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/probe-conflict")
    assert response.status_code == 409
    expected = {"detail": str(conflict)}
    if duplicate:
        expected["code"] = "duplicate_session_external_id"
    assert response.json() == expected
