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
"""Smoke test for the application's generated OpenAPI schema."""

import copy
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi.openapi.utils import validation_error_definition

from conftest import FakeAccountRepository, FakePasswordHasher, local_settings
from kitaru.api_models.v1.base import ValidationErrorBody, ValidationErrorItem
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.adapters.rest.dependencies import authorize, get_account_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.domain.account import Account

_ACTOR = AuthContext(
    account=Account(
        id=uuid.uuid4(),
        name="admin",
        is_admin=True,
        created=datetime.now(UTC),
        updated=datetime.now(UTC),
    )
)


def test_422_responses_document_the_string_error_body() -> None:
    """Document the validation error body on every operation that declares 422."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    schema = app.openapi()
    assert "ValidationErrorBody" in schema["components"]["schemas"]
    assert "ValidationErrorItem" in schema["components"]["schemas"]
    checked = 0
    for path_item in schema["paths"].values():
        for operation in path_item.values():
            response = operation.get("responses", {}).get("422")
            if response is None:
                continue
            checked += 1
            assert response["content"]["application/json"]["schema"] == {
                "$ref": "#/components/schemas/ValidationErrorBody"
            }
    assert checked > 0


async def test_422_matches_fastapi_runtime_body() -> None:
    """Match the documented validation error body against the runtime response."""
    app = create_app(local_settings())
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )
    app.dependency_overrides[get_account_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: _ACTOR
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/accounts", params={"size": "notanint"})
    assert response.status_code == 422
    body = ValidationErrorBody.model_validate(response.json())
    assert isinstance(body.detail, list)
    for item in body.detail:
        assert item.loc
        assert item.msg
        assert item.type


_DROPPED_KEYS = ("title", "description", "default")


def _normalize_schema(value: Any) -> Any:
    """Recursively drop title, description, and default keys from a schema dict.

    Also drops an additionalProperties entry whose value is True, since that
    is semantically identical to the key being absent.
    """
    if isinstance(value, dict):
        return {
            key: _normalize_schema(inner)
            for key, inner in value.items()
            if key not in _DROPPED_KEYS
            and not (key == "additionalProperties" and inner is True)
        }
    if isinstance(value, list):
        return [_normalize_schema(item) for item in value]
    return value


def test_validation_error_item_matches_fastapi_schema() -> None:
    """Match ValidationErrorItem's schema shape against FastAPI's own definition."""
    ours = _normalize_schema(copy.deepcopy(ValidationErrorItem.model_json_schema()))
    fastapi_schema = _normalize_schema(copy.deepcopy(validation_error_definition))
    assert ours == fastapi_schema


def test_fastapi_auto_422_is_suppressed() -> None:
    """Keep FastAPI's auto-generated HTTPValidationError out of the schema."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    schema = app.openapi()
    assert "HTTPValidationError" not in schema["components"]["schemas"]
