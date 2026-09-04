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


def test_declared_error_statuses() -> None:
    """Document the common error statuses on every router except auth and info."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    schema = app.openapi()
    error_ref = {"$ref": "#/components/schemas/ErrorBody"}
    excluded_paths = {
        "/api/v1/info",
        "/api/v1/device_authorization",
        "/api/v1/login",
        "/api/v1/logout",
    }
    for path, path_item in schema["paths"].items():
        if not path.startswith("/api/v1/") or path in excluded_paths:
            continue
        for operation in path_item.values():
            responses = operation["responses"]
            for status_code in ("401", "403", "503"):
                assert status_code in responses
                response = responses[status_code]
                assert response["content"]["application/json"]["schema"] == error_ref

    def assert_declared(path: str, method: str, status_code: str) -> None:
        response = schema["paths"][path][method]["responses"][status_code]
        assert response["content"]["application/json"]["schema"] == error_ref

    assert_declared("/api/v1/agents/{agent_id}", "get", "404")
    assert_declared("/api/v1/secrets/{secret_id}", "delete", "409")
    assert_declared("/api/v1/blobs", "post", "413")
    assert_declared("/api/v1/workers", "post", "426")
    assert_declared("/api/v1/login", "post", "401")
    assert_declared("/api/v1/login", "post", "503")
    assert_declared("/api/v1/agents", "post", "400")
    assert_declared("/health", "get", "503")


def test_response_schemas_declare_no_write_only_fields() -> None:
    """Keep write-only secret markers out of every schema a response can carry."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    schema = app.openapi()
    schemas = schema["components"]["schemas"]
    response_schema_names: set[str] = set()

    def collect_refs(value: Any) -> None:
        if isinstance(value, dict):
            ref = value.get("$ref")
            if isinstance(ref, str):
                name = ref.rsplit("/", 1)[1]
                if name not in response_schema_names:
                    response_schema_names.add(name)
                    collect_refs(schemas[name])
            for item in value.values():
                collect_refs(item)
        elif isinstance(value, list):
            for item in value:
                collect_refs(item)

    for path_item in schema["paths"].values():
        for operation in path_item.values():
            for response in operation["responses"].values():
                collect_refs(response.get("content", {}).get("application/json"))

    assert "WorkerTokenResponse" in response_schema_names
    assert "SecretWithValuesResponse" in response_schema_names
    assert "TaskWithSpec" in response_schema_names
    for name in response_schema_names:
        for field_name, field_schema in schemas[name].get("properties", {}).items():
            assert "writeOnly" not in field_schema, f"{name}.{field_name}"
    assert schemas["WorkerTokenResponse"]["properties"]["token"] == {
        "description": "Bearer token scoped to this worker.",
        "title": "Token",
        "type": "string",
    }
    assert schemas["SecretWithValuesResponse"]["properties"]["values"][
        "additionalProperties"
    ] == {"type": "string"}
    assert schemas["SecretCreateRequest"]["properties"]["values"][
        "additionalProperties"
    ] == {"format": "password", "type": "string", "writeOnly": True}
