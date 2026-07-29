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
"""Tests for PostgreSQL IAM database authentication."""

import ssl
import uuid
from typing import Any

import asyncpg
import pytest
from pydantic import ValidationError

from kitaru.server.api.config import APISettings, AuthScheme
from kitaru.server.config import DatabaseAuthMethod, DatabaseSSLMode
from kitaru.server.database.service import DatabaseService


class FakeTokenProvider:
    """Record database token requests."""

    def __init__(self) -> None:
        """Initialize the fake provider."""
        self.calls: list[dict[str, Any]] = []

    def generate_token(
        self,
        *,
        host: str,
        port: int,
        username: str,
        region: str,
    ) -> str:
        """Return a unique token for each request."""
        self.calls.append(
            {
                "host": host,
                "port": port,
                "username": username,
                "region": region,
            }
        )
        return f"token-{len(self.calls)}"


def iam_settings(**overrides: Any) -> APISettings:
    """Create valid managed database settings."""
    values: dict[str, Any] = {
        "DB_HOST": "kitaru.proxy.example.com",
        "DB_PORT": 5432,
        "DB_USER": "kitaru_workspace",
        "DB_NAME": "kitaru_workspace",
        "DB_AUTH_METHOD": DatabaseAuthMethod.AWS_IAM,
        "DB_AWS_REGION": "eu-central-1",
        "DB_SSL_MODE": DatabaseSSLMode.VERIFY_FULL,
        "CREATE_DB_IF_MISSING": False,
        "SECRET_ENCRYPTION_KEY": "test-encryption-key",
        **overrides,
    }
    return APISettings(**values)


async def test_iam_token_is_generated_for_each_physical_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generate a fresh token whenever asyncpg opens a connection."""
    provider = FakeTokenProvider()
    settings = iam_settings()
    connections: list[dict[str, Any]] = []

    async def connect(**kwargs: Any) -> Any:
        connections.append(kwargs)
        return object()

    monkeypatch.setattr(asyncpg, "connect", connect)
    service = DatabaseService(settings, token_provider=provider)
    connection_factory = service._iam_connection_factory(settings)

    await connection_factory()
    await connection_factory()
    await service.cleanup()

    expected_request = {
        "host": "kitaru.proxy.example.com",
        "port": 5432,
        "username": "kitaru_workspace",
        "region": "eu-central-1",
    }
    assert provider.calls == [expected_request, expected_request]
    assert [connection["password"] for connection in connections] == [
        "token-1",
        "token-2",
    ]
    assert all(
        isinstance(connection["ssl"], ssl.SSLContext)
        and connection["ssl"].check_hostname
        and connection["ssl"].verify_mode == ssl.CERT_REQUIRED
        for connection in connections
    )


def test_cloud_mode_requires_iam_database_authentication() -> None:
    """Reject password-backed databases for managed Cloud servers."""
    with pytest.raises(
        ValidationError,
        match="Cloud mode requires KITARU_SERVER_DB_AUTH_METHOD=aws_iam",
    ):
        APISettings(
            DB_HOST="localhost",
            AUTH_SCHEME=AuthScheme.CLOUD,
            SERVER_ID=uuid.uuid4(),
            CONTROL_PLANE_API_URL="https://cloud.example.com",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
        )


def test_cloud_mode_requires_external_database_provisioning() -> None:
    """Reject application-managed database creation in Cloud mode."""
    with pytest.raises(
        ValidationError,
        match="KITARU_SERVER_CREATE_DB_IF_MISSING=false",
    ):
        iam_settings(
            AUTH_SCHEME=AuthScheme.CLOUD,
            SERVER_ID=uuid.uuid4(),
            CONTROL_PLANE_API_URL="https://cloud.example.com",
            CREATE_DB_IF_MISSING=True,
        )


def test_cloud_mode_requires_https_control_plane() -> None:
    """Reject control-plane URLs that could expose bearer credentials."""
    with pytest.raises(
        ValidationError,
        match="CONTROL_PLANE_API_URL must be an HTTPS URL",
    ):
        iam_settings(
            AUTH_SCHEME=AuthScheme.CLOUD,
            SERVER_ID=uuid.uuid4(),
            CONTROL_PLANE_API_URL="http://cloud.example.com",
        )


def test_cloud_mode_restricts_control_plane_host() -> None:
    """Reject a Cloud host outside the configured allowlist."""
    with pytest.raises(
        ValidationError,
        match="CONTROL_PLANE_API_URL host is not allowed",
    ):
        iam_settings(
            AUTH_SCHEME=AuthScheme.CLOUD,
            SERVER_ID=uuid.uuid4(),
            CONTROL_PLANE_API_URL="https://attacker.example.com",
            CONTROL_PLANE_ALLOWED_HOSTS=["cloud.example.com"],
        )
