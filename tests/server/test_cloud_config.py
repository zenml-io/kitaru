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
"""Tests for managed Cloud server configuration."""

import uuid
from typing import Any

import pytest
from pydantic import ValidationError

from kitaru.server.api.config import APISettings, AuthScheme


def cloud_settings(**overrides: Any) -> APISettings:
    """Create valid managed Cloud settings."""
    values: dict[str, Any] = {
        "DB_HOST": "postgres.example.com",
        "DB_USER": "postgres",
        "DB_PWD": "password",
        "AUTH_SCHEME": AuthScheme.CLOUD,
        "SERVER_ID": uuid.uuid4(),
        "CONTROL_PLANE_API_URL": "https://cloud.example.com",
        "SECRET_ENCRYPTION_KEY": "test-encryption-key",
        **overrides,
    }
    return APISettings(**values)


def test_cloud_mode_accepts_password_database_authentication() -> None:
    """Use the standard PostgreSQL username and password settings in Cloud."""
    settings = cloud_settings()

    assert settings.DB_USER == "postgres"
    assert settings.DB_PWD == "password"


def test_cloud_mode_requires_https_control_plane() -> None:
    """Reject control-plane URLs that could expose bearer credentials."""
    with pytest.raises(
        ValidationError,
        match="CONTROL_PLANE_API_URL must be an HTTPS URL",
    ):
        cloud_settings(CONTROL_PLANE_API_URL="http://cloud.example.com")


def test_cloud_mode_restricts_control_plane_host() -> None:
    """Reject a Cloud host outside the configured allowlist."""
    with pytest.raises(
        ValidationError,
        match="CONTROL_PLANE_API_URL host is not allowed",
    ):
        cloud_settings(
            CONTROL_PLANE_API_URL="https://attacker.example.com",
            CONTROL_PLANE_ALLOWED_HOSTS=["cloud.example.com"],
        )
