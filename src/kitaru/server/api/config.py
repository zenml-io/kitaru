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
"""API process configuration."""

import uuid
from typing import Self
from urllib.parse import urlparse

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import SettingsConfigDict

from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.config import Settings

# Sentinel meaning the server has not been enrolled with a control plane.
UNSET_SERVER_ID = uuid.UUID(int=0)


def _get_otel_alias(name: str) -> AliasChoices:
    """Get the alias choices for an OTEL settings field.

    Args:
        name: Field name in lowercase.

    Returns:
        The prefixed and the standard environment variable name, with the
        prefixed one taking precedence.
    """
    return AliasChoices(f"kitaru_server_{name}", name)


class APISettings(Settings):
    """API server settings."""

    # The OTEL_* fields below set validation_alias to also accept the
    # unprefixed environment variables, which otherwise disables population
    # by field name. populate_by_name restores constructing these fields
    # directly by keyword, like every other field on this settings class.
    model_config = SettingsConfigDict(populate_by_name=True)

    HOST: str = "0.0.0.0"
    PORT: int = 8000

    AUTH_SCHEME: AuthScheme = AuthScheme.NONE

    SERVER_ID: uuid.UUID = UNSET_SERVER_ID
    SERVER_URL: str = ""

    CONTROL_PLANE_API_URL: str = ""
    # Prevent Cloud bearer credentials from being sent to an unexpected host.
    CONTROL_PLANE_ALLOWED_HOSTS: list[str] = Field(default_factory=list)
    CONTROL_PLANE_TIMEOUT_SECONDS: float = 10.0
    CONTROL_PLANE_CONNECTION_POOL_SIZE: int = 20
    CONTROL_PLANE_RETRY_CONNECT: int = 2
    CONTROL_PLANE_RETRY_READ: int = 2
    CONTROL_PLANE_RETRY_STATUS: int = 2
    CONTROL_PLANE_RETRY_OTHER: int = 1
    CONTROL_PLANE_RETRY_BACKOFF_SECONDS: float = 0.25

    JWT_SIGNING_KEY: str = ""
    JWT_ISSUER: str = "kitaru"
    JWT_AUDIENCE: str = "kitaru"
    JWT_LIFETIME_SECONDS: int = 3600
    AUTH_COOKIE_NAME: str = ""
    AUTH_COOKIE_DOMAIN: str = ""
    AUTH_COOKIE_SECURE: bool | None = None

    DASHBOARD_URL: str = ""
    DEVICE_AUTH_TIMEOUT_SECONDS: int = 300
    DEVICE_AUTH_POLLING_INTERVAL_SECONDS: int = 5
    MAX_FAILED_DEVICE_AUTH_ATTEMPTS: int = 3
    # None keeps a device usable until its account deletes it.
    DEVICE_EXPIRATION_MINUTES: int | None = None
    TRUSTED_DEVICE_EXPIRATION_MINUTES: int | None = None

    DEFAULT_ACCOUNT_NAME: str = "default"
    DEFAULT_ACCOUNT_PASSWORD: str | None = None

    SECRET_ENCRYPTION_KEY: str = ""

    ANALYTICS_OPT_IN: bool = True
    ANALYTICS_DEBUG: bool = False

    OTEL_EXPORTER_OTLP_ENDPOINT: str | None = Field(
        default=None,
        validation_alias=_get_otel_alias("otel_exporter_otlp_endpoint"),
    )
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: str | None = Field(
        default=None,
        validation_alias=_get_otel_alias("otel_exporter_otlp_traces_endpoint"),
    )
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: str | None = Field(
        default=None,
        validation_alias=_get_otel_alias("otel_exporter_otlp_metrics_endpoint"),
    )
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: str | None = Field(
        default=None,
        validation_alias=_get_otel_alias("otel_exporter_otlp_logs_endpoint"),
    )
    OTEL_SERVICE_NAME: str = Field(
        default="kitaru-server",
        validation_alias=_get_otel_alias("otel_service_name"),
    )
    OTEL_TRACES_ENABLED: bool = True
    OTEL_METRICS_ENABLED: bool = True
    OTEL_LOGS_ENABLED: bool = True

    @model_validator(mode="after")
    def validate_auth_settings(self) -> Self:
        """Validate authentication settings.

        Raises:
            ValueError: A required setting is not set.

        Returns:
            The validated settings object.
        """
        if (
            self.AUTH_SCHEME in (AuthScheme.LOCAL, AuthScheme.CONTROL_PLANE)
            and not self.JWT_SIGNING_KEY
        ):
            raise ValueError("Set KITARU_SERVER_JWT_SIGNING_KEY")
        if self.AUTH_SCHEME in (AuthScheme.CONTROL_PLANE, AuthScheme.CLOUD):
            if not self.CONTROL_PLANE_API_URL:
                raise ValueError("Set KITARU_SERVER_CONTROL_PLANE_API_URL")
            if self.SERVER_ID == UNSET_SERVER_ID:
                raise ValueError("Set KITARU_SERVER_SERVER_ID")
        if self.AUTH_SCHEME is AuthScheme.CLOUD:
            parsed_control_plane_url = urlparse(self.CONTROL_PLANE_API_URL)
            if (
                parsed_control_plane_url.scheme != "https"
                or not parsed_control_plane_url.hostname
                or parsed_control_plane_url.username
                or parsed_control_plane_url.password
            ):
                raise ValueError(
                    "KITARU_SERVER_CONTROL_PLANE_API_URL must be an HTTPS URL "
                    "without embedded credentials"
                )
            if (
                self.CONTROL_PLANE_ALLOWED_HOSTS
                and parsed_control_plane_url.hostname
                not in self.CONTROL_PLANE_ALLOWED_HOSTS
            ):
                raise ValueError(
                    "KITARU_SERVER_CONTROL_PLANE_API_URL host is not allowed"
                )
        if not self.SECRET_ENCRYPTION_KEY:
            raise ValueError("Set KITARU_SERVER_SECRET_ENCRYPTION_KEY")
        return self
