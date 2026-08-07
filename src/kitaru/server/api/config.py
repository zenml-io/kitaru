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

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import SettingsConfigDict

from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.config import Settings


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
    ROOT_URL_PATH: str = ""
    CORS_ALLOW_ORIGINS: str = "*"

    AUTH_SCHEME: AuthScheme = AuthScheme.NONE

    SERVER_ID: uuid.UUID | None = None
    SERVER_URL: str = ""

    CONTROL_PLANE_API_URL: str = ""
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
    WORKER_TOKEN_LIFETIME_SECONDS: int = 3600
    TASK_TOKEN_EXPIRY_LEEWAY_SECONDS: int = 300
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

    def get_cors_allow_origins(self) -> list[str]:
        """Get all allowed CORS origins.

        Returns:
            The configured origins and any configured service URLs.
        """
        origins = [
            origin.strip()
            for origin in self.CORS_ALLOW_ORIGINS.split(",")
            if origin.strip()
        ] or ["*"]
        service_origins = [
            url
            for url in (
                self.DASHBOARD_URL,
                self.CONTROL_PLANE_API_URL,
                self.SERVER_URL,
            )
            if url
        ]
        if service_origins:
            if origins == ["*"]:
                origins = service_origins
            else:
                origins.extend(service_origins)
                origins = [origin for origin in origins if origin != "*"]
                origins = list(dict.fromkeys(origins))
        return origins

    @model_validator(mode="after")
    def validate_auth_settings(self) -> Self:
        """Validate authentication settings.

        Raises:
            ValueError: A required setting is not set.

        Returns:
            The validated settings object.
        """
        if not self.JWT_SIGNING_KEY:
            raise ValueError("Set KITARU_SERVER_JWT_SIGNING_KEY")
        if self.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
            if not self.CONTROL_PLANE_API_URL:
                raise ValueError("Set KITARU_SERVER_CONTROL_PLANE_API_URL")
            if self.SERVER_ID is None:
                raise ValueError("Set KITARU_SERVER_SERVER_ID")
        if not self.SECRET_ENCRYPTION_KEY:
            raise ValueError("Set KITARU_SERVER_SECRET_ENCRYPTION_KEY")
        return self
