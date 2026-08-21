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
"""Server configuration via environment variables."""

from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSSLMode(StrEnum):
    """PostgreSQL SSL connection mode."""

    DISABLE = "disable"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"


class LogLevel(StrEnum):
    """Supported server logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings(BaseSettings):
    """Settings shared by every Kitaru server process."""

    model_config = SettingsConfigDict(
        env_prefix="KITARU_SERVER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    LOG_LEVEL: LogLevel = LogLevel.INFO
    SKIP_DB_MIGRATION: bool = False

    WORKER_LIVENESS_TIMEOUT_SECONDS: int = 60
    MAX_BLOB_SIZE_BYTES: int = 100 * 1024 * 1024

    LIST_QUERY_TIMEOUT_SECONDS: int = 10

    TASK_HEARTBEAT_TIMEOUT_SECONDS: int = 60
    TASK_RETRY_LIMIT: int = 3
    TASK_SWEEP_BATCH_LIMIT: int = 100
    TASK_SWEEP_INTERVAL_SECONDS: int = 15
    IDEMPOTENCY_KEY_RETENTION_SECONDS: int = 900
    EVALUATOR_TASK_TIMEOUT_SECONDS: int = 300
    IMPORTER_TASK_TIMEOUT_SECONDS: int = 600
    MAX_TASK_RESULT_BYTES: int = 1024 * 1024
    EVALUATION_PAIR_LIMIT: int = 100

    DB_HOST: str | None = None
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PWD: str = "password"
    DB_NAME: str = "kitaru"
    DATABASE_URL: str | None = None
    DB_READ_HOST: str | None = None
    READ_DATABASE_URL: str | None = None
    DB_SSL_MODE: DatabaseSSLMode = DatabaseSSLMode.DISABLE
    DB_SSL_CA: str | None = None
    DB_SSL_CERT: str | None = None
    DB_SSL_KEY: str | None = None
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_TIMEOUT_SECONDS: float = 30.0

    @model_validator(mode="after")
    def validate_database_settings(self) -> Self:
        """Validate database connection settings.

        Raises:
            ValueError: Neither KITARU_SERVER_DB_HOST nor
                KITARU_SERVER_DATABASE_URL is set.

        Returns:
            The validated settings object.
        """
        if not self.DB_HOST and not self.DATABASE_URL:
            raise ValueError("Set KITARU_SERVER_DB_HOST or KITARU_SERVER_DATABASE_URL")

        ssl_paths = (self.DB_SSL_CA, self.DB_SSL_CERT, self.DB_SSL_KEY)
        if self.DB_SSL_MODE is DatabaseSSLMode.DISABLE and any(ssl_paths):
            raise ValueError(
                "Database SSL certificates cannot be configured when "
                "KITARU_SERVER_DB_SSL_MODE=disable"
            )
        if bool(self.DB_SSL_CERT) != bool(self.DB_SSL_KEY):
            raise ValueError(
                "KITARU_SERVER_DB_SSL_CERT and KITARU_SERVER_DB_SSL_KEY "
                "must be set together"
            )
        if self.DB_SSL_MODE is DatabaseSSLMode.VERIFY_CA and not self.DB_SSL_CA:
            raise ValueError(
                "Set KITARU_SERVER_DB_SSL_CA when KITARU_SERVER_DB_SSL_MODE=verify-ca"
            )
        for setting_name, path in (
            ("KITARU_SERVER_DB_SSL_CA", self.DB_SSL_CA),
            ("KITARU_SERVER_DB_SSL_CERT", self.DB_SSL_CERT),
            ("KITARU_SERVER_DB_SSL_KEY", self.DB_SSL_KEY),
        ):
            if path and not Path(path).is_file():
                raise ValueError(f"{setting_name} does not exist: {path}")
        return self


@lru_cache
def get_settings() -> Settings:
    """Return the cached settings singleton for this process.

    Returns:
        Process-wide settings.
    """
    return Settings()
