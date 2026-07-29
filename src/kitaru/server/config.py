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
from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseAuthMethod(StrEnum):
    """Database authentication method."""

    PASSWORD = "password"
    AWS_IAM = "aws_iam"


class DatabaseSSLMode(StrEnum):
    """Database TLS verification mode."""

    DISABLE = "disable"
    VERIFY_FULL = "verify-full"


class Settings(BaseSettings):
    """Settings shared by every Kitaru server process."""

    model_config = SettingsConfigDict(
        env_prefix="KITARU_SERVER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    LOG_LEVEL: str = "INFO"
    SKIP_DB_MIGRATION: bool = False

    WORKER_LIVENESS_TIMEOUT_SECONDS: int = 60
    MAX_BLOB_SIZE_BYTES: int = 100 * 1024 * 1024

    LIST_QUERY_TIMEOUT_SECONDS: int = 10

    TASK_HEARTBEAT_TIMEOUT_SECONDS: int = 60
    TASK_RETRY_LIMIT: int = 3
    TASK_SWEEP_BATCH_LIMIT: int = 100
    TASK_SWEEP_INTERVAL_SECONDS: int = 15
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
    DB_AUTH_METHOD: DatabaseAuthMethod = DatabaseAuthMethod.PASSWORD
    DB_AWS_REGION: str | None = None
    DB_SSL_MODE: DatabaseSSLMode = DatabaseSSLMode.DISABLE
    CREATE_DB_IF_MISSING: bool = True

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
        if self.DB_AUTH_METHOD is DatabaseAuthMethod.AWS_IAM:
            if self.DATABASE_URL:
                raise ValueError(
                    "KITARU_SERVER_DATABASE_URL is not supported with AWS IAM "
                    "database authentication"
                )
            if not self.DB_AWS_REGION:
                raise ValueError("Set KITARU_SERVER_DB_AWS_REGION")
            if self.DB_SSL_MODE is not DatabaseSSLMode.VERIFY_FULL:
                raise ValueError(
                    "AWS IAM database authentication requires "
                    "KITARU_SERVER_DB_SSL_MODE=verify-full"
                )
        return self


@lru_cache
def get_settings() -> Settings:
    """Return the cached settings singleton for this process.

    Returns:
        Process-wide settings.
    """
    return Settings()
